/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logs

import (
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

const waitDuration = 150 * time.Millisecond

// newDedupWriteEvents reduce write events from fsnotify.Watcher to reduce calling function isContainerRunning.
// When container output logs quickly, and user run command `kubectl logs CONTAINER_ID -f`,
// then there will be too much function call of `isContainerRunning`.
// This will consume CPU time of kubelet and containerd.
func newDedupWriteEvents(logfileName string, w *fsnotify.Watcher) *dedupWriteEvents {
	return &dedupWriteEvents{
		w:           w,
		logFileName: logfileName,
		Errors:      make(chan error),
		Events:      make(chan fsnotify.Event, 4),
	}
}

func (de *dedupWriteEvents) close() {
	close(de.Events)
	// clean all events in channel
	for range de.Events {
	}
	close(de.Errors)
	for range de.Errors {
	}
}

type dedupWriteEvents struct {
	w           *fsnotify.Watcher
	logFileName string

	// Events sends fsnotify events.
	Events chan fsnotify.Event
	// Errors sends fsnotify errors.
	Errors chan error
}

// dedupLoop deduplicate Write events.
func (de *dedupWriteEvents) dedupLoop() {
	var lastAdd time.Time
	// All channels of dedupWriteEvents will close after fsnotify.Watcher has been closed.
	defer de.close()
	for {
		select {
		case err := <-de.w.Errors:
			de.Errors <- err

		case e, ok := <-de.w.Events:
			if !ok { // Channel was closed (i.e. Watcher.Close() was called).
				return
			}

			if filepath.Base(e.Name) != de.logFileName {
				continue
			}

			switch e.Op {
			case fsnotify.Write:
				// If there is one Write event in the channel, we can discard this one.
				// If there is one Create event in the channel, we will reopen the log file, and read from the
				// start. It's OK to discard the new Write event.
				if len(de.Events) > 0 ||
					time.Since(lastAdd) < waitDuration {
					continue
				}
				lastAdd = time.Now()
			case fsnotify.Create:
				// Always add a write event before create event. In case lost some log lines.
				de.Events <- fsnotify.Event{
					Name: e.Name,
					Op:   fsnotify.Write,
				}
				lastAdd = time.Now()
			default:
			}
			de.Events <- e
		}
	}
}
