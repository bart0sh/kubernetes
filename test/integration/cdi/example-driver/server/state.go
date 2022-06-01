/*
Copyright 2021 The Kubernetes Authors.

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

// Package state manages the internal state of the driver which needs to be maintained
// across driver restarts.
package server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"

	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AccessType int

const (
	MountAccess AccessType = iota
	BlockAccess
)

type Resource struct {
	ResName        string
	ResID          string
	ResPath        string
	ResAccessType  AccessType
	NodeID         string
	Kind           string
	ReadOnlyAttach bool
	Attached       bool
	// Prepared contains the preparing target path at which the resource
	// was prepared. A set of paths is used for consistency
	// with Published.
	Prepared Strings
}

type Snapshot struct {
	Name         string
	Id           string
	VolID        string
	Path         string
	CreationTime *timestamp.Timestamp
	SizeBytes    int64
	ReadyToUse   bool
}

// State is the interface that the rest of the code has to use to
// access and change state. All error messages contain gRPC
// status codes and can be returned without wrapping.
type State interface {
	// GetVolumeByID retrieves a volume by its unique ID or returns
	// an error including that ID when not found.
	GetResourceByID(volID string) (Resource, error)

	// GetVolumeByName retrieves a volume by its name or returns
	// an error including that name when not found.
	GetResourceByName(volName string) (Resource, error)

	// GetVolumes returns all currently existing volumes.
	GetResources() []Resource

	// UpdateVolume updates the existing hostpath volume,
	// identified by its volume ID, or adds it if it does
	// not exist yet.
	UpdateResource(resource Resource) error

	// DeleteVolume deletes the volume with the given
	// volume ID. It is not an error when such a volume
	// does not exist.
	DeleteResource(resourceID string) error

	// GetSnapshotByID retrieves a snapshot by its unique ID or returns
	// an error including that ID when not found.
	GetSnapshotByID(snapshotID string) (Snapshot, error)

	// GetSnapshotByName retrieves a snapshot by its name or returns
	// an error including that name when not found.
	GetSnapshotByName(volName string) (Snapshot, error)

	// GetSnapshots returns all currently existing snapshots.
	GetSnapshots() []Snapshot

	// UpdateSnapshot updates the existing hostpath snapshot,
	// identified by its snapshot ID, or adds it if it does
	// not exist yet.
	UpdateSnapshot(snapshot Snapshot) error

	// DeleteSnapshot deletes the snapshot with the given
	// snapshot ID. It is not an error when such a snapshot
	// does not exist.
	DeleteSnapshot(snapshotID string) error
}

type resources struct {
	Resources []Resource
	Snapshots []Snapshot
}

type state struct {
	resources

	statefilePath string
}

var _ State = &state{}

// New retrieves the complete state of the driver from the file if given
// and then ensures that all changes are mirrored immediately in the
// given file. If not given, the initial state is empty and changes
// are not saved.
func New(statefilePath string) (State, error) {
	s := &state{
		statefilePath: statefilePath,
	}

	return s, s.restore()
}

func (s *state) dump() error {
	data, err := json.Marshal(&s.resources)
	if err != nil {
		return status.Errorf(codes.Internal, "error encoding volumes and snapshots: %v", err)
	}
	if err := ioutil.WriteFile(s.statefilePath, data, 0600); err != nil {
		return status.Errorf(codes.Internal, "error writing state file: %v", err)
	}
	return nil
}

func (s *state) restore() error {
	s.Resources = nil
	s.Snapshots = nil

	data, err := ioutil.ReadFile(s.statefilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Nothing to do.
		return nil
	case err != nil:
		return status.Errorf(codes.Internal, "error reading state file: %v", err)
	}
	if err := json.Unmarshal(data, &s.resources); err != nil {
		return status.Errorf(codes.Internal, "error encoding volumes and snapshots from state file %q: %v", s.statefilePath, err)
	}
	return nil
}

func (s *state) GetResourceByID(resourcdID string) (Resource, error) {
	for _, volume := range s.Resources {
		if volume.ResID == resourcdID {
			return volume, nil
		}
	}
	return Resource{}, status.Errorf(codes.NotFound, "resource id %s does not exist in the volumes list", resourcdID)
}

func (s *state) GetResourceByName(resName string) (Resource, error) {
	for _, resource := range s.Resources {
		if resource.ResName == resName {
			return resource, nil
		}
	}
	return Resource{}, status.Errorf(codes.NotFound, "volume name %s does not exist in the volumes list", resName)
}

func (s *state) GetResources() []Resource {
	volumes := make([]Resource, len(s.Resources))
	for i, volume := range s.Resources {
		volumes[i] = volume
	}
	return volumes
}

func (s *state) UpdateResource(update Resource) error {
	for i, volume := range s.Resources {
		if volume.ResID == update.ResID {
			s.Resources[i] = update
			return nil
		}
	}
	s.Resources = append(s.Resources, update)
	return s.dump()
}

func (s *state) DeleteResource(volID string) error {
	for i, volume := range s.Resources {
		if volume.ResID == volID {
			s.Resources = append(s.Resources[:i], s.Resources[i+1:]...)
			return s.dump()
		}
	}
	return nil
}

func (s *state) GetSnapshotByID(snapshotID string) (Snapshot, error) {
	for _, snapshot := range s.Snapshots {
		if snapshot.Id == snapshotID {
			return snapshot, nil
		}
	}
	return Snapshot{}, status.Errorf(codes.NotFound, "snapshot id %s does not exist in the snapshots list", snapshotID)
}

func (s *state) GetSnapshotByName(name string) (Snapshot, error) {
	for _, snapshot := range s.Snapshots {
		if snapshot.Name == name {
			return snapshot, nil
		}
	}
	return Snapshot{}, status.Errorf(codes.NotFound, "snapshot name %s does not exist in the snapshots list", name)
}

func (s *state) GetSnapshots() []Snapshot {
	snapshots := make([]Snapshot, len(s.Snapshots))
	for i, snapshot := range s.Snapshots {
		snapshots[i] = snapshot
	}
	return snapshots
}

func (s *state) UpdateSnapshot(update Snapshot) error {
	for i, snapshot := range s.Snapshots {
		if snapshot.Id == update.Id {
			s.Snapshots[i] = update
			return s.dump()
		}
	}
	s.Snapshots = append(s.Snapshots, update)
	return s.dump()
}

func (s *state) DeleteSnapshot(snapshotID string) error {
	for i, snapshot := range s.Snapshots {
		if snapshot.Id == snapshotID {
			s.Snapshots = append(s.Snapshots[:i], s.Snapshots[i+1:]...)
			return s.dump()
		}
	}
	return nil
}
