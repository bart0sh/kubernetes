/*
Copyright 2016 The Kubernetes Authors.

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

/*
Package cache implements data structures used by the kubelet volume manager to
keep track of attached volumes and the pods that mounted them.
*/
package cache

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// DesiredStateOfWorld defines a set of thread-safe operations for the kubelet
// resource manager's desired state of the world cache.
// This cache contains resources->pods i.e. a set of all resources that should be
// prepared on this node and the pods that reference them.
type DesiredStateOfWorld interface {
	// PodExistsInResource returns true if the given pod exists in the list of
	// podsToPrepare for the given resource in the cache.
	// If a pod with the same unique name does not exist under the specified
	// resource, false is returned.
	// If a resource with the name resourceName does not exist in the list of
	// prepared resources, false is returned.
	PodExistsInResource(podName UniquePodName, resourceName UniqueResourceName) bool

	// PopPodErrors returns accumulated errors on a given pod and clears
	// them.
	PopPodErrors(podName UniquePodName) []string
}

// ResourceToPrepare represents a resource that needs to be prepared for PodName
type ResourceToPrepare struct {
}

// NewDesiredStateOfWorld returns a new instance of DesiredStateOfWorld.
func NewDesiredStateOfWorld() DesiredStateOfWorld {
	return &desiredStateOfWorld{
		resourcesToPrepare: make(map[UniqueResourceName]resourceToPrepare),
		podErrors:          make(map[UniquePodName]sets.String),
	}
}

type desiredStateOfWorld struct {
	// resourcesToPrepare is a map containing the set of resources that should be
	// prepared on this node. The key in the map is the name of the resource and
	// the value is a resource object containing more information about the resource.
	resourcesToPrepare map[UniqueResourceName]resourceToPrepare

	// podErrors are errors caught by desiredStateOfWorldPopulator about resources for a given pod.
	podErrors map[UniquePodName]sets.String

	sync.RWMutex
}

// The volume object represents a volume that should be attached to this node,
// and mounted to podsToMount.
type resourceToPrepare struct {
	// resourceName contains the unique identifier for this resource.
	resourceName UniqueResourceName

	// podsToAttach is a map containing the set of pods that reference this
	// resource and should attach it. The key in the map is
	// the name of the pod and the value is a pod object containing more
	// information about the pod.
	podsToAttach map[UniquePodName]podToAttach

	// volumeGidValue contains the value of the GID annotation, if present.
	resourceID string

	// reportedInUse indicates that the volume was successfully added to the
	// VolumesInUse field in the node's status.
	reportedInUse bool
}

// The pod object represents a pod that references the underlying volume and
// should mount it once it is attached.
type podToAttach struct {
	// podName contains the name of this pod.
	podName types.UniquePodName

	// Pod to mount the volume to. Used to create NewMounter.
	pod *v1.Pod

	// volume spec containing the specification for this volume. Used to
	// generate the volume plugin object, and passed to plugin methods.
	// For non-PVC volumes this is the same as defined in the pod object. For
	// PVC volumes it is from the dereferenced PV object.
	volumeSpec *volume.Spec

	// outerVolumeSpecName is the volume.Spec.Name() of the volume as referenced
	// directly in the pod. If the volume was referenced through a persistent
	// volume claim, this contains the volume.Spec.Name() of the persistent
	// volume claim
	outerVolumeSpecName string
	// mountRequestTime stores time at which mount was requested
	mountRequestTime time.Time
}

const (
	// Maximum errors to be stored per pod in desiredStateOfWorld.podErrors to
	// prevent unbound growth.
	maxPodErrors = 10
)

func (dsw *desiredStateOfWorld) PodExistsInResource(
	podName UniquePodName, resourceName UniqueResourceName) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	resourceObj, resourceExists := dsw.resourcesToPrepare[resourceName]
	if !resourceExists {
		return false
	}

	_, podExists := resourceObj.podsToAttach[podName]
	return podExists
}

func (dsw *desiredStateOfWorld) PopPodErrors(podName UniquePodName) []string {
	dsw.Lock()
	defer dsw.Unlock()

	if errs, found := dsw.podErrors[podName]; found {
		delete(dsw.podErrors, podName)
		return errs.List()
	}
	return []string{}
}
