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
	cditypes "k8s.io/kubernetes/pkg/apis/cdi"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// DesiredStateOfWorld defines a set of thread-safe operations for the kubelet
// volume manager's desired state of the world cache.
// This cache contains volumes->pods i.e. a set of all volumes that should be
// attached to this node and the pods that reference them and should mount the
// volume.
// Note: This is distinct from the DesiredStateOfWorld implemented by the
// attach/detach controller. They both keep track of different objects. This
// contains kubelet volume manager specific state.
type DesiredStateOfWorld interface {
	// PodExistsInVolume returns true if the given pod exists in the list of
	// podsToMount for the given volume in the cache.
	// If a pod with the same unique name does not exist under the specified
	// volume, false is returned.
	// If a volume with the name volumeName does not exist in the list of
	// attached volumes, false is returned.
	PodExistsInResource(podName cditypes.UniquePodName, resourceName cditypes.UniqueResourceName) bool
}

// ResourceToPrepare represents a resource that needs to be prepared for PodName
type ResourceToPrepare struct {
}

// NewDesiredStateOfWorld returns a new instance of DesiredStateOfWorld.
func NewDesiredStateOfWorld(volumePluginMgr *volume.VolumePluginMgr) DesiredStateOfWorld {
	return &desiredStateOfWorld{
		resourcesToPrepare: make(map[cditypes.UniqueResourceName]resourceToPrepare),
		podErrors:          make(map[cditypes.UniquePodName]sets.String),
	}
}

type desiredStateOfWorld struct {
	// volumesToMount is a map containing the set of volumes that should be
	// attached to this node and mounted to the pods referencing it. The key in
	// the map is the name of the volume and the value is a volume object
	// containing more information about the volume.
	resourcesToPrepare map[cditypes.UniqueResourceName]resourceToPrepare
	// podErrors are errors caught by desiredStateOfWorldPopulator about volumes for a given pod.
	podErrors map[cditypes.UniquePodName]sets.String

	sync.RWMutex
}

// The volume object represents a volume that should be attached to this node,
// and mounted to podsToMount.
type resourceToPrepare struct {
	// resourceName contains the unique identifier for this resource.
	resourceName cditypes.UniqueResourceName

	// podsToAttach is a map containing the set of pods that reference this
	// resource and should attach it. The key in the map is
	// the name of the pod and the value is a pod object containing more
	// information about the pod.
	podsToAttach map[cditypes.UniquePodName]podToAttach

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
	podName cditypes.UniquePodName, resourceName cditypes.UniqueResourceName) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	resourceObj, resourceExists := dsw.resourcesToPrepare[resourceName]
	if !resourceExists {
		return false
	}

	_, podExists := resourceObj.podsToAttach[podName]
	return podExists
}
