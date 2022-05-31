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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
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

	// AddPodToResource adds the given pod to the given resource in the cache
	// indicating the specified pod should use the specified resource.
	// A unique resourceName is generated from the resourceSpec and returned on
	// success.
	// If no resource plugin can support the given resourceSpec or more than one
	// plugin can support it, an error is returned.
	// If a pod with the same unique name already exists under the specified
	// resource, this is a no-op.
	AddPodToResource(podName UniquePodName, pod *v1.Pod, resourceSpec *ResourceSpec) error

	// PopPodErrors returns accumulated errors on a given pod and clears
	// them.
	PopPodErrors(podName UniquePodName) []string

	// AddErrorToPod adds the given error to the given pod in the cache.
	// It will be returned by subsequent GetPodErrors().
	// Each error string is stored only once.
	AddErrorToPod(podName UniquePodName, err string)
}

// ResourceToPrepare represents a resource that needs to be prepared for PodName
type ResourceToPrepare struct {
}

// ResourceSpec is an internal representation of a resource
// It contains attributes that are required to prepare and unprepare the resource.
type ResourceSpec struct {
	Name                 UniqueResourceName
	DriverName           string
	ResourceClaimUUID    types.UID
	AllocationAttributes map[string]string
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

	// reportedInUse indicates that the volume was successfully added to the
	// VolumesInUse field in the node's status.
	reportedInUse bool
}

// The pod object represents a pod that references the underlying volume and
// should mount it once it is attached.
type podToAttach struct {
	// podName contains the name of this pod.
	podName UniquePodName

	// Pod to mount the volume to. Used to create NewMounter.
	pod *v1.Pod

	// resource spec containing the specification for this resource.
	resourceSpec *ResourceSpec

	// prepareRequestTime stores time at which resource preparation was requested
	prepareRequestTime time.Time
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

func (dsw *desiredStateOfWorld) AddErrorToPod(podName UniquePodName, err string) {
	dsw.Lock()
	defer dsw.Unlock()

	if errs, found := dsw.podErrors[podName]; found {
		if errs.Len() <= maxPodErrors {
			errs.Insert(err)
		}
		return
	}
	dsw.podErrors[podName] = sets.NewString(err)
}

func (dsw *desiredStateOfWorld) AddPodToResource(
	podName UniquePodName,
	pod *v1.Pod,
	resourceSpec *ResourceSpec) error {
	dsw.Lock()
	defer dsw.Unlock()

	/*resourcePlugin, err := dsw.resourcePluginMgr.FindPluginByName(resourceSpec.DriverName)
	if err != nil || resourcePlugin == nil {
		return fmt.Errorf(
			"failed to get Plugin for resource %s, err=%v",
			resourceSpec.Name,
			err)
	}*/

	resourceName := resourceSpec.Name

	if _, resourceExists := dsw.resourcesToPrepare[resourceName]; !resourceExists {
		dsw.resourcesToPrepare[resourceName] = resourceToPrepare{
			resourceName:  resourceName,
			podsToAttach:  make(map[UniquePodName]podToAttach),
			reportedInUse: false,
		}
	}

	// Create new podToPrepare object. If it already exists, it is refreshed with
	// updated values (this is required for resources that require re-preparing on
	// pod update, like Downward API resources).
	dsw.resourcesToPrepare[resourceName].podsToAttach[podName] = podToAttach{
		podName:            podName,
		pod:                pod,
		resourceSpec:       resourceSpec,
		prepareRequestTime: time.Now(),
	}
	return nil
}
