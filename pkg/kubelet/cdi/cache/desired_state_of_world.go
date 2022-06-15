/*
Copyright 2022 The Kubernetes Authors.

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
Package cache implements data structures used by the kubelet resource manager to
keep track of prepared resources and the pods that use them.
*/
package cache

import (
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/cdi"
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
	PodExistsInResource(podName cdi.UniquePodName, resourceName cdi.UniqueResourceName) bool

	// AddPodToResource adds the given pod to the given resource in the cache
	// indicating the specified pod should use the specified resource.
	// A unique resourceName is generated from the resourceSpec and returned on
	// success.
	// If no resource plugin can support the given resourceSpec or more than one
	// plugin can support it, an error is returned.
	// If a pod with the same unique name already exists under the specified
	// resource, this is a no-op.
	AddPodToResource(podName cdi.UniquePodName, pod *v1.Pod, resourceSpec *ResourceSpec) error

	// PopPodErrors returns accumulated errors on a given pod and clears
	// them.
	PopPodErrors(podName cdi.UniquePodName) []string

	// AddErrorToPod adds the given error to the given pod in the cache.
	// It will be returned by subsequent GetPodErrors().
	// Each error string is stored only once.
	AddErrorToPod(podName cdi.UniquePodName, err string)

	// GetResourcesToPrepare generates and returns a list of resources that should be
	// prepared based on the current desired state of the world.
	GetResourcesToPrepare() []ResourceToPrepare
}

// ResourceToPrepare represents a resource that needs to be prepared for PodName
type ResourceToPrepare struct {
	// ResourceName contains the unique identifier for this resource.
	ResourceName cdi.UniqueResourceName

	// resource spec containing the specification for this resource.
	ResourceSpec *ResourceSpec

	// ReportedInUse indicates that the resource was successfully added to the
	// ResourceInUse field in the node's status.
	ReportedInUse bool

	// PodName is the unique identifier for the pod that the resource should be
	// prepared for
	PodName cdi.UniquePodName

	// Pod to mount the volume to. Used to create NewMounter.
	Pod *v1.Pod

	ResourcePluginClient cdi.CDIClient
}

// ResourceSpec is an internal representation of a resource
// It contains attributes that are required to prepare and unprepare the resource.
type ResourceSpec struct {
	Name                 cdi.UniqueResourceName
	DriverName           string
	ResourceClaimUUID    types.UID
	AllocationAttributes map[string]string
}

// NewDesiredStateOfWorld returns a new instance of DesiredStateOfWorld.
func NewDesiredStateOfWorld() DesiredStateOfWorld {
	return &desiredStateOfWorld{
		resourcesToPrepare: make(map[cdi.UniqueResourceName]resourceToPrepare),
		podErrors:          make(map[cdi.UniquePodName]sets.String),
	}
}

type desiredStateOfWorld struct {
	// resourcesToPrepare is a map containing the set of resources that should be
	// prepared on this node. The key in the map is the name of the resource and
	// the value is a resource object containing more information about the resource.
	resourcesToPrepare map[cdi.UniqueResourceName]resourceToPrepare

	// podErrors are errors caught by desiredStateOfWorldPopulator about resources for a given pod.
	podErrors map[cdi.UniquePodName]sets.String

	sync.RWMutex
}

// The resourceToPrepare object represents a resource that should be prepared on this node,
// and available for usage to podsToAttach.
type resourceToPrepare struct {
	// resourceName contains the unique identifier for this resource.
	resourceName cdi.UniqueResourceName

	// podsToAttach is a map containing the set of pods that reference this
	// resource. The key in the map is
	// the name of the pod and the value is a pod object containing more
	// information about the pod.
	podsToAttach map[cdi.UniquePodName]podToAttach

	// reportedInUse indicates that the resource was successfully added to the
	// ResourceInUse field in the node's status.
	reportedInUse bool

	resourcePluginClient cdi.CDIClient
}

// The pod object represents a pod that references the underlying resource and
// should prepare it.
type podToAttach struct {
	// podName contains the name of this pod.
	podName cdi.UniquePodName

	// Pod to use the resource.
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
	podName cdi.UniquePodName, resourceName cdi.UniqueResourceName) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	resourceObj, resourceExists := dsw.resourcesToPrepare[resourceName]
	if !resourceExists {
		return false
	}

	_, podExists := resourceObj.podsToAttach[podName]
	return podExists
}

func (dsw *desiredStateOfWorld) PopPodErrors(podName cdi.UniquePodName) []string {
	dsw.Lock()
	defer dsw.Unlock()

	if errs, found := dsw.podErrors[podName]; found {
		delete(dsw.podErrors, podName)
		return errs.List()
	}
	return []string{}
}

func (dsw *desiredStateOfWorld) AddErrorToPod(podName cdi.UniquePodName, err string) {
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
	podName cdi.UniquePodName,
	pod *v1.Pod,
	resourceSpec *ResourceSpec) error {
	dsw.Lock()
	defer dsw.Unlock()

	resourcePluginClient, err := cdi.NewCDIPluginClient(resourceSpec.DriverName)
	if err != nil || resourcePluginClient == nil {
		return fmt.Errorf(
			"failed to get CDI Plugin for driver name %s, err=%v",
			resourceSpec.DriverName,
			err)
	}

	resourceName := resourceSpec.Name

	if _, resourceExists := dsw.resourcesToPrepare[resourceName]; !resourceExists {
		dsw.resourcesToPrepare[resourceName] = resourceToPrepare{
			resourceName:         resourceName,
			podsToAttach:         make(map[cdi.UniquePodName]podToAttach),
			reportedInUse:        false,
			resourcePluginClient: resourcePluginClient,
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

func (dsw *desiredStateOfWorld) GetResourcesToPrepare() []ResourceToPrepare {
	dsw.RLock()
	defer dsw.RUnlock()

	resourcesToPrepare := make([]ResourceToPrepare, 0 /* len */, len(dsw.resourcesToPrepare) /* cap */)
	for resourceName, resourceObj := range dsw.resourcesToPrepare {
		for podName, podObj := range resourceObj.podsToAttach {
			rtp := ResourceToPrepare{
				ResourceName:         resourceName,
				ResourceSpec:         podObj.resourceSpec,
				PodName:              podName,
				Pod:                  podObj.pod,
				ReportedInUse:        resourceObj.reportedInUse,
				ResourcePluginClient: resourceObj.resourcePluginClient,
			}
			resourcesToPrepare = append(resourcesToPrepare, rtp)
		}
	}
	return resourcesToPrepare
}
