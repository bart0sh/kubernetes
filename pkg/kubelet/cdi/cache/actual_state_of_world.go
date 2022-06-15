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
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/cdi"
)

// ResourcePreparationState represents resource preparation state
type ResourcePreparationState string

const (
	// ResourcePrepared means that resource has been prepare for pod usage
	ResourcePrepared ResourcePreparationState = "ResourcePrepared"

	// ResourceNotPrepared means that resource has not been prepared
	ResourceNotPrepared ResourcePreparationState = "ResourceNotPrepared"
)

// ActualStateOfWorld defines a set of thread-safe operations for the kubelet
// resource manager's actual state of the world cache.
// This cache contains resources->pods i.e. a set of all resources present
// on the node and the pods that the manager believes have successfully prepared
// the resources.
type ActualStateOfWorld interface {
	// GetAllPreparedResources returns list of all prepared resources
	GetAllPreparedResources() []PreparedResource

	// GetPreparedResourcesForPod generates and returns a list of resources that are
	// successfully prepared for the specified pod based on the current actual state
	// of the world.
	GetPreparedResourcesForPod(podName cdi.UniquePodName) []PreparedResource

	// SyncResource checks the resource.claimName in asw and
	// the one populated from dsw , if they do not match, update this field from the value from dsw.
	SyncResource(resourceName cdi.UniqueResourceName, podName cdi.UniquePodName, claimUUID types.UID)
}

// PreparedResource represents a resource that has successfully been given to a pod.
type PreparedResource struct {
	PodName      cdi.UniquePodName
	ResourceName cdi.UniqueResourceName
}

// NewActualStateOfWorld returns a new instance of ActualStateOfWorld.
func NewActualStateOfWorld() ActualStateOfWorld {
	return &actualStateOfWorld{
		preparedResources: make(map[cdi.UniqueResourceName]preparedResource),
	}
}

type actualStateOfWorld struct {
	// preparedResources is a map containing the set of resources the kubelet resource
	// manager believes to be successfully prepared.
	// The key in this map is the name of the resource and the value is an object
	// containing more information about the prepared resource.
	preparedResources map[cdi.UniqueResourceName]preparedResource

	sync.RWMutex
}

// preparedResource represents a resource the kubelet resource manager believes to be
// successfully prepared.
type preparedResource struct {
	// resourceName contains the unique identifier for this resource.
	resourceName cdi.UniqueResourceName

	// pods is a map containing the set of pods that this resource has been
	// successfully used by. The key in this map is the name of the pod and
	// the value is a attachedPod object containing more information about the
	// pod.
	attachedPods map[cdi.UniquePodName]attachedPod

	// resourceClaim is a claim for this resource
	// resourceClaim core.PodResourceClaim

	// pluginName is the Unescaped Qualified name of the resource plugin used to
	// prepare this resource.
	pluginName string
}

// The attachedPod object represents a pod for which the kubelet resource manager
// believes the underlying resource has been successfully attached.
type attachedPod struct {
	// the name of the pod
	podName cdi.UniquePodName

	// the UID of the pod
	podUID types.UID

	// resource name
	resourceName cdi.UniqueResourceName

	// resourceClaim UUID
	claimUUID types.UID

	// resourceStateForPod stores state of resource preparation for the pod. if it is:
	//   - ResourcePrepared: means resource for pod has been successfully prepared
	//   - ResourceUnprepared: means resource for pod is not prepared, but it must be prepared
	resourceStateForPod ResourcePreparationState
}

// GetAllPreparedResources returns resources which could be prepared for a pod.
func (asw *actualStateOfWorld) GetAllPreparedResources() []PreparedResource {
	asw.RLock()
	defer asw.RUnlock()
	preparedResources := make([]PreparedResource, 0 /* len */, len(asw.preparedResources) /* cap */)
	for _, preparedResource := range asw.preparedResources {
		for _, podObj := range preparedResource.attachedPods {
			if podObj.resourceStateForPod == ResourcePrepared {
				preparedResources = append(
					preparedResources,
					getPreparedResource(&podObj, &preparedResource))
			}
		}
	}

	return preparedResources
}

// getPreparedResource constructs and returns a PreparedResource object from the given
// attachedPod and preparedResource objects.
func getPreparedResource(
	attachedPod *attachedPod, preparedResource *preparedResource) PreparedResource {
	return PreparedResource{}
}

// getPreparedResourcesForPod returns list of prepared resources
// that are attached to the given pod
func (asw *actualStateOfWorld) GetPreparedResourcesForPod(podName cdi.UniquePodName) []PreparedResource {
	asw.RLock()
	defer asw.RUnlock()
	preparedResources := []PreparedResource{}
	for _, resource := range asw.preparedResources {
		for preparedPodName, pod := range resource.attachedPods {
			if preparedPodName == podName {
				preparedResources = append(
					preparedResources,
					getPreparedResource(&pod, &resource))
			}
		}
	}

	return preparedResources
}

func (asw *actualStateOfWorld) SyncResource(resourceName cdi.UniqueResourceName, podName cdi.UniquePodName, resourceClaimUUID types.UID) {
	asw.Lock()
	defer asw.Unlock()
	if resourceObj, resourceExists := asw.preparedResources[resourceName]; resourceExists {
		if podObj, podExists := resourceObj.attachedPods[podName]; podExists {
			if podObj.claimUUID != resourceClaimUUID {
				podObj.claimUUID = resourceClaimUUID
				asw.preparedResources[resourceName].attachedPods[podName] = podObj
			}
		}
	}
}
