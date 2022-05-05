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

	"k8s.io/apimachinery/pkg/types"
	cditypes "k8s.io/kubernetes/pkg/apis/cdi"
	"k8s.io/kubernetes/pkg/apis/core"
)

const (
	// ResourcePrepared means that resource has been prepare for pod usage
	ResourcePrepared cditypes.ResourcePreparationState = "ResourcePrepared"

	// ResourceNotPrepared means that resource has not been prepared
	ResourceNotPrepared cditypes.ResourcePreparationState = "ResourceNotPrepared"
)

// ActualStateOfWorld defines a set of thread-safe operations for the kubelet
// resource manager's actual state of the world cache.
// This cache contains resources->pods i.e. a set of all resources present
// on the node and the pods that the manager believes have successfully prepared
// the resources.
type ActualStateOfWorld interface {
	// GetAllPreparedResources returns list of all possibly prepared resources
	GetAllPreparedResources() []PreparedResource
}

// PreparedResource represents a resource that has successfully been given to a pod.
type PreparedResource struct {
	PodName      cditypes.UniquePodName
	ResourceName cditypes.UniqueResourceName
}

// NewActualStateOfWorld returns a new instance of ActualStateOfWorld.
func NewActualStateOfWorld(nodeName types.NodeName) ActualStateOfWorld {
	return &actualStateOfWorld{
		nodeName:          nodeName,
		preparedResources: make(map[cditypes.UniqueResourceName]preparedResource),
	}
}

type actualStateOfWorld struct {
	// nodeName is the name of this node.
	nodeName types.NodeName

	// preparedResources is a map containing the set of resources the kubelet resource
	// manager believes to be successfully prepared.
	// The key in this map is the name of the resource and the value is an object
	// containing more information about the prepared resource.
	preparedResources map[cditypes.UniqueResourceName]preparedResource

	sync.RWMutex
}

// preparedResource represents a resource the kubelet resource manager believes to be
// successfully prepared.
type preparedResource struct {
	// resourceName contains the unique identifier for this resource.
	resourceName cditypes.UniqueResourceName

	// pods is a map containing the set of pods that this volume has been
	// successfully mounted to. The key in this map is the name of the pod and
	// the value is a mountedPod object containing more information about the
	// pod.
	attachedPods map[cditypes.UniquePodName]attachedPod

	// resourceClaim is a claim for this resource
	resourceClaim core.PodResourceClaim

	// pluginName is the Unescaped Qualified name of the resource plugin used to
	// prepare this resource.
	pluginName string
}

// The attachedPod object represents a pod for which the kubelet resource manager
// believes the underlying resource has been successfully attached.
type attachedPod struct {
	// the name of the pod
	podName cditypes.UniquePodName

	// the UID of the pod
	podUID types.UID

	// resource claims for all resources used by the pod
	resourceClaims []core.PodResourceClaim

	// resourceID contains the value of the resource identifier
	resourceID string

	// resourceStateForPod stores state of resource preparation for the pod. if it is:
	//   - ResourcePrepared: means resource for pod has been successfully prepared
	//   - ResourceUnprepared: means resource for pod is not prepared, but it must be prepared
	resourceStateForPod cditypes.ResourcePreparationState
}

// GetAllPreparedResources returns all resources which could be prepared for a pod.
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
