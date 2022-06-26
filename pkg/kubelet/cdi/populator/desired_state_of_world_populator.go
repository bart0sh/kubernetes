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
Package populator implements interfaces that monitor and keep the states of the
caches in sync with the "ground truth".
*/
package populator

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/cdi/resourceclaim"
	"k8s.io/klog/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/cdi"
	"k8s.io/kubernetes/pkg/kubelet/cdi/cache"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/pod"
)

// DesiredStateOfWorldPopulator periodically loops through the list of active
// pods and ensures that each one exists in the desired state of the world cache
// if it has resources. It also verifies that the pods in the desired state of the
// world cache still exist, if not, it removes them.
type DesiredStateOfWorldPopulator interface {
	Run(sourcesReady config.SourcesReady, stopCh <-chan struct{})

	// ReprocessPod sets value for the specified pod in processedPods
	// to false, forcing it to be reprocessed. This is required to enable
	// re-preparing resources on pod updates
	ReprocessPod(podName cdi.UniquePodName)

	// HasAddedPods returns whether the populator has looped through the list
	// of active pods and added them to the desired state of the world cache,
	// at a time after sources are all ready, at least once. It does not
	// return true before sources are all ready because before then, there is
	// a chance many or all pods are missing from the list of active pods and
	// so few to none will have been added.
	HasAddedPods() bool
}

// podStateProvider can determine if a pod is going to be terminated.
type podStateProvider interface {
	ShouldPodContainersBeTerminating(types.UID) bool
	ShouldPodRuntimeBeRemoved(types.UID) bool
}

// NewDesiredStateOfWorldPopulator returns a new instance of
// DesiredStateOfWorldPopulator.
//
// kubeClient - used to fetch PV and PVC objects from the API server
// loopSleepDuration - the amount of time the populator loop sleeps between
//     successive executions
// podManager - the kubelet podManager that is the source of truth for the pods
//     that exist on this host
// desiredStateOfWorld - the cache to populate
func NewDesiredStateOfWorldPopulator(
	kubeClient clientset.Interface,
	loopSleepDuration time.Duration,
	getPodStatusRetryDuration time.Duration,
	podManager pod.Manager,
	podStateProvider podStateProvider,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld,
	kubeContainerRuntime kubecontainer.Runtime) DesiredStateOfWorldPopulator {
	return &desiredStateOfWorldPopulator{
		kubeClient:                kubeClient,
		loopSleepDuration:         loopSleepDuration,
		getPodStatusRetryDuration: getPodStatusRetryDuration,
		podManager:                podManager,
		podStateProvider:          podStateProvider,
		desiredStateOfWorld:       desiredStateOfWorld,
		actualStateOfWorld:        actualStateOfWorld,
		pods: processedPods{
			processedPods: make(map[cdi.UniquePodName]bool)},
		kubeContainerRuntime: kubeContainerRuntime,
		hasAddedPods:         false,
		hasAddedPodsLock:     sync.RWMutex{},
	}
}

type desiredStateOfWorldPopulator struct {
	kubeClient                clientset.Interface
	loopSleepDuration         time.Duration
	getPodStatusRetryDuration time.Duration
	podManager                pod.Manager
	podStateProvider          podStateProvider
	desiredStateOfWorld       cache.DesiredStateOfWorld
	actualStateOfWorld        cache.ActualStateOfWorld
	pods                      processedPods
	kubeContainerRuntime      kubecontainer.Runtime
	timeOfLastGetPodStatus    time.Time
	hasAddedPods              bool
	hasAddedPodsLock          sync.RWMutex
}

type processedPods struct {
	processedPods map[cdi.UniquePodName]bool
	sync.RWMutex
}

func (dswp *desiredStateOfWorldPopulator) Run(sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	// Wait for the completion of a loop that started after sources are all ready, then set hasAddedPods accordingly
	klog.InfoS("Desired state populator starts to run")
	wait.PollUntil(dswp.loopSleepDuration, func() (bool, error) {
		done := sourcesReady.AllReady()
		dswp.populatorLoop()
		return done, nil
	}, stopCh)
	dswp.hasAddedPodsLock.Lock()
	dswp.hasAddedPods = true
	dswp.hasAddedPodsLock.Unlock()
	wait.Until(dswp.populatorLoop, dswp.loopSleepDuration, stopCh)
}

func (dswp *desiredStateOfWorldPopulator) populatorLoop() {
	dswp.findAndAddNewPods()
}

// Iterate through all pods and add to desired state of world if they don't
// exist but should
func (dswp *desiredStateOfWorldPopulator) findAndAddNewPods() {
	// Map unique pod name to outer resource name to PreparedResource.
	preparedResourcesForPod := make(map[cdi.UniquePodName]map[cdi.UniqueResourceName]cache.PreparedResource)
	for _, preparedResource := range dswp.actualStateOfWorld.GetAllPreparedResources() {
		preparedResources, exist := preparedResourcesForPod[preparedResource.PodName]
		if !exist {
			preparedResources = make(map[cdi.UniqueResourceName]cache.PreparedResource)
			preparedResourcesForPod[preparedResource.PodName] = preparedResources
		}
		preparedResources[preparedResource.ResourceName] = preparedResource
	}

	for _, pod := range dswp.podManager.GetPods() {
		if dswp.podStateProvider.ShouldPodContainersBeTerminating(pod.UID) {
			// Do not (re)add resources for pods that can't also be starting containers
			continue
		}
		klog.V(4).Infof("findAndAddNewPods: pod: %+v, resources: %+v", pod, preparedResourcesForPod)
		dswp.processPodResources(pod, preparedResourcesForPod)
	}
}

// podPreviouslyProcessed returns true if the resources for this pod have already
// been processed/reprocessed by the populator. Otherwise, the resources for this pod need to
// be reprocessed.
func (dswp *desiredStateOfWorldPopulator) podPreviouslyProcessed(podName cdi.UniquePodName) bool {
	dswp.pods.RLock()
	defer dswp.pods.RUnlock()
	return dswp.pods.processedPods[podName]
}

// GetUniquePodName returns a unique identifier to reference a pod by
func GetUniquePodName(pod *v1.Pod) cdi.UniquePodName {
	return cdi.UniquePodName(pod.UID)
}

// createResourceSpec creates and returns a resourceSpec object for the
// specified resource. It gets ResourceClass and AllocationResults objects
// to obtain driver name and allocation attributes.
// Returns an error if unable to obtain the resource at this time.
func (dswp *desiredStateOfWorldPopulator) createResourceSpec(
	podResourceClaim v1.PodResourceClaim, pod *v1.Pod) (*cache.ResourceSpec, error) {

	claimName := resourceclaim.Name(pod, &podResourceClaim)

	resourceClaim, err := dswp.kubeClient.CdiV1alpha1().ResourceClaims(pod.Namespace).Get(context.TODO(), claimName, metav1.GetOptions{})

	if err != nil {
		return nil, fmt.Errorf("failed to fetch ResourceClaim %s referenced by pod %s: %v", claimName, pod.Name, err)
	}

	driverName := resourceClaim.Status.DriverName

	return &cache.ResourceSpec{
		Name:                 GetUniqueResourceName(GetUniquePodName(pod), driverName, resourceClaim.GetUID()),
		PluginName:           driverName,
		ResourceClaimUUID:    resourceClaim.GetUID(),
		AllocationAttributes: resourceClaim.Status.Allocation.Attributes}, nil
}

// GetUniqueResourceName returns a unique resource name with pod
// name included. This is useful to generate different names for different pods
// using the same resource.
func GetUniqueResourceName(
	podName cdi.UniquePodName, pluginName string, resourceClaimUID types.UID) cdi.UniqueResourceName {
	return cdi.UniqueResourceName(
		fmt.Sprintf("%s/%v-%s", pluginName, podName, resourceClaimUID))
}

// processPodResources processes the resources in the given pod and adds them to the
// desired state of the world.
func (dswp *desiredStateOfWorldPopulator) processPodResources(
	pod *v1.Pod,
	preparedResourcesForPod map[cdi.UniquePodName]map[cdi.UniqueResourceName]cache.PreparedResource) {

	if pod == nil {
		return
	}

	uniquePodName := GetUniquePodName(pod)
	if dswp.podPreviouslyProcessed(uniquePodName) {
		klog.V(4).Infof("Pod %s has been previously processed, skipping ...", uniquePodName)
		return
	}

	allResourcesAdded := true

	klog.V(4).Infof("processPodResources, spec: %+v", pod.Spec)

	// Process resources for each resource claim defined in pod
	for _, podResourceClaim := range pod.Spec.ResourceClaims {
		klog.V(4).Infof("Processing resource claim %s", podResourceClaim.Name)

		resourceSpec, err := dswp.createResourceSpec(podResourceClaim, pod)
		if err != nil {
			klog.ErrorS(err, "Error processing resource", "pod", klog.KObj(pod), "resourceName", podResourceClaim.Name)
			dswp.desiredStateOfWorld.AddErrorToPod(uniquePodName, err.Error())
			allResourcesAdded = false
			continue
		}

		// Add resource to desired state of world
		err = dswp.desiredStateOfWorld.AddPodToResource(uniquePodName, pod, resourceSpec)
		if err != nil {
			klog.ErrorS(err, "Failed to add resource to desiredStateOfWorld", "pod", klog.KObj(pod), "resourceName", resourceSpec.Name)
			dswp.desiredStateOfWorld.AddErrorToPod(uniquePodName, err.Error())
			allResourcesAdded = false
		} else {
			klog.V(4).InfoS("Added resource to desired state", "pod", klog.KObj(pod), "resourceName", resourceSpec.Name)
		}
		// sync resource
		dswp.actualStateOfWorld.SyncResource(resourceSpec.Name, uniquePodName, resourceSpec.ResourceClaimUUID)
	}

	// some of the resource additions may have failed, should not mark this pod as fully processed
	if allResourcesAdded {
		dswp.markPodProcessed(uniquePodName)
		// Remove any stored errors for the pod, everything went well in this processPodResources
		dswp.desiredStateOfWorld.PopPodErrors(uniquePodName)
	} else if dswp.podHasBeenSeenOnce(uniquePodName) {
		// For the Pod which has been processed at least once, even though some resources
		// may not have been reprocessed successfully this round, we still mark it as processed to avoid
		// processing it at a very high frequency. The pod will be reprocessed when resource manager calls
		// ReprocessPod() which is triggered by SyncPod.
		dswp.markPodProcessed(uniquePodName)
	}
}

func (dswp *desiredStateOfWorldPopulator) ReprocessPod(
	podName cdi.UniquePodName) {
	dswp.markPodProcessingFailed(podName)
}

// markPodProcessingFailed marks the specified pod from processedPods as false to indicate that it failed processing
func (dswp *desiredStateOfWorldPopulator) markPodProcessingFailed(
	podName cdi.UniquePodName) {
	dswp.pods.Lock()
	dswp.pods.processedPods[podName] = false
	dswp.pods.Unlock()
}

// markPodProcessed records that the resources for the specified pod have been
// processed by the populator
func (dswp *desiredStateOfWorldPopulator) markPodProcessed(podName cdi.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	dswp.pods.processedPods[podName] = true
}

// podHasBeenSeenOnce returns true if the pod has been seen by the popoulator
// at least once.
func (dswp *desiredStateOfWorldPopulator) podHasBeenSeenOnce(
	podName cdi.UniquePodName) bool {
	dswp.pods.RLock()
	_, exist := dswp.pods.processedPods[podName]
	dswp.pods.RUnlock()
	return exist
}

func (dswp *desiredStateOfWorldPopulator) HasAddedPods() bool {
	dswp.hasAddedPodsLock.RLock()
	defer dswp.hasAddedPodsLock.RUnlock()
	return dswp.hasAddedPods
}
