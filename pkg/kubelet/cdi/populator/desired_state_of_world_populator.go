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
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
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
			processedPods: make(map[cache.UniquePodName]bool)},
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
	processedPods map[cache.UniquePodName]bool
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
	preparedResourcesForPod := make(map[cache.UniquePodName]map[cache.UniqueResourceName]cache.PreparedResource)
	for _, preparedResource := range dswp.actualStateOfWorld.GetAllPreparedResources() {
		preparedResources, exist := preparedResourcesForPod[preparedResource.PodName]
		if !exist {
			preparedResources = make(map[cache.UniqueResourceName]cache.PreparedResource)
			preparedResourcesForPod[preparedResource.PodName] = preparedResources
		}
		preparedResources[preparedResource.ResourceName] = preparedResource
	}

	for _, pod := range dswp.podManager.GetPods() {
		if dswp.podStateProvider.ShouldPodContainersBeTerminating(pod.UID) {
			// Do not (re)add volumes for pods that can't also be starting containers
			continue
		}
		klog.V(4).Infof("findAndAddNewPods: pod: %+v, resources: %+v", pod, preparedResourcesForPod)
		dswp.processPodResources(pod, preparedResourcesForPod)
	}
}

// podPreviouslyProcessed returns true if the volumes for this pod have already
// been processed/reprocessed by the populator. Otherwise, the resources for this pod need to
// be reprocessed.
func (dswp *desiredStateOfWorldPopulator) podPreviouslyProcessed(
	podName cache.UniquePodName) bool {
	dswp.pods.RLock()
	defer dswp.pods.RUnlock()

	return dswp.pods.processedPods[podName]
}

// GetUniquePodName returns a unique identifier to reference a pod by
func GetUniquePodName(pod *v1.Pod) cache.UniquePodName {
	return cache.UniquePodName(pod.UID)
}

// processPodResources processes the resources in the given pod and adds them to the
// desired state of the world.
func (dswp *desiredStateOfWorldPopulator) processPodResources(
	pod *v1.Pod,
	preparedResourcesForPod map[cache.UniquePodName]map[cache.UniqueResourceName]cache.PreparedResource) {
	if pod == nil {
		return
	}

	uniquePodName := GetUniquePodName(pod)
	if dswp.podPreviouslyProcessed(uniquePodName) {
		return
	}

	// Process resources for each resource claim defined in pod
	for _, podResourceClaim := range pod.Spec.ResourceClaims {
		klog.V(4).Infof("Processing resource claim %s", podResourceClaim.Name)
	}
}
