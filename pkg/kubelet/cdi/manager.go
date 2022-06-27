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

package cdi

import (
	"errors"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-helpers/cdi/resourceclaim"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/cdi"
	"k8s.io/kubernetes/pkg/kubelet/cdi/cache"
	"k8s.io/kubernetes/pkg/kubelet/cdi/populator"
	"k8s.io/kubernetes/pkg/kubelet/cdi/reconciler"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/pod"
)

const (
	// reconcilerLoopSleepPeriod is the amount of time the reconciler loop waits
	// between successive executions
	reconcilerLoopSleepPeriod = 100 * time.Millisecond

	// podPrepareResourceTimeout is the maximum amount of time the
	// WaitForPreparedResources call will wait for all CDI resources in the specified pod
	// to be prepared. Even though resource preparation operations can take long
	// to complete, we set the timeout to 2 minutes because kubelet
	// will retry in the next sync iteration. This frees the associated
	// goroutine of the pod to process newer updates if needed (e.g., a delete
	// request to the pod).
	// Value is slightly offset from 2 minutes to make timeouts due to this
	// constant recognizable.
	podPrepareResourceTimeout = 2*time.Minute + 3*time.Second

	// podPrepareResourceTimeout is the amount of time the GetResourcesForPod
	// call waits before retrying
	podPrepareResourceRetryInterval = 300 * time.Millisecond

	// desiredStateOfWorldPopulatorLoopSleepPeriod is the amount of time the
	// DesiredStateOfWorldPopulator loop waits between successive executions
	desiredStateOfWorldPopulatorLoopSleepPeriod = 100 * time.Millisecond

	// desiredStateOfWorldPopulatorGetPodStatusRetryDuration is the amount of
	// time the DesiredStateOfWorldPopulator loop waits between successive pod
	// cleanup calls (to prevent calling containerruntime.GetPodStatus too
	// frequently).
	desiredStateOfWorldPopulatorGetPodStatusRetryDuration = 2 * time.Second
)

// ResourceManager runs a set of asynchronous loops that figure out which resources
// need to be prepared based on the pods scheduled on this node and makes it so.
type ResourceManager interface {
	// WaitForPreparedResources processes the resources referenced in the specified
	// pod and blocks until they are all prepared (reflected in
	// actual state of the world).
	// An error is returned if all resources are not prepared within
	// the duration defined in resourcePrepareTimeout.
	WaitForPreparedResources(pod *v1.Pod) error

	// Starts the resource manager and all the asynchronous loops that it controls
	Run(sourcesReady config.SourcesReady, stopCh <-chan struct{})
}

// resourceManager implements the ResourceManager interface
type resourceManager struct {
	// reconciler runs an asynchronous periodic loop to reconcile the
	// desiredStateOfWorld with the actualStateOfWorld by triggering attach,
	// detach, mount, and unmount operations using the operationExecutor.
	reconciler reconciler.Reconciler

	// desiredStateOfWorld is a data structure containing the desired state of
	// the world according to the resource manager: i.e. which resources should
	// be prepared and which pods are referencing the resources).
	// The data structure is populated by the desired state of the world
	// populator using the kubelet pod manager.
	desiredStateOfWorld cache.DesiredStateOfWorld

	// actualStateOfWorld is a data structure containing the actual state of
	// the world according to the manager: i.e. which resources are prepared
	// on this node and which pods use the resources.
	// The data structure is populated upon successful completion of
	// prepare and unprepare actions triggered by the reconciler.
	actualStateOfWorld cache.ActualStateOfWorld

	// desiredStateOfWorldPopulator runs an asynchronous periodic loop to
	// populate the desiredStateOfWorld using the kubelet PodManager.
	desiredStateOfWorldPopulator populator.DesiredStateOfWorldPopulator
}

// podStateProvider can determine if a pod is is going to be terminated
type podStateProvider interface {
	ShouldPodContainersBeTerminating(k8stypes.UID) bool
	ShouldPodRuntimeBeRemoved(k8stypes.UID) bool
}

// NewResourceManager returns a new concrete instance implementing the
// ResourceManager interface.
func NewResourceManager(
	nodeName k8stypes.NodeName,
	podManager pod.Manager,
	podStateProvider podStateProvider,
	kubeClient clientset.Interface,
	kubeContainerRuntime container.Runtime) ResourceManager {

	rm := &resourceManager{
		desiredStateOfWorld: cache.NewDesiredStateOfWorld(), // TODO: add parameter resourcePluginManager
		actualStateOfWorld:  cache.NewActualStateOfWorld(),
	}

	rm.desiredStateOfWorldPopulator = populator.NewDesiredStateOfWorldPopulator(
		kubeClient,
		desiredStateOfWorldPopulatorLoopSleepPeriod,
		desiredStateOfWorldPopulatorGetPodStatusRetryDuration,
		podManager,
		podStateProvider,
		rm.desiredStateOfWorld,
		rm.actualStateOfWorld,
		kubeContainerRuntime)

	rm.reconciler = reconciler.NewReconciler(
		nodeName,
		rm.desiredStateOfWorld,
		rm.actualStateOfWorld,
		rm.desiredStateOfWorldPopulator.HasAddedPods,
		reconcilerLoopSleepPeriod,
	)

	return rm
}

func (rm *resourceManager) Run(sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	klog.InfoS("Starting Kubelet Resource Manager")
	defer runtime.HandleCrash()

	go rm.desiredStateOfWorldPopulator.Run(sourcesReady, stopCh)
	klog.V(2).InfoS("The desired_state_of_world populator starts")

	go rm.reconciler.Run(stopCh)

	<-stopCh
	klog.InfoS("Shutting down Kubelet Resource Manager")
}

// verifyResourcesPreparedFunc returns a method that returns true when all expected
// resources are prepared.
func (rm *resourceManager) verifyResourcesPreparedFunc(podName cdi.UniquePodName, expectedResources []cdi.UniqueResourceName) wait.ConditionFunc {
	return func() (done bool, err error) {
		if errs := rm.desiredStateOfWorld.PopPodErrors(podName); len(errs) > 0 {
			return true, errors.New(strings.Join(errs, "; "))
		}
		return len(rm.getUnpreparedResources(podName, expectedResources)) == 0, nil
	}
}

func (rm *resourceManager) WaitForPreparedResources(pod *v1.Pod) error {
	if pod == nil {
		return nil
	}

	if len(pod.Spec.ResourceClaims) == 0 {
		return nil
	}

	expectedResources := getExpectedResources(pod)
	if len(expectedResources) == 0 {
		// No resources to verify
		return nil
	}

	klog.V(3).Infof("Waiting for resources to be prepared for pod %s %s", pod.Name, pod.Status.Phase)
	uniquePodName := populator.GetUniquePodName(pod)

	rm.desiredStateOfWorldPopulator.ReprocessPod(uniquePodName)

	err := wait.PollImmediate(
		podPrepareResourceRetryInterval,
		podPrepareResourceTimeout,
		rm.verifyResourcesPreparedFunc(uniquePodName, expectedResources))

	if err != nil {
		unpreparedResources := rm.getUnpreparedResources(uniquePodName, expectedResources)
		if len(unpreparedResources) == 0 {
			return nil
		}

		return fmt.Errorf("unprepared resources=%v: %s", unpreparedResources, err)
	}

	klog.V(3).InfoS("All resources are prepared for pod %s %s", pod.Name, pod.Status.Phase)
	return nil
}

// getExpectedResources returns a list of resources that must be prepared in order to
// consider the resources setup step for this pod satisfied.
func getExpectedResources(pod *v1.Pod) []cdi.UniqueResourceName {
	resources := []cdi.UniqueResourceName{}

	for _, resourceClaim := range pod.Spec.ResourceClaims {
		resources = append(resources, populator.GetUniqueResourceName(populator.GetUniquePodName(pod), resourceclaim.Name(pod, &resourceClaim)))
	}

	klog.V(3).InfoS("expected resources for pod %s: %s", pod.Name, resources)

	return resources
}

// getUnpreparedResources fetches the current list of prepared resources
// from the actual state of the world, and uses it to process the list of
// expectedResources. It returns a list of unprepared resources.
func (rm *resourceManager) getUnpreparedResources(podName cdi.UniquePodName, expectedResources []cdi.UniqueResourceName) []cdi.UniqueResourceName {
	preparedResources := map[cdi.UniqueResourceName]bool{}
	for _, preparedResource := range rm.actualStateOfWorld.GetPreparedResourcesForPod(podName) {
		preparedResources[preparedResource.ResourceName] = true
	}
	return filterUnpreparedResources(preparedResources, expectedResources)
}

// filterUnpreparedResources adds each element of expectedResources that is not in
// preparedResources to a list of unpreparedResources and returns it.
func filterUnpreparedResources(preparedResources map[cdi.UniqueResourceName]bool, expectedResources []cdi.UniqueResourceName) []cdi.UniqueResourceName {
	unpreparedResources := []cdi.UniqueResourceName{}
	for _, expectedResource := range expectedResources {
		if _, exists := preparedResources[expectedResource]; !exists {
			unpreparedResources = append(unpreparedResources, expectedResource)
		}
	}
	return unpreparedResources
}
