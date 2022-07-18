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

package dra

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/container-orchestrated-devices/container-device-interface/pkg/cdi"
	cadvisorapi "github.com/google/cadvisor/info/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-helpers/cdi/resourceclaim"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	dra "k8s.io/kubernetes/pkg/kubelet/cm/dra/plugin"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// ManagerImpl is the structure in charge of managing Device Plugins.
type ManagerImpl struct {
	sync.Mutex

	// List of NUMA Nodes available on the underlying machine
	numaNodes []int

	// Store of Topology Affinties that the Device Manager can query.
	topologyAffinityStore topologymanager.Store

	// podResources contains resources referenced by the pod
	podResources *podResources

	// activePods is a method for listing active pods on the node
	activePods ActivePodsFunc

	// sourcesReady provides the readiness of kubelet configuration sources such as apiserver update readiness.
	// We use it to determine when we can purge inactive pods from checkpointed state.
	sourcesReady config.SourcesReady

	// Checkpoint manager
	checkpointdir     string
	checkpointManager checkpointmanager.CheckpointManager

	// pendingAdmissionPod contain the pod during the admission phase
	pendingAdmissionPod *v1.Pod

	// KubeClient reference
	kubeClient clientset.Interface
}

type sourcesReadyStub struct{}

func (s *sourcesReadyStub) AddSource(source string) {}
func (s *sourcesReadyStub) AllReady() bool          { return true }

// NewManagerImpl creates a new manager.
func NewManagerImpl(topology []cadvisorapi.Node, topologyAffinityStore topologymanager.Store, kubeClient clientset.Interface) (*ManagerImpl, error) {
	klog.V(2).InfoS("Creating DRA manager")

	var numaNodes []int
	for _, node := range topology {
		numaNodes = append(numaNodes, node.Id)
	}

	manager := &ManagerImpl{
		podResources:          newPodResources(),
		numaNodes:             numaNodes,
		topologyAffinityStore: topologyAffinityStore,
		kubeClient:            kubeClient,
	}

	// The following structures are populated with real implementations in manager.Start()
	// Before that, initializes them to perform no-op operations.
	manager.activePods = func() []*v1.Pod { return []*v1.Pod{} }
	manager.sourcesReady = &sourcesReadyStub{}

	// Initialize checkpoint manager
	var err error
	manager.checkpointdir = DRACheckpointDir
	manager.checkpointManager, err = checkpointmanager.NewCheckpointManager(manager.checkpointdir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint manager: %v", err)
	}

	return manager, nil
}

func (m *ManagerImpl) setPodPendingAdmission(pod *v1.Pod) {
	m.Lock()
	defer m.Unlock()

	m.pendingAdmissionPod = pod
}

// prepareContainerResources attempts to prepare all of required resource
// plugin resources for the input container, issues an NodePrepareResource rpc request
// for each new resource requirement, processes their responses and updates the cached
// containerResources on success.
func (m *ManagerImpl) prepareContainerResources(pod *v1.Pod, container *v1.Container) error {
	podResourcesUpdated := false
	// Process resources for each resource claim referenced by pod
	for _, podResourceClaim := range pod.Spec.ResourceClaims {
		klog.V(3).InfoS("Processing resource claim", podResourceClaim.Name)

		// Update pod resources cache to garbage collect any stranded resources
		// before calling resource plugin APIs.
		if !podResourcesUpdated {
			m.UpdatePodResources()
			podResourcesUpdated = true
		}

		claimName := resourceclaim.Name(pod, &podResourceClaim)

		if m.podResources.prepared(pod.UID, container.Name, claimName) {
			// resource is already prepared, skipping
			continue
		}

		// Query claim object from the API server
		resourceClaim, err := m.kubeClient.CoreV1().ResourceClaims(pod.Namespace).Get(context.TODO(), claimName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to fetch ResourceClaim %s referenced by pod %s: %+v", claimName, pod.Name, err)
		}

		// Call NodePrepareResource RPC
		driverName := resourceClaim.Status.DriverName
		client, err := dra.NewDRAPluginClient(driverName)
		if err != nil || client == nil {
			return fmt.Errorf("failed to get DRA Plugin client for plugin name %s, err=%+v", driverName, err)
		}

		resourceName := fmt.Sprintf("%s-%s", pod.UID, claimName)
		response, err := client.NodePrepareResource(context.Background(), pod.Namespace, resourceClaim.GetUID(), resourceName, resourceClaim.Status.Allocation.ResourceHandle)
		if err != nil {
			return fmt.Errorf("NodePrepareResource failed, pod: %s, err: %+v", pod.Name, err)
		}
		klog.V(3).Infof("NodePrepareResource: response: %+v", response)

		annotations := []kubecontainer.Annotation{}
		for _, device := range response.CdiDevice {
			deviceID := strings.Replace(strings.Replace(device, "/", "-", -1), "=", "-", -1)
			key, err := cdi.AnnotationKey(driverName, deviceID)
			if err != nil {
				return fmt.Errorf("could not get annotaion key, plugin: %s, err: %+v", driverName, err)
			}
			value, err := cdi.AnnotationValue(response.CdiDevice)
			if err != nil {
				return fmt.Errorf("could not get annotation value, devices: %s", response.CdiDevice)
			}

			annotations = append(annotations, kubecontainer.Annotation{Name: key, Value: value})
			klog.V(3).Infof("pod: %s, container: %s, annotations: %s", pod.Name, container.Name, annotations)
		}

		containerClaimName := podResourceClaim.Name
		if podResourceClaim.Claim.ResourceClaimName != nil {
			containerClaimName = *podResourceClaim.Claim.ResourceClaimName
		}

		// Cache prepared resource
		m.podResources.insert(
			pod.UID,
			container.Name,
			containerClaimName,
			&resource{
				name:                 resourceName,
				resourcePluginClient: client,
				annotations:          annotations,
			})
	}

	return nil
}

// Allocate calls plugin NodePrepareResource from the registered device plugins.
func (m *ManagerImpl) Allocate(pod *v1.Pod, container *v1.Container) error {
	// The pod is during the admission phase. We need to save the pod to avoid it
	// being cleaned before the admission ended
	m.setPodPendingAdmission(pod)

	// Prepare resources for init containers first as we know the caller always loops
	// through init containers before looping through app containers. Should the caller
	// ever change those semantics, this logic will need to be amended.
	for _, initContainer := range pod.Spec.InitContainers {
		if container.Name == initContainer.Name {
			if err := m.prepareContainerResources(pod, container); err != nil {
				return err
			}
			return nil
		}
	}
	if err := m.prepareContainerResources(pod, container); err != nil {
		return err
	}
	return nil
}

// UpdateAllocatedDevices frees any Devices that are bound to terminated pods.
func (m *ManagerImpl) UpdatePodResources() {
	if !m.sourcesReady.AllReady() {
		return
	}

	m.Lock()
	defer m.Unlock()

	activeAndAdmittedPods := m.activePods()
	if m.pendingAdmissionPod != nil {
		activeAndAdmittedPods = append(activeAndAdmittedPods, m.pendingAdmissionPod)
	}

	podsToBeRemoved := m.podResources.pods()
	for _, pod := range activeAndAdmittedPods {
		podsToBeRemoved.Delete(string(pod.UID))
	}
	if len(podsToBeRemoved) <= 0 {
		return
	}
	klog.V(3).InfoS("Pods to be removed", "podUIDs", podsToBeRemoved.List())
	m.podResources.delete(podsToBeRemoved.List())
}

func (m *ManagerImpl) GetCDIAnnotations(pod *v1.Pod, container *v1.Container) []kubecontainer.Annotation {
	annotations := []kubecontainer.Annotation{}
	for _, claim := range container.Resources.Claims {
		resource := m.podResources.get(pod.UID, container.Name, claim)
		klog.V(3).Infof("GetCDIAnnotations: claim %s: add resource annotations: %+v", resource.annotations)
		annotations = append(annotations, resource.annotations...)
	}
	return annotations
}
