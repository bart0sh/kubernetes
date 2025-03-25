/*
Copyright 2025 The Kubernetes Authors.

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

package allocation

import (
	"path/filepath"
	"sync"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/allocation/state"
)

// podStatusManagerStateFile is the file name where status manager stores its state
const (
	allocatedPodsStateFile = "allocated_pods_state"
	actuatedPodsStateFile  = "actuated_pods_state"
)

// AllocationManager tracks pod resource allocations.
type Manager interface {
	// GetContainerResourceAllocation returns the AllocatedResources value for the container
	GetContainerResourceAllocation(podUID types.UID, containerName string) (v1.ResourceRequirements, bool)

	// UpdatePodFromAllocation overwrites the pod spec with the allocation.
	// This function does a deep copy only if updates are needed.
	// Returns the updated (or original) pod, and whether there was an allocation stored.
	UpdatePodFromAllocation(pod *v1.Pod) (*v1.Pod, bool)

	// SetAllocatedResources checkpoints the resources allocated to a pod's containers.
	SetAllocatedResources(allocatedPod *v1.Pod) error

	// SetActuatedResources records the actuated resources of the given container (or the entire
	// pod, if actuatedContainer is nil).
	SetActuatedResources(allocatedPod *v1.Pod, actuatedContainer *v1.Container) error

	// GetActuatedResources returns the stored actuated resources for the container, and whether they exist.
	GetActuatedResources(podUID types.UID, containerName string) (v1.ResourceRequirements, bool)

	// RemovePod removes any stored state for the given pod UID.
	RemovePod(uid types.UID)

	// RemoveOrphanedPods removes the stored state for any pods not included in the set of remaining pods.
	RemoveOrphanedPods(remainingPods sets.Set[types.UID])
}

type manager struct {
	allocated state.State
	actuated  state.State
}

func NewManager(checkpointDirectory string) Manager {
	return &manager{
		allocated: newStateImpl(checkpointDirectory, allocatedPodsStateFile),
		actuated:  newStateImpl(checkpointDirectory, actuatedPodsStateFile),
	}
}

func newStateImpl(checkpointDirectory, checkpointName string) state.State {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		return state.NewNoopStateCheckpoint()
	}

	stateImpl, err := state.NewStateCheckpoint(checkpointDirectory, checkpointName)
	if err != nil {
		// This is a critical, non-recoverable failure.
		klog.ErrorS(err, "Failed to initialize allocation checkpoint manager",
			"checkpointPath", filepath.Join(checkpointDirectory, checkpointName))
		panic(err)
	}

	return stateImpl
}

// PodResourceSummary save pod resize info
type PodResourceSummary struct {
	// TODO: Add pod-level resources here once resizing pod-level resources is supported
	// Resources v1.ResourceRequirements
	InitContainers []ContainerAllocation
	Containers     []ContainerAllocation
}

type ContainerAllocation struct {
	Name      string
	Resources v1.ResourceRequirements
}

// AllocationManager manages pod resource allocations with thread-safe operations.
type ResizeAllocationManager struct {
	mutex               sync.RWMutex
	podResizeAllocation map[string]*PodResourceSummary
}

var resizeManager = &ResizeAllocationManager{
	podResizeAllocation: make(map[string]*PodResourceSummary),
}

// NewInMemoryManager returns an allocation manager that doesn't persist state.
// For testing purposes only!
func NewInMemoryManager() Manager {
	return &manager{
		allocated: state.NewStateMemory(nil),
		actuated:  state.NewStateMemory(nil),
	}
}

// GetContainerResourceAllocation returns the last checkpointed AllocatedResources values
// If checkpoint manager has not been initialized, it returns nil, false
func (m *manager) GetContainerResourceAllocation(podUID types.UID, containerName string) (v1.ResourceRequirements, bool) {
	return m.allocated.GetContainerResources(podUID, containerName)
}

// UpdatePodFromAllocation overwrites the pod spec with the allocation.
// This function does a deep copy only if updates are needed.
func (m *manager) UpdatePodFromAllocation(pod *v1.Pod) (*v1.Pod, bool) {
	// TODO(tallclair): This clones the whole cache, but we only need 1 pod.
	allocs := m.allocated.GetPodResourceInfoMap()
	return updatePodFromAllocation(pod, allocs)
}

func GetPodResizeAllocation(pod *v1.Pod) (*PodResourceSummary, bool) {
	resizeManager.mutex.Lock()
	defer resizeManager.mutex.Unlock()

	resizeAllocation, exists := resizeManager.podResizeAllocation[pod.ObjectMeta.Name]
	if !exists {
		return nil, false
	}
	return resizeAllocation, true
}

func ClearPodResizeAllocation(pod *v1.Pod) {
	resizeManager.mutex.Lock()
	defer resizeManager.mutex.Unlock()
	delete(resizeManager.podResizeAllocation, pod.ObjectMeta.Name)
}

func setPodResizeAllocation(pod *v1.Pod, allocationsummary *PodResourceSummary) {
	resizeManager.mutex.Lock()
	defer resizeManager.mutex.Unlock()

	resizeManager.podResizeAllocation[pod.ObjectMeta.Name] = allocationsummary
}

// Compare req1 and req2, if different, store req2 resource to diffReq
func compareAndSaveDiff(req1, req2 v1.ResourceRequirements) v1.ResourceRequirements {
	var diffReq v1.ResourceRequirements

	// Compare Requests resource
	diffReq.Requests = make(v1.ResourceList)
	for key, val1 := range req1.Requests {
		if val2, exists := req2.Requests[key]; !exists || !val2.Equal(val1) {
			diffReq.Requests[key] = val1
		}
	}

	// Compare Limits resource
	diffReq.Limits = make(v1.ResourceList)
	for key, val1 := range req1.Limits {
		if val2, exists := req2.Limits[key]; !exists || !val2.Equal(val1) {
			diffReq.Limits[key] = val1
		}
	}
	return diffReq
}

// Check Requests and Limists are empty
func isResizeResourceEmpty(req v1.ResourceRequirements) bool {
	return len(req.Requests) == 0 && len(req.Limits) == 0
}

// Compare and save Resize resource to PodResizeReqs
func updatePodResizeAllocation(pod *v1.Pod, c v1.Container, req1, req2 v1.ResourceRequirements) {

	diffReq := compareAndSaveDiff(req1, req2)
	if isResizeResourceEmpty(diffReq) {
		return
	}

	containerName := c.Name

	podResizeAlloc, exists := GetPodResizeAllocation(pod)
	if !exists {
		podResizeAlloc = &PodResourceSummary{}
	}

	saveResizeInfo := func(containers []ContainerAllocation) []ContainerAllocation {
		for i, container := range containers {
			if container.Name == containerName {
				containers[i].Resources = diffReq
				return containers
			}
		}
		return append(containers, ContainerAllocation{
			Name:      containerName,
			Resources: diffReq,
		})
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.SidecarContainers) && podutil.IsRestartableInitContainer(&c) {
		podResizeAlloc.InitContainers = saveResizeInfo(podResizeAlloc.InitContainers)
	} else {
		podResizeAlloc.Containers = saveResizeInfo(podResizeAlloc.Containers)
	}

	setPodResizeAllocation(pod, podResizeAlloc)
}

func updatePodFromAllocation(pod *v1.Pod, allocs state.PodResourceInfoMap) (*v1.Pod, bool) {
	allocated, found := allocs[pod.UID]
	if !found {
		return pod, false
	}

	updated := false
	containerAlloc := func(c v1.Container) (v1.ResourceRequirements, bool) {
		if cAlloc, ok := allocated.ContainerResources[c.Name]; ok {
			if !apiequality.Semantic.DeepEqual(c.Resources, cAlloc) {
				// Allocation differs from pod spec, retrieve the allocation
				if !updated {
					// If this is the first update to be performed, cache resize resource
					updatePodResizeAllocation(pod, c, c.Resources, cAlloc)
					// If this is the first update to be performed, copy the pod
					pod = pod.DeepCopy()
					updated = true
				}
				return cAlloc, true
			}
		}
		return v1.ResourceRequirements{}, false
	}

	for i, c := range pod.Spec.Containers {
		if cAlloc, found := containerAlloc(c); found {
			// Allocation differs from pod spec, update
			pod.Spec.Containers[i].Resources = cAlloc
		}
	}
	for i, c := range pod.Spec.InitContainers {
		if cAlloc, found := containerAlloc(c); found {
			// Allocation differs from pod spec, update
			pod.Spec.InitContainers[i].Resources = cAlloc
		}
	}
	return pod, updated
}

// SetAllocatedResources checkpoints the resources allocated to a pod's containers
func (m *manager) SetAllocatedResources(pod *v1.Pod) error {
	return m.allocated.SetPodResourceInfo(pod.UID, allocationFromPod(pod))
}

func allocationFromPod(pod *v1.Pod) state.PodResourceInfo {
	var podAlloc state.PodResourceInfo
	podAlloc.ContainerResources = make(map[string]v1.ResourceRequirements)
	for _, container := range pod.Spec.Containers {
		alloc := *container.Resources.DeepCopy()
		podAlloc.ContainerResources[container.Name] = alloc
	}

	for _, container := range pod.Spec.InitContainers {
		if podutil.IsRestartableInitContainer(&container) {
			alloc := *container.Resources.DeepCopy()
			podAlloc.ContainerResources[container.Name] = alloc
		}
	}

	return podAlloc
}

func (m *manager) RemovePod(uid types.UID) {
	if err := m.allocated.RemovePod(uid); err != nil {
		// If the deletion fails, it will be retried by RemoveOrphanedPods, so we can safely ignore the error.
		klog.V(3).ErrorS(err, "Failed to delete pod allocation", "podUID", uid)
	}

	if err := m.actuated.RemovePod(uid); err != nil {
		// If the deletion fails, it will be retried by RemoveOrphanedPods, so we can safely ignore the error.
		klog.V(3).ErrorS(err, "Failed to delete pod allocation", "podUID", uid)
	}
}

func (m *manager) RemoveOrphanedPods(remainingPods sets.Set[types.UID]) {
	m.allocated.RemoveOrphanedPods(remainingPods)
	m.actuated.RemoveOrphanedPods(remainingPods)
}

func (m *manager) SetActuatedResources(allocatedPod *v1.Pod, actuatedContainer *v1.Container) error {
	if actuatedContainer == nil {
		alloc := allocationFromPod(allocatedPod)
		return m.actuated.SetPodResourceInfo(allocatedPod.UID, alloc)
	}

	return m.actuated.SetContainerResources(allocatedPod.UID, actuatedContainer.Name, actuatedContainer.Resources)
}

func (m *manager) GetActuatedResources(podUID types.UID, containerName string) (v1.ResourceRequirements, bool) {
	return m.actuated.GetContainerResources(podUID, containerName)
}
