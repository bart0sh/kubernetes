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

// Package reconciler implements interfaces that attempt to reconcile the
// desired state of the world with the actual state of the world by triggering
// relevant actions (prepare resource, unprepare resource).
package reconciler

import (
	"context"
	"strings"
	"time"

	codcdi "github.com/container-orchestrated-devices/container-device-interface/pkg/cdi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/cdi"
	"k8s.io/kubernetes/pkg/kubelet/cdi/cache"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// Reconciler runs a periodic loop to reconcile the desired state of the world
// with the actual state of the world by triggering preparere and unprepare
// resource operations.
type Reconciler interface {
	// Starts running the reconciliation loop which executes periodically, checks
	// if resources that should be prepared are prepared and resources that should
	// be unprepared are unprepared. If not, it will trigger prepare/unprepare
	// operations to rectify.
	Run(stopCh <-chan struct{})

	// StatesHasBeenSynced returns true only after syncStates process starts to sync
	// states at least once after kubelet starts
	StatesHasBeenSynced() bool

	// GetCDIAnnotations checks whether we have cached resource
	// for the passed-in <pod, container> and returns its container annotations or
	// empty map if resource is not cached
	GetCDIAnnotations(pod *v1.Pod, container *v1.Container) []kubecontainer.Annotation
}

// NewReconciler returns a new instance of Reconciler.
// nodeName - the Name for this node
func NewReconciler(nodeName types.NodeName, desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld, populatorHasAddedPods func() bool, loopSleepDuration time.Duration) Reconciler {
	return &reconciler{
		nodeName:              nodeName,
		desiredStateOfWorld:   desiredStateOfWorld,
		actualStateOfWorld:    actualStateOfWorld,
		populatorHasAddedPods: populatorHasAddedPods,
		loopSleepDuration:     loopSleepDuration,
		timeOfLastSync:        time.Time{},
	}
}

type reconciler struct {
	nodeName              types.NodeName
	actualStateOfWorld    cache.ActualStateOfWorld
	desiredStateOfWorld   cache.DesiredStateOfWorld
	populatorHasAddedPods func() bool
	loopSleepDuration     time.Duration
	timeOfLastSync        time.Time
}

func (rc *reconciler) Run(stopCh <-chan struct{}) {
	wait.Until(rc.reconciliationLoopFunc(), rc.loopSleepDuration, stopCh)
}

func (rc *reconciler) reconciliationLoopFunc() func() {
	return func() {
		rc.reconcile()

		// Sync the state with the reality once after all existing pods are added to the desired state from all sources.
		// Otherwise, the reconstruct process may clean up pods' resources that are still in use because
		// desired state of world does not contain a complete list of pods.
		if rc.populatorHasAddedPods() && !rc.StatesHasBeenSynced() {
			klog.InfoS("Reconciler: start to sync state")
			rc.sync()
		}
	}
}

func (rc *reconciler) reconcile() {
	// Unprepare is triggered before prepare so that a resource that was
	// referenced by a pod that was deleted and is now referenced by another
	// pod is unprepared from the first pod before being prepared to the new
	// pod.
	rc.unprepareResources()

	// Next we prepare required resources.
	rc.prepareResources()
}

func (rc *reconciler) unprepareResources() {
	// Ensure resources that should be unprepared are unprepared.
	for _, preparedResource := range rc.actualStateOfWorld.GetAllPreparedResources() {
		if !rc.desiredStateOfWorld.PodExistsInResource(preparedResource.PodName, preparedResource.ResourceName) {
			// Resource is prepared, unprepare it
			klog.V(5).InfoS("Reconciler: unpreparing prepared resource %+v", preparedResource)
		}
	}
}

func (rc *reconciler) prepareResources() {
	// Ensure resources that should be prepared are prepared.
	for _, resourceToPrepare := range rc.desiredStateOfWorld.GetResourcesToPrepare() {

		resPrepared, err := rc.actualStateOfWorld.PodExistsInResource(resourceToPrepare.PodName, resourceToPrepare.ResourceName)

		if !resPrepared || err != nil {
			klog.V(4).Infof("calling NodePrepareResource: pod: %p, resource: %+v", resourceToPrepare.Pod, resourceToPrepare)

			go func(resourceToPrepare cache.ResourceToPrepare) {
				response, err := resourceToPrepare.ResourcePluginClient.NodePrepareResource(
					context.Background(),
					resourceToPrepare.Pod.Namespace,
					resourceToPrepare.ResourceSpec.ResourceClaimUUID,
					resourceToPrepare.ResourceSpec.Name,
					resourceToPrepare.ResourceSpec.AllocationAttributes,
				)
				if err != nil {
					klog.ErrorS(err, "NodePrepareResource failed", "pod", klog.KObj(resourceToPrepare.Pod))
					return
				}

				klog.V(4).Infof("NodePreparResource: response: %+v", response)

				for _, device := range response.CdiDevice {
					deviceID := strings.Replace(strings.Replace(device, "/", "-", -1), "=", "-", -1)
					key, err := codcdi.AnnotationKey(resourceToPrepare.ResourceSpec.PluginName, deviceID)
					if err != nil {
						klog.ErrorS(err, "Could not get annotaion key", "plugin", resourceToPrepare.ResourceSpec.PluginName, "device id", deviceID)
						return
					}
					value, err := codcdi.AnnotationValue(response.CdiDevice)
					if err != nil {
						klog.ErrorS(err, "Could not get annotaion value", "devices", response.CdiDevice)
						return
					}

					val, ok := resourceToPrepare.ResourceSpec.Annotations[key]
					if !ok || val != value {
						resourceToPrepare.ResourceSpec.Annotations[key] = value
						klog.V(4).Infof("prepareResource: pod: %+v: updated annotations: %+v", resourceToPrepare.PodName, resourceToPrepare.ResourceSpec.Annotations)
					}
				}

				err = rc.actualStateOfWorld.MarkResourceAsPrepared(resourceToPrepare)
				if err != nil {
					klog.ErrorS(err, "Could not add prepared resource information to actual state of world", "pod", resourceToPrepare.PodName, "resource", resourceToPrepare.ResourceName)
					return
				}

			}(resourceToPrepare)
		}
	}
}

// sync process tries to observe the real world by scanning all pods' resources.
// If the actual and desired state of worlds are not consistent with the observed world, it means that some
// prepared resources are left out probably during kubelet restart. This process will reconstruct
// the resources and update the actual and desired states.
func (rc *reconciler) sync() {
	defer rc.updateLastSyncTime()
	rc.syncStates()
}

func (rc *reconciler) updateLastSyncTime() {
	rc.timeOfLastSync = time.Now()
}

func (rc *reconciler) StatesHasBeenSynced() bool {
	return !rc.timeOfLastSync.IsZero()
}

// syncStates scans the volume directories under the given pod directory.
// If the volume is not in desired state of world, this function will reconstruct
// the volume related information and put it in both the actual and desired state of worlds.
// For some volume plugins that cannot support reconstruction, it will clean up the existing
// mount points since the volume is no long needed (removed from desired state)
func (rc *reconciler) syncStates() {
	klog.InfoS("Reconciler: start to sync state")
}

func (rc *reconciler) GetCDIAnnotations(pod *v1.Pod, container *v1.Container) []kubecontainer.Annotation {
	annotations := []kubecontainer.Annotation{}
	for _, claim := range container.Resources.Claims {
		klog.V(4).Infof("GetCDIAnnotations: claim %s", claim)
		for _, resource := range rc.actualStateOfWorld.GetPreparedResourcesForPod(cdi.UniquePodName(pod.UID)) {
			klog.V(4).Infof("GetCDIAnnotations: prepared resource: %+v", resource)
			if resource.ContainerClaimName == claim {
				klog.V(4).Infof("GetCDIAnnotations: add resource annotations: %+v", resource.Annotations)
				annotations = append(annotations, resource.Annotations...)
			}
		}
	}
	return annotations
}
