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
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cdi/cache"
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
			klog.V(4).InfoS("Starting operationExecutor.PrepareResource", "pod", klog.KObj(resourceToPrepare.Pod), "resource", resourceToPrepare)

			go func(resourceToPrepare cache.ResourceToPrepare) {
				err := resourceToPrepare.ResourcePluginClient.NodePrepareResource(
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
