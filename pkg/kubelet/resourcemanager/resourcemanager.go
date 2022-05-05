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

package resourcemanager

import (
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/resourcemanager/reconciler"
)

// ResourceManager runs a set of asynchronous loops that figure out which resources
// need to be prepared based on the pods scheduled on this node and makes it so.
type ResourceManager interface {
	// Starts the resource manager and all the asynchronous loops that it controls
	Run(sourcesReady config.SourcesReady, stopCh <-chan struct{})
}

// resourceManager implements the ResourceManager interface
type resourceManager struct {
	// reconciler runs an asynchronous periodic loop to reconcile the
	// desiredStateOfWorld with the actualStateOfWorld by triggering attach,
	// detach, mount, and unmount operations using the operationExecutor.
	reconciler reconciler.Reconciler
}

// NewResourceManager returns a new concrete instance implementing the
// ResourceManager interface.
func NewResourceManager(nodeName k8stypes.NodeName) ResourceManager {
	rm := &resourceManager{}

	rm.reconciler = reconciler.NewReconciler(nodeName)
	return rm
}

func (rm *resourceManager) Run(sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	klog.InfoS("Starting Kubelet Resource Manager")
	defer runtime.HandleCrash()

	go rm.reconciler.Run(stopCh)

	<-stopCh
	klog.InfoS("Shutting down Kubelet Resource Manager")
}
