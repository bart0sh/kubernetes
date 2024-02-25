/*
Copyright 2024 The Kubernetes Authors.

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
	"time"

	resourcev1alpha2 "k8s.io/api/resource/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/listers/resource/v1alpha2"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/dra/plugin"
)

const (
	// Value controlling monitoring period, i.e. how often does the controller
	// check NodeResourceSlice objects.
	monitorPeriod = 10 * time.Second
)

// InformerManager caches NodeResourceSlice API objects, and provides accessors to the Kubelet.
type informerManager struct {
	informerFactory informers.SharedInformerFactory
	lister          v1alpha2.NodeResourceSliceLister
	hasSynced       func() bool
	client          clientset.Interface
}

// newInformerManager returns a new NodeResourceSlices InformerManager. Run must be called before the manager can be used.
func newInformerManager(ctx context.Context, nodeName types.NodeName, client clientset.Interface) (*informerManager, error) {
	logger := klog.FromContext(ctx)
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
		options.FieldSelector = fields.Set{"nodeName": string(nodeName)}.String()
	}))
	lister := factory.Resource().V1alpha2().NodeResourceSlices().Lister()
	informer := factory.Resource().V1alpha2().NodeResourceSlices().Informer()
	hasSynced := func() bool {
		return informer.HasSynced()
	}

	manager := &informerManager{
		informerFactory: factory,
		lister:          lister,
		hasSynced:       hasSynced,
		client:          client,
	}

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			slice, ok := obj.(*resourcev1alpha2.NodeResourceSlice)
			if !ok {
				return
			}
			logger.V(2).Info("NodeResourceSlice add", "obj", slice)
		},
		UpdateFunc: func(old, new any) {
			sliceOld, ok := old.(*resourcev1alpha2.NodeResourceSlice)
			if !ok {
				return
			}
			sliceNew, ok := new.(*resourcev1alpha2.NodeResourceSlice)
			if !ok {
				return
			}
			logger.V(2).Info("NodeResourceSlice update", "old", sliceOld, "new", sliceNew)
		},
		DeleteFunc: func(obj any) {
			slice, ok := obj.(*resourcev1alpha2.NodeResourceSlice)
			if !ok {
				return
			}
			logger.V(2).Info("NodeResourceSlice delete", "obj", slice)
			if plugin.IsRegistered(slice.DriverName) {
				logger.V(2).Info("Re-create incorrectly removed NodeResourceSlice", "name", slice.Name, "node", slice.NodeName, "driver", slice.DriverName)
				slice.ResourceVersion = ""
				slice, err := client.ResourceV1alpha2().NodeResourceSlices().Create(ctx, slice, metav1.CreateOptions{})
				if err != nil {
					logger.Error(err, "Re-creating incorrectly removed NodeResourceSlice", "name", slice.Name, "node", slice.NodeName, "driver", slice.DriverName)
				}
			}
		},
	})

	if err != nil {
		return nil, fmt.Errorf("while registering event handler on the NodeResourceSlice informer: %w", err)
	}

	return manager, nil
}

// Start starts syncing the NodeResourceSlices cache with the apiserver.
func (m *informerManager) Start(ctx context.Context) {
	m.informerFactory.Start(ctx.Done())
	go wait.UntilWithContext(ctx, m.monitorOrphaned, monitorPeriod)
}

func (m *informerManager) monitorOrphaned(ctx context.Context) {
	logger := klog.FromContext(ctx)
	nrsList, err := m.lister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "Listing NodeResourceSlices")
		return
	}
	for _, slice := range nrsList {
		if !plugin.IsRegistered(slice.DriverName) {
			logger.Info("Delete orphaned NodeResourceSlice", "name", slice.Name, "node", slice.NodeName, "driver", slice.DriverName)
			if err := m.client.ResourceV1alpha2().NodeResourceSlices().Delete(ctx, slice.Name, metav1.DeleteOptions{}); err != nil {
				logger.Error(err, "Deleting orphaned NodeResourceSlice", "name", slice.Name, "node", slice.NodeName, "driver", slice.DriverName)
			}
		}
	}
}
