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

// Package app does all of the work necessary to configure and run a
// Kubernetes app process.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"k8s.io/kubernetes/test/integration/cdi/example-driver/controller"
)

func runController(ctx context.Context, clientset kubernetes.Interface, driverName string, workers int) {
	driver := &exampleDriver{
		clientset: clientset,
	}
	informerFactory := informers.NewSharedInformerFactory(clientset, 0 /* resync period */)
	ctrl := controller.New(ctx, driverName, driver, clientset, informerFactory)
	informerFactory.Start(ctx.Done())
	ctrl.Run(workers)
}

type exampleDriver struct {
	clientset kubernetes.Interface
}
type parameters map[string]string

var _ controller.Driver = exampleDriver{}

func (d exampleDriver) GetClassParameters(ctx context.Context, class *corev1.ResourceClass) (interface{}, error) {
	// TODO: read config map, but from what namespace?!
	var p parameters
	return p, nil
}

func (d exampleDriver) GetClaimParameters(ctx context.Context, claim *corev1.ResourceClaim, class *corev1.ResourceClass, classParameters interface{}) (interface{}, error) {
	if claim.Spec.Parameters != nil {
		if claim.Spec.Parameters.APIVersion != "v1" ||
			claim.Spec.Parameters.Kind != "ConfigMap" {
			return nil, fmt.Errorf("claim parameters are only supported in APIVersion v1, Kind ConfigMap, got: %v", claim.Spec.Parameters)
		}
		return d.readParametersFromConfigMap(ctx, claim.Namespace, claim.Spec.Parameters.Name)
	}
	return nil, nil
}

func (d exampleDriver) readParametersFromConfigMap(ctx context.Context, namespace, name string) (parameters, error) {
	configMap, err := d.clientset.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get config map: %v", err)
	}
	return parameters(configMap.Data), nil
}

// Allocate simply copies parameters as JSON map into ResourceHandle.
func (d exampleDriver) Allocate(ctx context.Context, claim *corev1.ResourceClaim, claimParameters interface{}, class *corev1.ResourceClass, classParameters interface{}, selectedNode string) (*corev1.AllocationResult, error) {
	allocation := &corev1.AllocationResult{
		SharedResource: true,
	}
	p := parameters{}
	toEnvVars("user", claimParameters.(parameters), p)
	toEnvVars("admin", classParameters.(parameters), p)
	data, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode env variables: %v", err)
	}
	allocation.ResourceHandle = string(data)
	return allocation, nil
}

func (d exampleDriver) StopAllocation(ctx context.Context, claim *corev1.ResourceClaim) error {
	return nil
}

func (d exampleDriver) Deallocate(ctx context.Context, claim *corev1.ResourceClaim) error {
	return nil
}

func (d exampleDriver) UnsuitableNodes(ctx context.Context, pod *v1.Pod, claims []*controller.ClaimAllocation, potentialNodes []string) error {
	// All nodes are suitable.
	return nil
}

func toEnvVars(what string, from, to parameters) {
	for key, value := range from {
		to[what+"_"+strings.ToLower(key)] = value
	}
}
