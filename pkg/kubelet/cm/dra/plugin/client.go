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

package plugin

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	resourcev1alpha2 "k8s.io/api/resource/v1alpha2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	counterv1alpha1 "k8s.io/dynamic-resource-allocation/apis/counter/v1alpha1"
	"k8s.io/klog/v2"
	drapbv1alpha2 "k8s.io/kubelet/pkg/apis/dra/v1alpha2"
	drapb "k8s.io/kubelet/pkg/apis/dra/v1alpha3"
)

const (
	PluginClientTimeout    = 45 * time.Second
	ResourceCapacitySuffix = "-resourcecapacity"
)

type (
	nodeResourceManager interface {
		Prepare(context.Context, *grpc.ClientConn, *plugin, *drapb.NodePrepareResourcesRequest) (*drapb.NodePrepareResourcesResponse, error)
		Unprepare(context.Context, *grpc.ClientConn, *plugin, *drapb.NodeUnprepareResourcesRequest) (*drapb.NodeUnprepareResourcesResponse, error)
		ResourceCapacity(context.Context, *grpc.ClientConn, *plugin, *drapb.ResourceCapacityRequest) (drapb.Node_ResourceCapacityClient, error)
	}

	v1alpha2NodeResourceManager struct{}
	v1alpha3NodeResourceManager struct{}
)

var nodeResourceManagers = map[string]nodeResourceManager{
	v1alpha2Version: v1alpha2NodeResourceManager{},
	v1alpha3Version: v1alpha3NodeResourceManager{},
}

// These 2 variables are used to create/update NodeResourceCapacity on API server
var kubeClient kubernetes.Interface = nil
var nodeName string

func SetKubeClient(client kubernetes.Interface, node types.NodeName) {
	kubeClient = client
	nodeName = string(node)
}

func (v1alpha2rm v1alpha2NodeResourceManager) Prepare(ctx context.Context, conn *grpc.ClientConn, _ *plugin, req *drapb.NodePrepareResourcesRequest) (*drapb.NodePrepareResourcesResponse, error) {
	nodeClient := drapbv1alpha2.NewNodeClient(conn)
	response := &drapb.NodePrepareResourcesResponse{
		Claims: make(map[string]*drapb.NodePrepareResourceResponse),
	}

	for _, claim := range req.Claims {
		res, err := nodeClient.NodePrepareResource(ctx,
			&drapbv1alpha2.NodePrepareResourceRequest{
				Namespace:      claim.Namespace,
				ClaimUid:       claim.Uid,
				ClaimName:      claim.Name,
				ResourceHandle: claim.ResourceHandle,
			})
		result := &drapb.NodePrepareResourceResponse{}
		if err != nil {
			result.Error = err.Error()
		} else {
			result.CDIDevices = res.CdiDevices
		}
		response.Claims[claim.Uid] = result
	}

	return response, nil
}

func (v1alpha2rm v1alpha2NodeResourceManager) Unprepare(ctx context.Context, conn *grpc.ClientConn, _ *plugin, req *drapb.NodeUnprepareResourcesRequest) (*drapb.NodeUnprepareResourcesResponse, error) {
	nodeClient := drapbv1alpha2.NewNodeClient(conn)
	response := &drapb.NodeUnprepareResourcesResponse{
		Claims: make(map[string]*drapb.NodeUnprepareResourceResponse),
	}

	for _, claim := range req.Claims {
		_, err := nodeClient.NodeUnprepareResource(ctx,
			&drapbv1alpha2.NodeUnprepareResourceRequest{
				Namespace:      claim.Namespace,
				ClaimUid:       claim.Uid,
				ClaimName:      claim.Name,
				ResourceHandle: claim.ResourceHandle,
			})
		result := &drapb.NodeUnprepareResourceResponse{}
		if err != nil {
			result.Error = err.Error()
		}
		response.Claims[claim.Uid] = result
	}

	return response, nil
}

func (v1alpha2rm v1alpha2NodeResourceManager) ResourceCapacity(context.Context, *grpc.ClientConn, *plugin, *drapb.ResourceCapacityRequest) (drapb.Node_ResourceCapacityClient, error) {
	return nil, fmt.Errorf("ResourceCapacity rpc is not implemented for this version of DRA plugin")
}

func (v1alpha3rm v1alpha3NodeResourceManager) Prepare(ctx context.Context, conn *grpc.ClientConn, p *plugin, req *drapb.NodePrepareResourcesRequest) (*drapb.NodePrepareResourcesResponse, error) {
	nodeClient := drapb.NewNodeClient(conn)
	response, err := nodeClient.NodePrepareResources(ctx, req)
	if err != nil {
		status, _ := grpcstatus.FromError(err)
		if status.Code() == grpccodes.Unimplemented {
			p.setVersion(v1alpha2Version)
			return nodeResourceManagers[v1alpha2Version].Prepare(ctx, conn, p, req)
		}
		return nil, err
	}

	return response, nil
}

func (v1alpha3rm v1alpha3NodeResourceManager) Unprepare(ctx context.Context, conn *grpc.ClientConn, p *plugin, req *drapb.NodeUnprepareResourcesRequest) (*drapb.NodeUnprepareResourcesResponse, error) {
	nodeClient := drapb.NewNodeClient(conn)
	response, err := nodeClient.NodeUnprepareResources(ctx, req)
	if err != nil {
		status, _ := grpcstatus.FromError(err)
		if status.Code() == grpccodes.Unimplemented {
			p.setVersion(v1alpha2Version)
			return nodeResourceManagers[v1alpha2Version].Unprepare(ctx, conn, p, req)
		}
		return nil, err
	}

	return response, nil
}

func (v1alpha3rm v1alpha3NodeResourceManager) ResourceCapacity(ctx context.Context, conn *grpc.ClientConn, p *plugin, req *drapb.ResourceCapacityRequest) (drapb.Node_ResourceCapacityClient, error) {
	nodeClient := drapb.NewNodeClient(conn)
	return nodeClient.ResourceCapacity(ctx, req)
}

func NewDRAPluginClient(pluginName string) (drapb.NodeClient, error) {
	if pluginName == "" {
		return nil, fmt.Errorf("plugin name is empty")
	}

	existingPlugin := draPlugins.get(pluginName)
	if existingPlugin == nil {
		return nil, fmt.Errorf("plugin name %s not found in the list of registered DRA plugins", pluginName)
	}

	return existingPlugin, nil
}

func (p *plugin) NodePrepareResources(
	ctx context.Context,
	req *drapb.NodePrepareResourcesRequest,
	opts ...grpc.CallOption,
) (*drapb.NodePrepareResourcesResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info(log("calling NodePrepareResources rpc"), "request", req)

	conn, err := p.getOrCreateGRPCConn()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, PluginClientTimeout)
	defer cancel()

	version := p.getVersion()
	resourceManager, exists := nodeResourceManagers[version]
	if !exists {
		err := fmt.Errorf("unsupported plugin version: %s", version)
		logger.V(4).Info(log("done calling NodePrepareResources rpc"), "response", nil, "err", err)
		return nil, err
	}

	response, err := resourceManager.Prepare(ctx, conn, p, req)
	logger.V(4).Info(log("done calling NodePrepareResources rpc"), "response", response, "err", err)
	return response, err
}

func (p *plugin) NodeUnprepareResources(
	ctx context.Context,
	req *drapb.NodeUnprepareResourcesRequest,
	opts ...grpc.CallOption,
) (*drapb.NodeUnprepareResourcesResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info(log("calling NodeUnprepareResource rpc"), "request", req)

	conn, err := p.getOrCreateGRPCConn()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, PluginClientTimeout)
	defer cancel()

	version := p.getVersion()
	resourceManager, exists := nodeResourceManagers[version]
	if !exists {
		err := fmt.Errorf("unsupported plugin version: %s", version)
		logger.V(4).Info(log("done calling NodeUnprepareResources rpc"), "response", nil, "err", err)
		return nil, err
	}

	response, err := resourceManager.Unprepare(ctx, conn, p, req)
	logger.V(4).Info(log("done calling NodeUnprepareResources rpc"), "response", response, "err", err)
	return response, err
}

func (p *plugin) ResourceCapacity(
	ctx context.Context,
	req *drapb.ResourceCapacityRequest,
	opts ...grpc.CallOption,
) (drapb.Node_ResourceCapacityClient, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info(log("calling ResourceCapacity rpc"), "request", req)

	conn, err := p.getOrCreateGRPCConn()
	if err != nil {
		return nil, err
	}

	version := p.getVersion()
	resourceManager, exists := nodeResourceManagers[version]
	if !exists {
		err := fmt.Errorf("unsupported plugin version: %s", version)
		logger.V(4).Info(log("done calling ResourceCapacity rpc"), "response", nil, "err", err)
		return nil, err
	}

	response, err := resourceManager.ResourceCapacity(ctx, conn, p, req)
	logger.V(4).Info(log("done calling ResourceCapacity rpc"), "response", response, "err", err)
	return response, err
}

func (p *plugin) processResourceCapacityStream(pluginName string) {
	ctx := context.Background()
	logger := klog.FromContext(ctx)
	logger.Info("processResourceCapacityStream", "plugin", pluginName, "node", nodeName)
	stream, err := p.ResourceCapacity(ctx, &drapb.ResourceCapacityRequest{})
	if err != nil {
		logger.Error(err, "ResourceCapacity failed", "plugin", pluginName)
		return
	}
	for {
		response, err := stream.Recv()
		if err != nil {
			logger.Error(err, "can't get data from the stream", "plugin", pluginName)
			return
		}
		klog.V(4).InfoS("Got a response from the stream", "plugin", pluginName, "response", response)
		if kubeClient == nil {
			logger.Info("KubeClient is not set, can't talk to API server")
			continue
		}
		name := nodeName + ResourceCapacitySuffix
		instances := make([]resourcev1alpha2.NodeResourceInstance, len(response.Instances))
		for i, instance := range response.Instances {
			instances[i] = resourcev1alpha2.NodeResourceInstance{
				Kind:       "Capacity",
				ID:         instance.Id,
				APIVersion: counterv1alpha1.SchemeGroupVersion.String(),
				Data:       runtime.RawExtension{Raw: instance.Data.Raw},
			}
		}

		nrc := &resourcev1alpha2.NodeResourceCapacity{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			NodeName:   nodeName,
			DriverName: response.Driver,
			Instances:  instances,
		}
		_, err = kubeClient.ResourceV1alpha2().NodeResourceCapacities().Update(ctx, nrc, metav1.UpdateOptions{})
		if err != nil {
			logger.Error(err, "can't update NodeResourceCapacity", "name", name, "node", nodeName, "driver", response.Driver)
		}
		if !apierrors.IsNotFound(err) {
			logger.Info("create", "name", name, "node", nodeName, "driver", response.Driver)
			_, err = kubeClient.ResourceV1alpha2().NodeResourceCapacities().Create(ctx, nrc, metav1.CreateOptions{})
			if err != nil {
				logger.Error(err, "can't create NodeResourceCapacity", "name", name, "node", nodeName, "driver", response.Driver)
			}
		}
	}
}
