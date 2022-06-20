package server

import (
	"context"
	"os"
	"path/filepath"

	"k8s.io/klog/v2"
	cdipbv1 "k8s.io/kubernetes/pkg/kubelet/apis/cdi/v1alpha1"
)

type nodeServerConfig struct {
	driverName string
	cdiAddress string
}

type exampleDriver struct {
	config nodeServerConfig
}

// newExampleDriver returns an initialized exampleDriver instance
func newExampleDriver(config nodeServerConfig) (*exampleDriver, error) {

	if err := os.MkdirAll(filepath.Dir(config.cdiAddress), 0750); err != nil {
		return nil, err
	}

	return (&exampleDriver{
		config: config,
	}), nil
}

func (ex *exampleDriver) run() {
	s := newNonBlockingGRPCServer()
	// ex itself implements NodeServer.
	s.Start(ex.config.cdiAddress, ex)
	s.Wait()
}

func (ex *exampleDriver) NodePrepareResource(ctx context.Context, req *cdipbv1.NodePrepareResourceRequest) (*cdipbv1.NodePrepareResourceResponse, error) {
	klog.Infof("NodePrepareResource is called")
	return &cdipbv1.NodePrepareResourceResponse{}, nil
}

func (ex *exampleDriver) NodeUnprepareResource(ctx context.Context, req *cdipbv1.NodeUnprepareResourceRequest) (*cdipbv1.NodeUnprepareResourceResponse, error) {
	klog.Infof("NodeUnprepareResource is called")
	return &cdipbv1.NodeUnprepareResourceResponse{}, nil
}
