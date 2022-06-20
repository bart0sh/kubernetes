package server

import (
	"context"
	"sync"

	"k8s.io/klog/v2"
	cdipbv1 "k8s.io/kubernetes/pkg/kubelet/apis/cdi/v1alpha1"
)

type nodeServerConfig struct {
	DriverName    string
	Endpoint      string
	NodeID        string
	VendorVersion string
}

type exampleDriver struct {
	config nodeServerConfig

	// gRPC calls involving any of the fields below must be serialized
	// by locking this mutex before starting. Internal helper
	// functions assume that the mutex has been locked.
	mutex sync.Mutex
	state State
}

// newExampleDriver returns an initialized exampleDriver instance
func newExampleDriver(config nodeServerConfig) *exampleDriver {
	return &exampleDriver{
		config: config,
	}
}

func (ex *exampleDriver) run() {
	s := newNonBlockingGRPCServer()
	// ex itself implements NodeServer.
	s.Start(ex.config.Endpoint, ex)
	s.Wait()
}

func (ex *exampleDriver) NodePrepareResource(ctx context.Context, req *cdipbv1.NodePrepareResourceRequest) (*cdipbv1.NodePrepareResourceResponse, error) {
	klog.Infof("NodePrepareResource is called")

	klog.V(4).Infof("NodePrepareResource request: %+v", req)

	/*
		// Check arguments
		if len(req.) == 0 {
			return nil, status.Error(codes.InvalidArgument, "Resource ID missing in request")
		}
		preparingTargetPath := "/dev/tty"

		// Lock before acting on global state. A production-quality
		// driver might use more fine-grained locking.
		ex.mutex.Lock()
		defer ex.mutex.Unlock()

		res, err := ex.state.GetResourceByID(req.ResourceId)
		if err != nil {
			return nil, err
		}

		if res.Prepared.Has(preparingTargetPath) {
			klog.V(4).Infof("Resource %q is already prepared at %q, nothing to do.", req.ResourceId, preparingTargetPath)
			return &cdipbv1.NodePrepareResourceResponse{}, nil
		}

		if !res.Prepared.Empty() {
			return nil, status.Errorf(codes.FailedPrecondition, "resource %q is already prepared at %v", req.ResourceId, res.Prepared)
		}

		res.Prepared.Add(preparingTargetPath)
		if err := ex.state.UpdateResource(res); err != nil {
			return nil, err
		}
	*/
	return &cdipbv1.NodePrepareResourceResponse{CdiDevice: []string{"vendor.com/class1=device1", "vendor.com/class2=device2"}}, nil
}

func (ex *exampleDriver) NodeUnprepareResource(ctx context.Context, req *cdipbv1.NodeUnprepareResourceRequest) (*cdipbv1.NodeUnprepareResourceResponse, error) {
	klog.Infof("NodeUnprepareResource is called")

	klog.V(4).Infof("NodeUnprepareResource request: %+v", req)

	/*	if len(req.GetResourceId()) == 0 {
			return nil, status.Error(codes.InvalidArgument, "Resource ID missing in request")
		}

		ex.mutex.Lock()
		defer ex.mutex.Unlock()*/

	return &cdipbv1.NodeUnprepareResourceResponse{}, nil
}
