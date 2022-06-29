package server

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	cdiapi "github.com/container-orchestrated-devices/container-device-interface/pkg/cdi"
	"github.com/container-orchestrated-devices/container-device-interface/specs-go"
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

var jsonFilePath = "/etc/cdi/vendor.json"
var devicePath = "/dev/tty"
var cdiVersion = "0.4.0"
var kind = "vendor.com/device"
var deviceName = "example"

func (ex *exampleDriver) NodePrepareResource(ctx context.Context, req *cdipbv1.NodePrepareResourceRequest) (*cdipbv1.NodePrepareResourceResponse, error) {
	klog.Infof("NodePrepareResource is called: request: %+v", req)
	// create CDI Files

	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		klog.Errorf(err.Error())
		return &cdipbv1.NodePrepareResourceResponse{}, err
	} else {
		// create spec
		klog.Infof("Creating CDI Files")

		containerEditsInDevices := &cdiapi.ContainerEdits{}
		containerEditsInDevices.Append(&cdiapi.ContainerEdits{ContainerEdits: &specs.ContainerEdits{
			DeviceNodes: []*specs.DeviceNode{
				{
					Path: devicePath,
					Type: "c",
				},
			},
		}})
		containerEdits := &cdiapi.ContainerEdits{}
		containerEdits.Append(&cdiapi.ContainerEdits{ContainerEdits: &specs.ContainerEdits{
			Env: []string{"envVar2=envVal2"},
		}})

		spec, err := cdiapi.NewSpec(
			&specs.Spec{
				Version: cdiVersion,
				Kind:    kind,
				Devices: []specs.Device{{
					Name:           deviceName,
					ContainerEdits: *containerEditsInDevices.ContainerEdits,
				}},
				ContainerEdits: *containerEdits.ContainerEdits,
			},
			jsonFilePath, 1)
		if err != nil {
			return nil, err
		} else {
			klog.Infof("CDI spec is validated")
		}

		// create json file from spec
		doc, err := json.Marshal(*spec)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(jsonFilePath, doc, os.FileMode(0644)); err != nil {
			return nil, err
		} else {
			klog.Infof("Json file is created at " + jsonFilePath)
		}

		//return qualified name of the cdi device
		dev := spec.GetDevice(deviceName).GetQualifiedName()
		klog.Infof("Return: " + dev)
		return &cdipbv1.NodePrepareResourceResponse{CdiDevice: []string{dev}}, nil
	}
}

func (ex *exampleDriver) NodeUnprepareResource(ctx context.Context, req *cdipbv1.NodeUnprepareResourceRequest) (*cdipbv1.NodeUnprepareResourceResponse, error) {
	klog.Infof("NodeUnprepareResource is called: request: %+v", req)
	os.Remove(jsonFilePath)
	return &cdipbv1.NodeUnprepareResourceResponse{}, nil
}
