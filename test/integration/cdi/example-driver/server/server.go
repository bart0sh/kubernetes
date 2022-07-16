package server

import (
	"fmt"
)

func RunServer(driverName, draAddress, pluginRegistrationPath string) error {
	//regsitrar will register driver with Kubelet
	registrarConfig := nodeRegistrarConfig{
		draDriverName:          driverName,
		draAddress:             draAddress,
		pluginRegistrationPath: pluginRegistrationPath,
	}
	registrar := newRegistrar(registrarConfig)
	go registrar.nodeRegister()

	//driver will listen from a socket that deals with the call from Kubelet
	driverConfig := nodeServerConfig{
		driverName: driverName,
		draAddress: draAddress,
	}
	if driver, err := newExampleDriver(driverConfig); err != nil {
		fmt.Printf("new example driver not created with the an error: %v", err)
		return err
	} else {
		driver.run()
		return nil
	}
}
