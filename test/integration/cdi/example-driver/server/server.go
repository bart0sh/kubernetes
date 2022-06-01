package server

func RunServer(vendorVersion, driverName, nodeID, endPoint, kubeletRegistrationPath, pluginRegistrationPath string) error {
	//regsitrar will register driver with Kubelet
	registrarConfig := nodeRegistrarConfig{
		cdiDriverName:           driverName,
		kubeletRegistrationPath: kubeletRegistrationPath,
		pluginRegistrationPath:  pluginRegistrationPath,
	}
	registrar := newRegistrar(registrarConfig)
	go registrar.nodeRegister()

	//driver will listen from a socket that deals with the call from Kubelet
	driverConfig := nodeServerConfig{
		NodeID:        nodeID,
		VendorVersion: vendorVersion,
		Endpoint:      endPoint,
	}
	driver := newExampleDriver(driverConfig)
	driver.run()

	return nil
}
