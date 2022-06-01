package server

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	cdipbv1 "k8s.io/kubernetes/pkg/kubelet/apis/cdi/v1alpha1"
)

// NonBlocking server
type nonBlockingGRPCServer struct {
	wg      sync.WaitGroup
	server  *grpc.Server
	cleanup func()
}

func (s *nonBlockingGRPCServer) Start(endpoint string, ns cdipbv1.NodeServer) {

	s.wg.Add(1)

	go s.serve(endpoint, ns)
}

func (s *nonBlockingGRPCServer) Wait() {
	s.wg.Wait()
}

func (s *nonBlockingGRPCServer) Stop() {
	s.server.GracefulStop()
	s.cleanup()
}

func (s *nonBlockingGRPCServer) ForceStop() {
	s.server.Stop()
	s.cleanup()
}

func (s *nonBlockingGRPCServer) serve(ep string, ns cdipbv1.NodeServer) {
	listener, cleanup, err := Listen(ep)
	if err != nil {
		fmt.Printf("Failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)
	s.server = server
	s.cleanup = cleanup

	if ns != nil {
		cdipbv1.RegisterNodeServer(server, ns)
	}

	server.Serve(listener)

}

func newNonBlockingGRPCServer() *nonBlockingGRPCServer {
	return &nonBlockingGRPCServer{}
}
