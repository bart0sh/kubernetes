/*
Copyright 2023 The Kubernetes Authors.

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

/*
E2E Node test for DRA (Dynamic Resource Allocation)
This test covers node-specific aspects of DRA
The test can be run locally on Linux this way:
  make test-e2e-node FOCUS='\[NodeAlphaFeature:DynamicResourceAllocation\]' SKIP='\[Flaky\]' PARALLELISM=1 \
       TEST_ARGS='--feature-gates="DynamicResourceAllocation=true" --service-feature-gates="DynamicResourceAllocation=true" --runtime-config=api/all=true'
*/

package e2enode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	resourcev1alpha2 "k8s.io/api/resource/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	dra "k8s.io/kubernetes/pkg/kubelet/cm/dra/plugin"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"

	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	testdriver "k8s.io/kubernetes/test/e2e/dra/test-driver/app"
)

const (
	driverName                = "test-driver.cdi.k8s.io"
	cdiDir                    = "/var/run/cdi"
	endpoint                  = "/var/lib/kubelet/plugins/test-driver/dra.sock"
	pluginRegistrationPath    = "/var/lib/kubelet/plugins_registry"
	draAddress                = "/var/lib/kubelet/plugins/test-driver/dra.sock"
	pluginRegistrationTimeout = time.Second * 60 // how long to wait for a node plugin to be registered
	podInPendingStateTimeout  = time.Second * 60 // how long to wait for a pod to stay in pending state
)

var _ = framework.SIGDescribe("node")("DRA", feature.DynamicResourceAllocation, "[NodeAlphaFeature:DynamicResourceAllocation]", func() {
	f := framework.NewDefaultFramework("dra-node")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	f.Context("Resource Kubelet Plugin", f.WithSerial(), func() {
		ginkgo.It("must register after Kubelet restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), nil)
			oldCalls := kubeletPlugin.GetGRPCCalls()
			getNewCalls := func() []testdriver.GRPCCall {
				calls := kubeletPlugin.GetGRPCCalls()
				return calls[len(oldCalls):]
			}

			ginkgo.By("restarting Kubelet")
			restartKubelet(true)

			ginkgo.By("wait for Kubelet plugin re-registration")
			gomega.Eventually(getNewCalls).WithTimeout(pluginRegistrationTimeout).Should(testdriver.BeRegistered)
		})

		ginkgo.It("must register after plugin restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), nil)
			ginkgo.By("restart Kubelet Plugin")
			kubeletPlugin.Stop()
			kubeletPlugin = newKubeletPlugin(ctx, getNodeName(ctx, f), nil)

			ginkgo.By("wait for Kubelet plugin re-registration")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdriver.BeRegistered)
		})

		ginkgo.It("must process pod created when kubelet is not running", func(ctx context.Context) {
			_ = newKubeletPlugin(ctx, getNodeName(ctx, f), nil)
			// Stop Kubelet
			startKubelet := stopKubelet()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 0, true)
			// Pod must be in pending state
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)
			// Start Kubelet
			startKubelet()
			// Pod should succeed
			err = e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, f.Namespace.Name, framework.PodStartShortTimeout)
			framework.ExpectNoError(err)
		})

		f.It("must keep pod in pending state while NodePrepareResources times out", f.WithSlow(), func(ctx context.Context) {
			interceptor := newInterceptor()
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), interceptor.interceptGRPCCall)

			interceptor.setBlock("NodePrepareResources", true)
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 0, true)

			ginkgo.By("wait for failed NodePrepareResources call")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodePrepareResourcesErrored)

			// TODO: Check condition or event when implemented
			// see https://github.com/kubernetes/kubernetes/issues/118468 for details
			ginkgo.By("check that pod is consistently in Pending state")
			gomega.Consistently(ctx, e2epod.Get(f.ClientSet, pod)).WithTimeout(podInPendingStateTimeout).Should(e2epod.BeInPhase(v1.PodPending),
				"Pod should be in Pending state as resource preparation time outed")

			// Allow pod to run.
			interceptor.setBlock("NodePrepareResources", false)

			ginkgo.By("wait for successful NodePrepareResources call")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodePrepareResourcesSucceeded)

			// Pod should succeed
			err := e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, f.Namespace.Name, framework.PodStartShortTimeout)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must run pod if NodePrepareResources fails and then succeeds", func(ctx context.Context) {
			interceptor := newInterceptor()
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), interceptor.interceptGRPCCall)

			interceptor.setFailure("NodePrepareResources", errors.New("Simulated failure"))
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 0, true)

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodePrepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodePrepareResourcesErrored)

			interceptor.setFailure("NodePrepareResources", nil)

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must run pod if NodeUnprepareResources fails and then succeeds", func(ctx context.Context) {
			interceptor := newInterceptor()
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), interceptor.interceptGRPCCall)

			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 10, true)

			ginkgo.By("wait for pod to run")
			err := e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)
			framework.ExpectNoError(err)

			interceptor.setFailure("NodeUnprepareResources", errors.New("Simulated failure"))
			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesErrored)

			interceptor.setFailure("NodeUnprepareResources", nil)

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must retry NodePrepareResources after Kubelet restart", func(ctx context.Context) {
			interceptor := newInterceptor()
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), interceptor.interceptGRPCCall)

			interceptor.setFailure("NodePrepareResources", errors.New("Simulated failure"))
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 0, true)

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodePrepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodePrepareResourcesErrored)

			ginkgo.By("stop Kubelet")
			startKubelet := stopKubelet()

			interceptor.setFailure("NodePrepareResources", nil)

			ginkgo.By("start Kubelet")
			startKubelet()

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must retry NodeUnprepareResources after Kubelet restart", func(ctx context.Context) {
			interceptor := newInterceptor()
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), interceptor.interceptGRPCCall)

			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 10, true)

			ginkgo.By("wait for pod to run")
			err := e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)
			framework.ExpectNoError(err)

			interceptor.setFailure("NodeUnprepareResources", errors.New("Simulated failure"))
			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesErrored)

			ginkgo.By("stop Kubelet")
			startKubelet := stopKubelet()

			interceptor.setFailure("NodeUnprepareResources", nil)

			ginkgo.By("start Kubelet")
			startKubelet()

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must call NodeUnprepareResources for deleted pod", func(ctx context.Context) {
			interceptor := newInterceptor()
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), interceptor.interceptGRPCCall)

			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 10, false)

			ginkgo.By("wait for pod to run")
			err := e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)
			framework.ExpectNoError(err)

			interceptor.setFailure("NodeUnprepareResources", errors.New("Simulated failure"))
			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesErrored)

			ginkgo.By("kill pod")
			err = e2epod.DeletePodWithGracePeriod(ctx, f.ClientSet, pod, 0)
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesErrored)

			interceptor.setFailure("NodeUnprepareResources", nil)

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesSucceeded)
		})

		ginkgo.It("must call NodeUnprepareResources for deleted pod after Kubelet restart", func(ctx context.Context) {
			interceptor := newInterceptor()
			kubeletPlugin := newKubeletPlugin(ctx, getNodeName(ctx, f), interceptor.interceptGRPCCall)

			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", 10, false)

			ginkgo.By("wait for pod to run")
			err := e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)
			framework.ExpectNoError(err)

			interceptor.setFailure("NodeUnprepareResources", errors.New("Simulated failure"))
			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesErrored)

			ginkgo.By("kill pod")
			err = e2epod.DeletePodWithGracePeriod(ctx, f.ClientSet, pod, 0)
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesErrored)

			ginkgo.By("restart Kubelet")
			startKubelet := stopKubelet()
			interceptor.setFailure("NodeUnprepareResources", nil)
			startKubelet()

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(dra.PluginClientTimeout * 2).Should(testdriver.NodeUnprepareResourcesSucceeded)
		})
	})
})

// Run Kubelet plugin and wait until it's registered
func newKubeletPlugin(ctx context.Context, nodeName string, interceptor grpc.UnaryServerInterceptor) *testdriver.ExamplePlugin {
	ginkgo.By("start Kubelet plugin")
	logger := klog.LoggerWithValues(klog.LoggerWithName(klog.Background(), "kubelet plugin"), "node", nodeName)
	ctx = klog.NewContext(ctx, logger)

	// Ensure that directories exist, creating them if necessary. We want
	// to know early if there is a setup problem that would prevent
	// creating those directories.
	err := os.MkdirAll(cdiDir, os.FileMode(0750))
	framework.ExpectNoError(err, "create CDI directory")
	err = os.MkdirAll(filepath.Dir(endpoint), 0750)
	framework.ExpectNoError(err, "create socket directory")

	plugin, err := testdriver.StartPlugin(
		ctx,
		cdiDir,
		driverName,
		"",
		testdriver.PluginConfig{
			InterceptGRPCCall: interceptor,
		},
		kubeletplugin.PluginSocketPath(endpoint),
		kubeletplugin.RegistrarSocketPath(path.Join(pluginRegistrationPath, driverName+"-reg.sock")),
		kubeletplugin.KubeletPluginSocketPath(draAddress),
	)
	framework.ExpectNoError(err)

	gomega.Eventually(plugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdriver.BeRegistered)

	ginkgo.DeferCleanup(plugin.Stop)

	return plugin
}

// newInterceptor returns a gRPC interceptor which blocks and/or returns a
// failure if requested. It distinguishes between methods based on their basic
// name without interface (e.g. NodePrepareResources).
func newInterceptor() *interceptor {
	i := &interceptor{
		block:      make(map[string]bool),
		failure:    make(map[string]error),
		isBlocking: make(map[string]bool),
	}
	i.cond = sync.NewCond(&i.mutex)
	return i
}

type interceptor struct {
	mutex sync.Mutex
	cond  *sync.Cond

	block      map[string]bool
	failure    map[string]error
	isBlocking map[string]bool
}

func (i *interceptor) interceptGRPCCall(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	parts := strings.Split(info.FullMethod, "/")
	method := parts[len(parts)-1]
	logger := klog.FromContext(ctx)
	if i.block[method] {
		// Wait for timeout or block to get lifted.
		logger.Info("Interceptor blocking gRPC call", "info", info)
		i.isBlocking[method] = true
		i.cond.Broadcast()
		for {
			i.condWait(ctx)
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			if !i.block[method] {
				i.isBlocking[method] = false
				i.cond.Broadcast()
				if err := i.failure[method]; err != nil {
					logger.Info("Interceptor failing gRPC call", "info", info, "err", err)
					return nil, err
				}
				logger.Info("Interceptor executing gRPC call", "info", info)
				return handler(ctx, req)
			}
		}
	}

	if err := i.failure[method]; err != nil {
		logger.Info("Interceptor failing gRPC call", "info", info, "err", err)
		return nil, err
	}

	// No logging for normal calls.
	return handler(ctx, req)
}

// condWait returns when the context gets canceled or
// the condition variable gets signaled.
func (i *interceptor) condWait(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		i.mutex.Lock()
		defer i.mutex.Unlock()
		i.cond.Broadcast()
	}()
	i.cond.Wait()
}

func (i *interceptor) setBlock(method string, block bool) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.block[method] = block
	i.cond.Broadcast()
}

func (i *interceptor) setFailure(method string, failure error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.failure[method] = failure
	i.cond.Broadcast()
}

// createTestObjects creates objects required by the test
// NOTE: as scheduler and controller manager are not running by the Node e2e,
// the objects must contain all required data to be processed correctly by the API server
// and placed on the node without involving the scheduler and the DRA controller
func createTestObjects(ctx context.Context, clientSet kubernetes.Interface, nodename, namespace, className, claimName, podName string, sleepTime int, deferPodDeletion bool) *v1.Pod {
	// ResourceClass
	class := &resourcev1alpha2.ResourceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
		DriverName: driverName,
	}
	_, err := clientSet.ResourceV1alpha2().ResourceClasses().Create(ctx, class, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.DeferCleanup(clientSet.ResourceV1alpha2().ResourceClasses().Delete, className, metav1.DeleteOptions{})

	// ResourceClaim
	podClaimName := "resource-claim"
	claim := &resourcev1alpha2.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: claimName,
		},
		Spec: resourcev1alpha2.ResourceClaimSpec{
			ResourceClassName: className,
		},
	}
	createdClaim, err := clientSet.ResourceV1alpha2().ResourceClaims(namespace).Create(ctx, claim, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.DeferCleanup(clientSet.ResourceV1alpha2().ResourceClaims(namespace).Delete, claimName, metav1.DeleteOptions{})

	// Pod
	containerName := "testcontainer"
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			NodeName: nodename, // Assign the node as the scheduler is not running
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name: podClaimName,
					Source: v1.ClaimSource{
						ResourceClaimName: &claimName,
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:  containerName,
					Image: e2epod.GetDefaultTestImage(),
					Resources: v1.ResourceRequirements{
						Claims: []v1.ResourceClaim{{Name: podClaimName}},
					},
					Command: []string{"/bin/sh", "-c", fmt.Sprintf("env | grep DRA_PARAM1=PARAM1_VALUE && sleep %d", sleepTime)},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	createdPod, err := clientSet.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	if deferPodDeletion {
		ginkgo.DeferCleanup(clientSet.CoreV1().Pods(namespace).Delete, podName, metav1.DeleteOptions{})
	}

	// Update claim status: set ReservedFor and AllocationResult
	// NOTE: This is usually done by the DRA controller
	createdClaim.Status = resourcev1alpha2.ResourceClaimStatus{
		DriverName: driverName,
		ReservedFor: []resourcev1alpha2.ResourceClaimConsumerReference{
			{Resource: "pods", Name: podName, UID: createdPod.UID},
		},
		Allocation: &resourcev1alpha2.AllocationResult{
			ResourceHandles: []resourcev1alpha2.ResourceHandle{
				{
					DriverName: driverName,
					Data:       "{\"EnvVars\":{\"DRA_PARAM1\":\"PARAM1_VALUE\"},\"NodeName\":\"\"}",
				},
			},
		},
	}
	_, err = clientSet.ResourceV1alpha2().ResourceClaims(namespace).UpdateStatus(ctx, createdClaim, metav1.UpdateOptions{})
	framework.ExpectNoError(err)

	return pod
}
