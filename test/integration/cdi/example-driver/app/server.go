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
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/term"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/integration/cdi/example-driver/leaderelection"
)

// NewCommand creates a *cobra.Command object with default parameters.
func NewCommand() *cobra.Command {
	o := logsapi.NewLoggingConfiguration()
	var clientset kubernetes.Interface
	var config *rest.Config
	ctx := context.Background()

	cmd := &cobra.Command{
		Use:  "cdi-example-driver",
		Long: "cdi-example-driver implements a resource driver controller and kubelet plugin.",
	}
	sharedFlagSets := cliflag.NamedFlagSets{}
	fs := sharedFlagSets.FlagSet("logging")
	logsapi.AddFlags(o, fs)
	logs.AddFlags(fs, logs.SkipLoggingConfigurationFlags())

	fs = sharedFlagSets.FlagSet("Kubernetes client")
	kubeconfig := fs.String("kubeconfig", "", "Absolute path to the kube.config file. Either this or KUBECONFIG need to be set if the driver is being run out of cluster.")
	kubeAPIQPS := fs.Float32("kube-api-qps", 5, "QPS to use while communicating with the kubernetes apiserver.")
	kubeAPIBurst := fs.Int("kube-api-burst", 10, "Burst to use while communicating with the kubernetes apiserver.")
	workers := fs.Int("workers", 10, "Concurrency to process multiple claims")

	fs = sharedFlagSets.FlagSet("http server")
	httpEndpoint := fs.String("http-endpoint", "",
		"The TCP network address where the HTTP server for diagnostics, including pprof, metrics and (if applicable) leader election health check, will listen (example: `:8080`). The default is the empty string, which means the server is disabled.")
	metricsPath := fs.String("metrics-path", "/metrics", "The HTTP path where Prometheus metrics will be exposed, disabled if empty.")
	profilePath := fs.String("pprof-path", "", "The HTTP path where pprof profiling will be available, disabled if empty.")

	fs = sharedFlagSets.FlagSet("CDI")
	driverName := fs.String("drivername", "example-driver.cdi.k8s.io", "Resource driver name.")

	fs = sharedFlagSets.FlagSet("other")
	featureGate := featuregate.NewFeatureGate()
	utilruntime.Must(logsapi.AddFeatureGates(featureGate))
	featureGate.AddFlag(fs)

	fs = cmd.PersistentFlags()
	for _, f := range sharedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	mux := http.NewServeMux()

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Activate logging as soon as possible, after that
		// show flags with the final logging configuration.

		if err := logsapi.ValidateAndApply(o, featureGate); err != nil {
			return err
		}

		// get the KUBECONFIG from env if specified (useful for local/debug cluster)
		kubeconfigEnv := os.Getenv("KUBECONFIG")

		if kubeconfigEnv != "" {
			klog.Infof("Found KUBECONFIG environment variable set, using that..")
			*kubeconfig = kubeconfigEnv
		}

		var err error
		if *kubeconfig == "" {
			config, err = rest.InClusterConfig()
			if err != nil {
				return fmt.Errorf("create in-cluster client configuration: %v", err)
			}
		} else {
			config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
			if err != nil {
				return fmt.Errorf("create out-of-cluster client configuration: %v", err)
			}
		}
		config.QPS = *kubeAPIQPS
		config.Burst = *kubeAPIBurst

		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("create client: %v", err)
		}

		if *httpEndpoint != "" {
			if *metricsPath != "" {
				// To collect metrics data from the metric handler itself, we
				// let it register itself and then collect from that registry.
				reg := prometheus.NewRegistry()
				gatherers := prometheus.Gatherers{
					// For workqueue and leader election metrics, set up via the anonymous imports of:
					// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/component-base/metrics/prometheus/workqueue/metrics.go
					// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/component-base/metrics/prometheus/clientgo/leaderelection/metrics.go
					//
					// Also to happens to include Go runtime and process metrics:
					// https://github.com/kubernetes/kubernetes/blob/9780d88cb6a4b5b067256ecb4abf56892093ee87/staging/src/k8s.io/component-base/metrics/legacyregistry/registry.go#L46-L49
					legacyregistry.DefaultGatherer,
				}
				gatherers = append(gatherers, reg)

				actualPath := path.Join("/", *metricsPath)
				klog.InfoS("Starting metrics", "path", actualPath)
				// This is similar to k8s.io/component-base/metrics HandlerWithReset
				// except that we gather from multiple sources.
				mux.Handle(actualPath,
					promhttp.InstrumentMetricHandler(
						reg,
						promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})))
			}

			if *profilePath != "" {
				actualPath := path.Join("/", *profilePath)
				klog.InfoS("Starting profiling", "path", actualPath)
				mux.HandleFunc(path.Join("/", *profilePath), pprof.Index)
				mux.HandleFunc(path.Join("/", *profilePath, "cmdline"), pprof.Cmdline)
				mux.HandleFunc(path.Join("/", *profilePath, "profile"), pprof.Profile)
				mux.HandleFunc(path.Join("/", *profilePath, "symbol"), pprof.Symbol)
				mux.HandleFunc(path.Join("/", *profilePath, "trace"), pprof.Trace)
			}

			listener, err := net.Listen("tcp", *httpEndpoint)
			if err != nil {
				return fmt.Errorf("Listen on HTTP endpoint: %v", err)
			}

			go func() {
				klog.InfoS("Starting HTTP server", "endpoint", *httpEndpoint)
				err := http.Serve(listener, mux)
				if err != nil {
					klog.ErrorS(err, "HTTP server failed")
					klog.FlushAndExit(klog.ExitFlushTimeout, 1)
				}
			}()
		}

		return nil
	}

	controller := &cobra.Command{
		Use:   "controller",
		Short: "run as resource controller",
		Long:  "cdi-example-driver controller runs as a resource driver controller.",
		Args:  cobra.ExactArgs(0),
	}
	controllerFlagSets := cliflag.NamedFlagSets{}
	fs = controllerFlagSets.FlagSet("leader election")
	enableLeaderElection := fs.Bool("leader-election", false,
		"Enables leader election. If leader election is enabled, additional RBAC rules are required.")
	leaderElectionNamespace := fs.String("leader-election-namespace", "",
		"Namespace where the leader election resource lives. Defaults to the pod namespace if not set.")
	leaderElectionLeaseDuration := fs.Duration("leader-election-lease-duration", 15*time.Second,
		"Duration, in seconds, that non-leader candidates will wait to force acquire leadership.")
	leaderElectionRenewDeadline := fs.Duration("leader-election-renew-deadline", 10*time.Second,
		"Duration, in seconds, that the acting leader will retry refreshing leadership before giving up.")
	leaderElectionRetryPeriod := fs.Duration("leader-election-retry-period", 5*time.Second,
		"Duration, in seconds, the LeaderElector clients should wait between tries of actions.")
	fs = controller.Flags()
	for _, f := range controllerFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	controller.RunE = func(cmd *cobra.Command, args []string) error {
		run := func() {
			runController(ctx, clientset, *driverName, *workers)
		}

		if !*enableLeaderElection {
			run()
			return nil
		}

		// This must not change between releases.
		lockName := *driverName

		// Create a new clientset for leader election
		// to avoid starving it when the normal traffic
		// exceeds the QPS+burst limits.
		leClientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("Failed to create leaderelection client: %v", err)
		}

		le := leaderelection.New(leClientset, lockName,
			func(ctx context.Context) {
				run()
			},
			leaderelection.LeaseDuration(*leaderElectionLeaseDuration),
			leaderelection.RenewDeadline(*leaderElectionRenewDeadline),
			leaderelection.RetryPeriod(*leaderElectionRetryPeriod),
			leaderelection.Namespace(*leaderElectionNamespace),
		)
		if *httpEndpoint != "" {
			le.PrepareHealthCheck(mux)
		}
		if err := le.Run(); err != nil {
			return fmt.Errorf("leader election failed: %v", err)
		}

		return nil
	}
	cmd.AddCommand(controller)

	kubeletPlugin := &cobra.Command{
		Use:   "kubelet-plugin",
		Short: "run as kubelet plugin",
		Long:  "cdi-example-driver kubelet-plugin runs as a device plugin for kubelet that supports dynamic resource allocation.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("%s: not implemented", cmd.Use)
		},
	}
	cmd.AddCommand(kubeletPlugin)

	// SetUsageAndHelpFunc takes care of flag grouping. However,
	// it doesn't support listing child commands. We add those
	// to cmd.Use.
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, sharedFlagSets, cols)
	var children []string
	for _, child := range cmd.Commands() {
		children = append(children, child.Use)
	}
	cmd.Use += " [shared flags] " + strings.Join(children, "|")
	cliflag.SetUsageAndHelpFunc(controller, controllerFlagSets, cols)

	return cmd
}
