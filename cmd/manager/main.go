/*
Copyright 2026 Backblaze, Inc.

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

// Command manager runs the Backblaze B2 operator.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/controller"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/policy"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/remote"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/version"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(b2v1.AddToScheme(scheme))
	metrics.Registry.MustRegister(b2.Collectors()...)
}

func main() {
	var (
		metricsAddr, probeAddr                       string
		secureMetrics, enableHTTP2, leaderElect      bool
		allowKeyManagement, allowInsecureAPIURL      bool
		revokeOnPolicyViolation                      bool
		resyncPeriod, keyVerifyInterval, gracePeriod time.Duration
		sweepInterval                                time.Duration
		clusterID                                    string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "Address the metrics endpoint binds to; \"0\" disables it.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true, "Serve metrics over HTTPS with Kubernetes authn/authz.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the health probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader election so only one replica reconciles at a time.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false, "Enable HTTP/2 for the metrics server (off by default because of HTTP/2 rapid-reset CVEs).")
	flag.DurationVar(&resyncPeriod, "resync-period", 10*time.Minute, "How often resources are compared with B2 to detect drift.")
	flag.DurationVar(&keyVerifyInterval, "key-verify-interval", time.Hour, "How often each application key is checked against B2.")
	flag.DurationVar(&gracePeriod, "default-grace-period", 15*time.Minute, "How long a replaced key stays valid when the resource sets no gracePeriod.")
	flag.DurationVar(&sweepInterval, "orphan-key-sweep-interval", time.Hour, "How often to revoke keys this cluster created for ApplicationKeys that no longer exist; 0 disables.")
	flag.StringVar(&clusterID, "cluster-id", "", "8-character [a-z0-9] ID embedded in B2 key names to tell clusters sharing an account apart. Defaults to a prefix of the kube-system namespace UID.")
	flag.BoolVar(&revokeOnPolicyViolation, "revoke-on-policy-violation", true, "Revoke existing keys that B2AccessPolicies no longer allow.")
	flag.BoolVar(&allowKeyManagement, "allow-key-management-capabilities", false, "Allow policies to grant listKeys, writeKeys and deleteKeys. Keys with these capabilities can escalate to full account access.")
	flag.BoolVar(&allowInsecureAPIURL, "allow-insecure-api-url", false, "Allow http:// API URLs in ClusterProviderConfigs. For testing only.")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("starting b2-kubernetes-operator", "version", version.Version, "commit", version.Commit)

	var tlsOpts []func(*tls.Config)
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *tls.Config) { c.NextProtos = []string{"http/1.1"} })
	}
	metricsOpts := metricsserver.Options{BindAddress: metricsAddr, SecureServing: secureMetrics, TLSOpts: tlsOpts}
	if secureMetrics {
		metricsOpts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	managedSecrets := labels.SelectorFromSet(labels.Set{controller.LabelManagedBy: controller.ManagedByValue})
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsOpts,
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                leaderElect,
		LeaderElectionID:              "b2-operator.b2.backblaze.com",
		LeaderElectionReleaseOnCancel: true,
		Cache: cache.Options{
			// Only Secrets written by the operator are watched and cached;
			// credentials are read directly from the API server.
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {Label: managedSecrets},
			},
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if clusterID == "" {
		var ns corev1.Namespace
		if err := mgr.GetAPIReader().Get(context.Background(), client.ObjectKey{Name: "kube-system"}, &ns); err != nil {
			setupLog.Error(err, "unable to derive --cluster-id from the kube-system namespace; set it explicitly")
			os.Exit(1)
		}
		clusterID = strings.ReplaceAll(string(ns.UID), "-", "")[:8]
	}
	if !regexp.MustCompile(`^[a-z0-9]{8}$`).MatchString(clusterID) {
		setupLog.Error(nil, "--cluster-id must be 8 characters of [a-z0-9]", "clusterID", clusterID)
		os.Exit(1)
	}
	setupLog.Info("cluster identity", "clusterID", clusterID)

	deps := controller.Deps{
		Client: mgr.GetClient(),
		Registry: &provider.Registry{
			APIReader:           mgr.GetAPIReader(),
			UserAgent:           version.UserAgent(),
			AllowInsecureAPIURL: allowInsecureAPIURL,
		},
		Policy:   &policy.Evaluator{Reader: mgr.GetClient(), AllowKeyManagement: allowKeyManagement},
		Recorder: mgr.GetEventRecorder("b2-operator"),
		Options: controller.Options{
			ResyncPeriod:            resyncPeriod,
			KeyVerifyInterval:       keyVerifyInterval,
			DefaultGracePeriod:      gracePeriod,
			RevokeOnPolicyViolation: revokeOnPolicyViolation,
			ClusterID:               clusterID,
		},
	}

	if err := (&controller.ClusterProviderConfigReconciler{Deps: deps}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ClusterProviderConfig")
		os.Exit(1)
	}
	if err := (&controller.BucketReconciler{Deps: deps}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Bucket")
		os.Exit(1)
	}
	remotes := &remote.Registry{APIReader: mgr.GetAPIReader()}
	if err := (&controller.RemoteClusterReconciler{Deps: deps, Remote: remotes}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RemoteCluster")
		os.Exit(1)
	}
	if err := (&controller.ApplicationKeyReconciler{Deps: deps, APIReader: mgr.GetAPIReader(), Remote: remotes}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ApplicationKey")
		os.Exit(1)
	}

	if err := (&controller.B2AccountReconciler{Deps: deps, APIReader: mgr.GetAPIReader()}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "B2Account")
		os.Exit(1)
	}
	if err := mgr.Add(&controller.KeySweeper{Deps: deps, APIReader: mgr.GetAPIReader(), Interval: sweepInterval}); err != nil {
		setupLog.Error(err, "unable to add key sweeper")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
