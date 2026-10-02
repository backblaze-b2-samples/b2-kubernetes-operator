//go:build live

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

// Package live runs the operator's controllers against a real Kubernetes API
// server (envtest) and the real Backblaze B2 API, and checks every outcome by
// reading B2 directly. It creates real buckets and keys (and, if enabled,
// real Partner API accounts). See README.md before running it.
package live

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/controller"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/policy"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

const (
	operatorNS = "b2-operator-system"
	tenantNS   = "live"
	timeout    = 3 * time.Minute
	poll       = 2 * time.Second
	grace      = 10 * time.Second
)

var (
	k8s       client.Client
	direct    *b2.Client // the test's own view of B2, independent of the operator
	runID     string     // unique per run; prefixes every bucket and key
	clusterID string
	skipMsg   string
)

func env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func TestMain(m *testing.M) {
	switch {
	case env("B2_LIVE_KEY_ID") == "" || env("B2_LIVE_KEY") == "":
		skipMsg = "B2_LIVE_KEY_ID and B2_LIVE_KEY are not set; see test/live/README.md"
	case env("KUBEBUILDER_ASSETS") == "":
		skipMsg = "KUBEBUILDER_ASSETS is not set; run `make test-live`"
	}
	if skipMsg != "" {
		os.Exit(m.Run())
	}
	logf.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(true), zap.Level(zapcore.InfoLevel)))
	os.Exit(run(m))
}

func run(m *testing.M) int {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	runID = "b2op-live-" + hex.EncodeToString(b)
	clusterID = "lv" + hex.EncodeToString(b)

	apiURL := env("B2_LIVE_API_URL")
	direct = b2.New(b2.Options{BaseURL: apiURL, ApplicationKeyID: env("B2_LIVE_KEY_ID"), ApplicationKey: env("B2_LIVE_KEY")})
	auth, err := direct.Authorize(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "authorizing with B2: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "live run %s against account %s (%s)\n", runID, auth.AccountID, auth.APIInfo.StorageAPI.S3APIURL)

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		panic(err)
	}
	defer func() { _ = testEnv.Stop() }()

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(b2v1.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {Label: labels.SelectorFromSet(labels.Set{controller.LabelManagedBy: controller.ManagedByValue})},
		}},
	})
	must(err)
	deps := controller.Deps{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Accounts: &provider.Registry{
			APIReader: mgr.GetAPIReader(),
			UserAgent: "b2-kubernetes-operator-live-test",
			// Only for dry-running this suite against cmd/b2fake.
			AllowInsecureAPIURL: strings.HasPrefix(apiURL, "http://"),
		},
		Policy:   &policy.Evaluator{Reader: mgr.GetClient()},
		Recorder: mgr.GetEventRecorder("b2-operator"),
		Options: controller.Options{
			ResyncPeriod:            time.Hour, // tests trigger resyncs explicitly
			KeyVerifyInterval:       time.Hour,
			DefaultGracePeriod:      grace,
			RevokeOnPolicyViolation: true,
			ClusterID:               clusterID,
		},
	}
	must((&controller.ClusterProviderConfigReconciler{Deps: deps}).SetupWithManager(mgr))
	must((&controller.BucketReconciler{Deps: deps}).SetupWithManager(mgr))
	must((&controller.ApplicationKeyReconciler{Deps: deps}).SetupWithManager(mgr))
	must((&controller.B2AccountReconciler{Deps: deps}).SetupWithManager(mgr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := mgr.Start(ctx); err != nil {
			panic(err)
		}
	}()
	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	must(err)
	must(seed(apiURL))

	code := m.Run()
	sweepB2(context.Background())
	return code
}

// seed creates the operator namespace, the live credentials and a permissive
// policy for the tenant namespace, limited to this run's bucket prefix.
func seed(apiURL string) error {
	ctx := context.Background()
	objs := []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: operatorNS}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenantNS, Labels: map[string]string{"b2-live": "true"}}},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "b2-credentials", Namespace: operatorNS},
			StringData: map[string]string{"applicationKeyId": env("B2_LIVE_KEY_ID"), "applicationKey": env("B2_LIVE_KEY")},
		},
		&b2v1.ClusterProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "default"},
			Spec: b2v1.ClusterProviderConfigSpec{
				APIURL:               apiURL,
				CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "b2-credentials"},
			},
		},
		&b2v1.B2AccessPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "live"},
			Spec: b2v1.B2AccessPolicySpec{
				NamespaceSelector: metav1.LabelSelector{MatchLabels: map[string]string{"b2-live": "true"}},
				ProviderConfigs:   []string{"*"},
				Buckets: b2v1.BucketPolicy{
					NamePatterns:     []string{runID + "-*"},
					AllowDeletion:    true,
					AllowUnencrypted: true,
					AllowReplication: true,
				},
				Keys: b2v1.KeyPolicy{AllowedCapabilities: []b2v1.Capability{
					"listBuckets", "listFiles", "readFiles", "writeFiles", "deleteFiles",
				}},
			},
		},
	}
	for _, o := range objs {
		if err := k8s.Create(ctx, o); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating %T %s: %w", o, o.GetName(), err)
		}
	}
	return nil
}

// sweepB2 removes anything this run left in B2, in case a test failed
// before its own cleanup. Partner accounts cannot be deleted and are left.
func sweepB2(ctx context.Context) {
	if keys, err := direct.FindKeys(ctx, func(k b2.ApplicationKey) bool {
		return strings.HasPrefix(k.KeyName, controller.KeyNamePrefix+"-"+clusterID+"-")
	}); err == nil {
		for _, k := range keys {
			fmt.Fprintf(os.Stderr, "cleanup: revoking leftover key %s (%s)\n", k.ApplicationKeyID, k.KeyName)
			_ = direct.DeleteKey(ctx, k.ApplicationKeyID)
		}
	}
	buckets, err := direct.ListBuckets(ctx, b2.ListBucketsRequest{})
	if err != nil {
		return
	}
	for _, b := range buckets {
		if !strings.HasPrefix(b.BucketName, runID) {
			continue
		}
		if rc := b.ReplicationConfiguration; rc != nil && rc.Value != nil && (rc.Value.AsReplicationSource != nil || rc.Value.AsReplicationDestination != nil) {
			// An empty configuration clears both sides.
			_, _ = direct.UpdateBucket(ctx, b2.UpdateBucketRequest{BucketID: b.BucketID, ReplicationConfiguration: &b2.ReplicationConfiguration{}})
		}
		fmt.Fprintf(os.Stderr, "cleanup: deleting leftover bucket %s\n", b.BucketName)
		if err := direct.DeleteBucket(ctx, b.BucketID); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup: could not delete bucket %s: %v\n", b.BucketName, err)
		}
	}
}

func requireLive(t *testing.T) *WithT {
	t.Helper()
	if skipMsg != "" {
		t.Skip(skipMsg)
	}
	return NewWithT(t)
}

// bucketName returns a bucket name unique to this run.
func bucketName(suffix string) string { return runID + "-" + suffix }

func waitReady(g *WithT, obj client.Object, conds func() []metav1.Condition) {
	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		c := meta.FindStatusCondition(conds(), b2v1.ConditionReady)
		g.Expect(c).NotTo(BeNil())
		g.Expect(c.Status).To(Equal(metav1.ConditionTrue), "reason %s: %s", c.Reason, c.Message)
		g.Expect(c.ObservedGeneration).To(Equal(obj.GetGeneration()))
	}, timeout, poll).Should(Succeed())
}

func waitGone(g *WithT, obj client.Object) {
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
	}, timeout, poll).Should(BeTrue(), "%T %s was not deleted", obj, obj.GetName())
}

// update re-reads obj, applies mutate and writes it, retrying conflicts.
func update(g *WithT, obj client.Object, mutate func()) {
	g.Eventually(func() error {
		if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
			return err
		}
		mutate()
		return k8s.Update(context.Background(), obj)
	}, timeout, poll).Should(Succeed())
}

// resync forces a reconcile and waits for it to finish.
func resync(g *WithT, b *b2v1.Bucket) {
	before := b.Status.LastSyncTime
	update(g, b, func() {
		if b.Annotations == nil {
			b.Annotations = map[string]string{}
		}
		b.Annotations["b2-live/resync"] = time.Now().Format(time.RFC3339Nano)
	})
	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(context.Background(), client.ObjectKeyFromObject(b), b)).To(Succeed())
		g.Expect(b.Status.LastSyncTime).NotTo(BeNil())
		if before != nil {
			g.Expect(b.Status.LastSyncTime.After(before.Time)).To(BeTrue())
		}
	}, timeout, poll).Should(Succeed())
}

// b2Bucket reads a bucket straight from B2.
func b2Bucket(g *WithT, name string) *b2.Bucket {
	b, err := direct.GetBucketByName(context.Background(), name)
	g.Expect(err).NotTo(HaveOccurred())
	return b
}

func bucketConds(b *b2v1.Bucket) func() []metav1.Condition {
	return func() []metav1.Condition { return b.Status.Conditions }
}

func keyConds(k *b2v1.ApplicationKey) func() []metav1.Condition {
	return func() []metav1.Condition { return k.Status.Conditions }
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func ptr[T any](v T) *T { return &v }
