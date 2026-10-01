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

package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2/b2fake"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/policy"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/remote"
)

const (
	timeout = 30 * time.Second
	poll    = 100 * time.Millisecond

	tenantLabel      = "b2-test/tenant"
	externalLabel    = "b2-test/external"
	testClusterID    = "testclst"
	partnerConfig    = "partner"
	operatorNS       = "b2-operator-system"
	credsSecretName  = "b2-credentials"
	testGracePeriod  = 2 * time.Second
	testVerifyPeriod = 2 * time.Second
)

// Shared state for the integration tests.
var (
	k8s     client.Client
	fakeB2  *b2fake.Server
	sweeper *KeySweeper
	groupID string
	// remoteK8s is a second API server standing in for a customer cluster
	// that keys are delivered to.
	remoteK8s        client.Client
	remoteKubeconfig []byte
	testCtx          context.Context
	skipMsg          string
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		skipMsg = "KUBEBUILDER_ASSETS not set; run `make test` to run integration tests"
		os.Exit(m.Run())
	}
	logf.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(true), zap.Level(zapLevel())))
	os.Exit(runSuite(m))
}

func zapLevel() zapcore.Level {
	if os.Getenv("TEST_VERBOSE") != "" {
		return zapcore.DebugLevel
	}
	return zapcore.ErrorLevel
}

func runSuite(m *testing.M) int {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		panic(err)
	}
	defer func() { _ = env.Stop() }()

	remoteEnv := &envtest.Environment{}
	remoteCfg, err := remoteEnv.Start()
	if err != nil {
		panic(err)
	}
	defer func() { _ = remoteEnv.Stop() }()
	remoteUser, err := remoteEnv.AddUser(envtest.User{Name: "b2-operator", Groups: []string{"system:masters"}}, nil)
	must(err)
	remoteKubeconfig, err = remoteUser.KubeConfig()
	must(err)

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(b2v1.AddToScheme(scheme))

	fakeB2 = b2fake.New()
	defer fakeB2.Close()
	// Retained buckets accumulate when the suite is run repeatedly
	// (-count=N) against one fake; lift B2's 100-bucket limit here.
	fakeB2.MaxBuckets = 100000

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {Label: labels.SelectorFromSet(labels.Set{LabelManagedBy: ManagedByValue})},
		}},
	})
	must(err)

	deps := Deps{
		Client:   mgr.GetClient(),
		Registry: &provider.Registry{APIReader: mgr.GetAPIReader(), AllowInsecureAPIURL: true},
		Policy:   &policy.Evaluator{Reader: mgr.GetClient()},
		Recorder: mgr.GetEventRecorder("b2-operator"),
		Options: Options{
			ResyncPeriod:            time.Hour,
			KeyVerifyInterval:       testVerifyPeriod,
			DefaultGracePeriod:      testGracePeriod,
			RevokeOnPolicyViolation: true,
			ClusterID:               testClusterID,
		},
	}
	sweeper = &KeySweeper{Deps: deps, APIReader: mgr.GetAPIReader()}
	must((&ClusterProviderConfigReconciler{Deps: deps}).SetupWithManager(mgr))
	must((&BucketReconciler{Deps: deps}).SetupWithManager(mgr))
	remotes := &remote.Registry{APIReader: mgr.GetAPIReader()}
	must((&RemoteClusterReconciler{Deps: deps, Remote: remotes}).SetupWithManager(mgr))
	must((&ApplicationKeyReconciler{Deps: deps, APIReader: mgr.GetAPIReader(), Remote: remotes}).SetupWithManager(mgr))
	must((&B2AccountReconciler{Deps: deps, APIReader: mgr.GetAPIReader()}).SetupWithManager(mgr))

	var cancel context.CancelFunc
	testCtx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := mgr.Start(testCtx); err != nil {
			panic(err)
		}
	}()

	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	must(err)
	remoteK8s, err = client.New(remoteCfg, client.Options{Scheme: scheme})
	must(err)
	must(seedCluster())
	return m.Run()
}

// seedCluster creates the operator namespace, credentials, the default
// provider config, and a policy for namespaces labelled as tenants.
func seedCluster() error {
	ctx := context.Background()
	groupID = fakeB2.AddGroup("Hosting Co customers")
	objs := []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: operatorNS}},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credsSecretName, Namespace: operatorNS},
			StringData: map[string]string{"applicationKeyId": fakeB2.MasterKeyID, "applicationKey": fakeB2.MasterKey},
		},
		&b2v1.ClusterProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: partnerConfig},
			Spec: b2v1.ClusterProviderConfigSpec{
				APIURL:               fakeB2.URL(),
				CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: credsSecretName},
				Partner:              &b2v1.PartnerSettings{GroupID: groupID, MemberEmailTemplate: "{customer}-{region}@hosting.example.com"},
			},
		},
		&b2v1.ClusterProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "default"},
			Spec: b2v1.ClusterProviderConfigSpec{
				APIURL:               fakeB2.URL(),
				CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: credsSecretName},
			},
		},
		&b2v1.B2AccessPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "external"},
			Spec: b2v1.B2AccessPolicySpec{
				NamespaceSelector: metav1.LabelSelector{MatchLabels: map[string]string{externalLabel: "true"}},
				ProviderConfigs:   []string{"default"},
				Buckets:           b2v1.BucketPolicy{NamePatterns: []string{"{namespace}-*"}, AllowAdoption: true},
				Keys: b2v1.KeyPolicy{
					AllowedCapabilities:  []b2v1.Capability{"listFiles", "readFiles"},
					AllowExternalBuckets: true,
				},
			},
		},
		&b2v1.B2AccessPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "tenants"},
			Spec: b2v1.B2AccessPolicySpec{
				NamespaceSelector: metav1.LabelSelector{MatchLabels: map[string]string{tenantLabel: "true"}},
				ProviderConfigs:   []string{"default"},
				Buckets: b2v1.BucketPolicy{
					NamePatterns:  []string{"{namespace}-*"},
					AllowAdoption: true,
					AllowDeletion: true,
				},
				Keys: b2v1.KeyPolicy{
					AllowedCapabilities: []b2v1.Capability{"listBuckets", "listFiles", "readFiles", "writeFiles", "deleteFiles", "writeKeys"},
					AllowedDeliveryTargets: []b2v1.DeliveryTargetPattern{
						{RemoteCluster: "customer-*", Namespaces: []string{"{namespace}"}},
					},
				},
			},
		},
	}
	for _, o := range objs {
		if err := k8s.Create(ctx, o); err != nil {
			return fmt.Errorf("creating %T %s: %w", o, o.GetName(), err)
		}
	}
	return nil
}

func requireEnv(t *testing.T) *WithT {
	t.Helper()
	if skipMsg != "" {
		t.Skip(skipMsg)
	}
	return NewWithT(t)
}

// newNamespace creates a namespace; tenant namespaces are selected by the
// seeded B2AccessPolicy.
func newNamespace(t *testing.T, tenant bool) string {
	t.Helper()
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	labels := map[string]string{}
	if tenant {
		labels[tenantLabel] = "true"
	}
	return createNamespace(t, "t"+hex.EncodeToString(b), labels)
}

func createNamespace(t *testing.T, name string, labels map[string]string) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	if err := k8s.Create(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	return name
}

func readyCondition(g Gomega, obj client.Object, conds func() []metav1.Condition) *metav1.Condition {
	g.Expect(k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)).To(Succeed())
	return meta.FindStatusCondition(conds(), b2v1.ConditionReady)
}

// eventuallyReason waits until obj's Ready condition has the given reason.
func eventuallyReason(g *WithT, obj client.Object, conds func() []metav1.Condition, reason string) {
	g.Eventually(func(g Gomega) {
		c := readyCondition(g, obj, conds)
		g.Expect(c).NotTo(BeNil())
		g.Expect(c.Reason).To(Equal(reason), "message: %s", c.Message)
	}, timeout, poll).Should(Succeed())
}

// touch sets an annotation to force a reconcile.
func touch(g *WithT, obj client.Object) {
	g.Expect(k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)).To(Succeed())
	base := obj.DeepCopyObject().(client.Object)
	a := obj.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	a["b2-test/touched"] = time.Now().String()
	obj.SetAnnotations(a)
	g.Expect(k8s.Patch(context.Background(), obj, client.MergeFrom(base))).To(Succeed())
}

// expectInvalidUpdate applies mutate to a fresh copy of obj and expects the
// API server to reject the update as invalid. It retries on conflicts, since
// the operator may be updating the object (e.g. adding its finalizer).
func expectInvalidUpdate(g *WithT, obj client.Object, mutate func()) {
	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		mutate()
		err := k8s.Update(context.Background(), obj)
		g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "err = %v", err)
	}, timeout, poll).Should(Succeed())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
