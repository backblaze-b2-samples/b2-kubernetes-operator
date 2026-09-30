//go:build e2e

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

// Package e2e installs the operator image and Helm chart into a kind cluster
// (see `make test-e2e`) and exercises it end to end against the fake B2 API
// running in the cluster.
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

const (
	operatorNS = "b2-operator-system"
	timeout    = 2 * time.Minute
	poll       = time.Second
)

func newClient(t *testing.T) client.Client {
	t.Helper()
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: os.Getenv("KUBECONTEXT")}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = b2v1.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func applyOrUpdate(ctx context.Context, c client.Client, obj client.Object) error {
	err := c.Create(ctx, obj)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func waitReady(g *WithT, c client.Client, obj client.Object, conds func() []metav1.Condition) {
	g.Eventually(func(g Gomega) {
		g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		cond := meta.FindStatusCondition(conds(), b2v1.ConditionReady)
		g.Expect(cond).NotTo(BeNil())
		g.Expect(cond.Status).To(Equal(metav1.ConditionTrue), "reason %s: %s", cond.Reason, cond.Message)
	}, timeout, poll).Should(Succeed())
}

func TestEndToEnd(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	c := newClient(t)
	ns := fmt.Sprintf("e2e-%d", time.Now().Unix()%100000)

	// Cluster-admin setup: credentials, provider config, policy.
	g.Expect(applyOrUpdate(ctx, c, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "b2-credentials", Namespace: operatorNS},
		StringData: map[string]string{"applicationKeyId": "e2eaccount01", "applicationKey": "e2e-master-key"},
	})).To(Succeed())
	pc := &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               "http://b2fake." + operatorNS + ".svc:8080",
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "b2-credentials"},
		},
	}
	g.Expect(applyOrUpdate(ctx, c, pc)).To(Succeed())
	g.Expect(applyOrUpdate(ctx, c, &b2v1.B2AccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-tenants"},
		Spec: b2v1.B2AccessPolicySpec{
			NamespaceSelector: metav1.LabelSelector{MatchLabels: map[string]string{"b2.backblaze.com/tenant": "true"}},
			ProviderConfigs:   []string{"default"},
			Buckets:           b2v1.BucketPolicy{NamePatterns: []string{"{namespace}-*"}},
			Keys:              b2v1.KeyPolicy{AllowedCapabilities: []b2v1.Capability{"listFiles", "readFiles", "writeFiles"}},
		},
	})).To(Succeed())
	waitReady(g, c, pc, func() []metav1.Condition { return pc.Status.Conditions })

	// Tenant: namespace, bucket, key.
	g.Expect(c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: ns, Labels: map[string]string{"b2.backblaze.com/tenant": "true"},
	}})).To(Succeed())
	t.Cleanup(func() { _ = c.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}) })

	bkt := &b2v1.Bucket{
		ObjectMeta: metav1.ObjectMeta{Name: "media", Namespace: ns},
		Spec: b2v1.BucketSpec{
			BucketName:     ns + "-media",
			LifecycleRules: []b2v1.LifecycleRule{{FileNamePrefix: "", DaysFromStartingToCancelingUnfinishedLargeFiles: ptr[int32](7)}},
		},
	}
	g.Expect(c.Create(ctx, bkt)).To(Succeed())
	waitReady(g, c, bkt, func() []metav1.Condition { return bkt.Status.Conditions })
	g.Expect(bkt.Status.BucketID).NotTo(BeEmpty())

	key := &b2v1.ApplicationKey{
		ObjectMeta: metav1.ObjectMeta{Name: "media-rw", Namespace: ns},
		Spec: b2v1.ApplicationKeySpec{
			BucketRef:    &b2v1.LocalBucketReference{Name: "media"},
			Capabilities: []b2v1.Capability{"listFiles", "readFiles", "writeFiles"},
		},
	}
	g.Expect(c.Create(ctx, key)).To(Succeed())
	waitReady(g, c, key, func() []metav1.Condition { return key.Status.Conditions })

	var secret corev1.Secret
	g.Expect(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "media-rw"}, &secret)).To(Succeed())
	for _, k := range []string{b2v1.SecretKeyAWSAccessKeyID, b2v1.SecretKeyAWSSecretAccessKey, b2v1.SecretKeyAWSEndpointURL, b2v1.SecretKeyB2BucketName} {
		g.Expect(secret.Data).To(HaveKey(k))
	}

	// A disallowed capability is denied by policy.
	bad := &b2v1.ApplicationKey{
		ObjectMeta: metav1.ObjectMeta{Name: "too-strong", Namespace: ns},
		Spec: b2v1.ApplicationKeySpec{
			BucketRef:    &b2v1.LocalBucketReference{Name: "media"},
			Capabilities: []b2v1.Capability{"deleteFiles"},
		},
	}
	g.Expect(c.Create(ctx, bad)).To(Succeed())
	g.Eventually(func(g Gomega) {
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(bad), bad)).To(Succeed())
		cond := meta.FindStatusCondition(bad.Status.Conditions, b2v1.ConditionReady)
		g.Expect(cond).NotTo(BeNil())
		g.Expect(cond.Reason).To(Equal(b2v1.ReasonPolicyDenied))
	}, timeout, poll).Should(Succeed())

	// Deleting the key revokes it and removes its Secret.
	g.Expect(c.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "media-rw"}, &corev1.Secret{}))
	}, timeout, poll).Should(BeTrue())

	// Deleting the bucket (Retain) completes.
	g.Expect(c.Delete(ctx, bkt)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(bkt), &b2v1.Bucket{}))
	}, timeout, poll).Should(BeTrue())

	// The operator never restarted.
	var pods corev1.PodList
	g.Expect(c.List(ctx, &pods, client.InNamespace(operatorNS), client.MatchingLabels{"app.kubernetes.io/name": "b2-operator"})).To(Succeed())
	g.Expect(pods.Items).NotTo(BeEmpty())
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			g.Expect(cs.RestartCount).To(BeZero(), "pod %s restarted", p.Name)
		}
	}
}

func ptr[T any](v T) *T { return &v }
