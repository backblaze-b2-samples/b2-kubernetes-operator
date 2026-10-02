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
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

// newRemoteCluster registers a RemoteCluster with the given kubeconfig.
func newRemoteCluster(t *testing.T, g *WithT, name string, kubeconfig []byte) *b2v1.RemoteCluster {
	t.Helper()
	ctx := context.Background()
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: name + "-kubeconfig"},
		Data:       map[string][]byte{"kubeconfig": kubeconfig},
	})).To(Succeed())
	rc := &b2v1.RemoteCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: b2v1.RemoteClusterSpec{KubeconfigSecretRef: b2v1.KubeconfigSecretReference{
			Namespace: operatorNS, Name: name + "-kubeconfig",
		}},
	}
	g.Expect(k8s.Create(ctx, rc)).To(Succeed())
	return rc
}

func getRemoteSecret(g Gomega, ns, name string) *corev1.Secret {
	var s corev1.Secret
	g.Expect(remoteK8s.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &s)).To(Succeed())
	return &s
}

func TestKeyDeliveredToRemoteCluster(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	rc := newRemoteCluster(t, g, uniqueCustomer("customer-"), remoteKubeconfig)
	eventuallyReason(g, rc, b2v1.ReasonReconciled)
	g.Expect(rc.Status.ServerVersion).NotTo(BeEmpty())

	ns := newNamespace(t, true)
	g.Expect(remoteK8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
	readyBucket(g, ns, "remote")
	key := newKey(ns, "app", "remote", "readFiles")
	key.Spec.DeliverTo = &b2v1.DeliveryTarget{RemoteCluster: rc.Name, Namespace: ns}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, b2v1.ReasonReconciled)
	g.Expect(key.Status.DeliveredTo).To(Equal(rc.Name + "/" + ns))

	// The Secret is in the remote cluster, marked as ours, and not local.
	s := getRemoteSecret(g, ns, "app")
	g.Expect(string(s.Data[b2v1.SecretKeyB2KeyID])).To(Equal(key.Status.KeyID))
	g.Expect(s.Annotations).To(HaveKeyWithValue(annotationOwnerUID, string(key.UID)))
	g.Expect(s.Annotations[annotationSource]).To(HaveSuffix("/" + ns + "/app"))
	g.Expect(apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "app"}, &corev1.Secret{}))).To(BeTrue())

	// Rotation updates the remote Secret.
	oldID := key.Status.KeyID
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
	key.Spec.Capabilities = append(key.Spec.Capabilities, "listFiles")
	g.Expect(k8s.Update(ctx, key)).To(Succeed())
	g.Eventually(func(g Gomega) {
		g.Expect(string(getRemoteSecret(g, ns, "app").Data[b2v1.SecretKeyB2KeyID])).NotTo(Equal(oldID))
	}, timeout, poll).Should(Succeed())

	// Deletion revokes the key and removes the remote Secret.
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
	currentID := key.Status.KeyID
	g.Expect(k8s.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(remoteK8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "app"}, &corev1.Secret{}))
	}, timeout, poll).Should(BeTrue())
	g.Eventually(func() bool { return fakeB2.Key(currentID) == nil }, timeout, poll).Should(BeTrue())
}

func TestRemoteDeliveryNeedsPolicy(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	rc := newRemoteCluster(t, g, uniqueCustomer("customer-"), remoteKubeconfig)
	eventuallyReason(g, rc, b2v1.ReasonReconciled)
	ns := newNamespace(t, true)
	readyBucket(g, ns, "denied")

	// The policy only allows the namespace of the same name.
	key := newKey(ns, "app", "denied", "readFiles")
	key.Spec.DeliverTo = &b2v1.DeliveryTarget{RemoteCluster: rc.Name, Namespace: "kube-system"}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	c := eventuallyReason(g, key, b2v1.ReasonPolicyDenied)
	g.Expect(c.Message).To(ContainSubstring("kube-system"))
	g.Expect(apierrors.IsNotFound(remoteK8s.Get(ctx, client.ObjectKey{Namespace: "kube-system", Name: "app"}, &corev1.Secret{}))).To(BeTrue())

	// So does an unregistered cluster name outside the allowed pattern.
	other := newKey(ns, "other", "denied", "readFiles")
	other.Spec.DeliverTo = &b2v1.DeliveryTarget{RemoteCluster: "prod-cluster", Namespace: ns}
	g.Expect(k8s.Create(ctx, other)).To(Succeed())
	eventuallyReason(g, other, b2v1.ReasonPolicyDenied)
}

func TestRemoteSecretConflictIsNotOverwritten(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	rc := newRemoteCluster(t, g, uniqueCustomer("customer-"), remoteKubeconfig)
	eventuallyReason(g, rc, b2v1.ReasonReconciled)
	ns := newNamespace(t, true)
	g.Expect(remoteK8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
	g.Expect(remoteK8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "app"},
		StringData: map[string]string{"password": "customer-owned"},
	})).To(Succeed())
	readyBucket(g, ns, "conflict")

	key := newKey(ns, "app", "conflict", "readFiles")
	key.Spec.DeliverTo = &b2v1.DeliveryTarget{RemoteCluster: rc.Name, Namespace: ns}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, b2v1.ReasonSecretConflict)
	g.Expect(string(getRemoteSecret(g, ns, "app").Data["password"])).To(Equal("customer-owned"))

	g.Expect(k8s.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(key), &b2v1.ApplicationKey{}))
	}, timeout, poll).Should(BeTrue())
	getRemoteSecret(g, ns, "app") // still there
}

func TestKeyWaitsForRemoteCluster(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "waiting")
	key := newKey(ns, "app", "waiting", "readFiles")
	key.Spec.DeliverTo = &b2v1.DeliveryTarget{RemoteCluster: "customer-not-registered", Namespace: ns}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, b2v1.ReasonRemoteClusterNotReady)
	g.Expect(key.Status.KeyID).To(BeEmpty(), "no key may be created before its Secret can be delivered")
	expectInvalidUpdate(g, key, func() { key.Spec.DeliverTo.Namespace = "elsewhere" })
}

func TestRemoteClusterRejectsUnsafeKubeconfig(t *testing.T) {
	g := requireEnv(t)
	unsafe := []byte(`apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: "https://example.invalid"}}]
users: [{name: u, user: {tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token}}]
contexts: [{name: x, context: {cluster: c, user: u}}]
current-context: x
`)
	rc := newRemoteCluster(t, g, uniqueCustomer("customer-"), unsafe)
	c := eventuallyReason(g, rc, b2v1.ReasonInvalidSpec)
	g.Expect(c.Message).To(ContainSubstring("local files"))
}
