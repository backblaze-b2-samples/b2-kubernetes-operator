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

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

// TestRemoteDelivery runs the central layout across two real clusters: the
// operator in one, and key Secrets delivered into a second that plays a
// customer's cluster. The second cluster is set up exactly as
// config/samples/08-remote-cluster.yaml describes: a ServiceAccount that can
// only manage Secrets in one namespace, behind a token kubeconfig.
//
// REMOTE_KUBECONTEXT is the test's own context for that cluster, and
// REMOTE_API_SERVER its API server address as seen from the operator's pods.
func TestRemoteDelivery(t *testing.T) {
	remoteContext, server := os.Getenv("REMOTE_KUBECONTEXT"), os.Getenv("REMOTE_API_SERVER")
	if remoteContext == "" || server == "" {
		t.Skip("REMOTE_KUBECONTEXT and REMOTE_API_SERVER are not set; run `make test-e2e`")
	}
	g := NewWithT(t)
	ctx := context.Background()
	c := newClient(t)
	remoteCfg := restConfig(t, remoteContext)
	remote := newClientFor(t, remoteCfg)
	suffix := time.Now().Unix() % 100000
	ns := fmt.Sprintf("e2e-remote-%d", suffix)
	storage := fmt.Sprintf("storage-%d", suffix)

	// Customer cluster: a namespace and an identity limited to its Secrets.
	g.Expect(remote.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: storage}})).To(Succeed())
	t.Cleanup(func() {
		_ = remote.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: storage}})
	})
	const sa = "b2-operator-delivery"
	for _, o := range []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: sa, Namespace: storage}},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: sa, Namespace: storage},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "create", "update", "delete"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: sa, Namespace: storage},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: sa},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: sa, Namespace: storage}},
		},
	} {
		g.Expect(remote.Create(ctx, o)).To(Succeed())
	}
	token, err := kubernetes.NewForConfigOrDie(remoteCfg).CoreV1().ServiceAccounts(storage).
		CreateToken(ctx, sa, &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: ptr[int64](3600)}}, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"customer": {Server: server, CertificateAuthorityData: remoteCfg.CAData}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"delivery": {Token: token.Status.Token}},
		Contexts:       map[string]*clientcmdapi.Context{"customer": {Cluster: "customer", AuthInfo: "delivery"}},
		CurrentContext: "customer",
	})
	g.Expect(err).NotTo(HaveOccurred())

	// Operator cluster: register the customer cluster and allow delivery.
	rcName := fmt.Sprintf("customer-%d", suffix)
	kubeconfigSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: rcName + "-kubeconfig", Namespace: operatorNS},
		Data:       map[string][]byte{"kubeconfig": kubeconfig},
	}
	rc := &b2v1.RemoteCluster{
		ObjectMeta: metav1.ObjectMeta{Name: rcName},
		Spec: b2v1.RemoteClusterSpec{KubeconfigSecretRef: b2v1.KubeconfigSecretReference{
			Namespace: operatorNS, Name: kubeconfigSecret.Name,
		}},
	}
	policy := &b2v1.B2AccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: rcName},
		Spec: b2v1.B2AccessPolicySpec{
			NamespaceSelector: metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": ns}},
			ProviderConfigs:   []string{"default"},
			Buckets:           b2v1.BucketPolicy{NamePatterns: []string{"{namespace}-*"}},
			Keys: b2v1.KeyPolicy{
				AllowedCapabilities:    []b2v1.Capability{"listFiles", "readFiles", "writeFiles"},
				AllowedDeliveryTargets: []b2v1.DeliveryTargetPattern{{RemoteCluster: rcName, Namespaces: []string{storage}}},
			},
		},
	}
	for _, o := range []client.Object{kubeconfigSecret, rc, policy, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}} {
		g.Expect(c.Create(ctx, o)).To(Succeed())
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		_ = c.Delete(ctx, policy)
		_ = c.Delete(ctx, rc)
		_ = c.Delete(ctx, kubeconfigSecret)
	})
	waitReady(g, c, rc, func() []metav1.Condition { return rc.Status.Conditions })
	g.Expect(rc.Status.ServerVersion).NotTo(BeEmpty())
	t.Logf("connected to the customer cluster (Kubernetes %s) at %s", rc.Status.ServerVersion, server)

	bkt := &b2v1.Bucket{
		ObjectMeta: metav1.ObjectMeta{Name: "datasets", Namespace: ns},
		Spec:       b2v1.BucketSpec{BucketName: ns + "-datasets"},
	}
	g.Expect(c.Create(ctx, bkt)).To(Succeed())
	waitReady(g, c, bkt, func() []metav1.Condition { return bkt.Status.Conditions })

	// The key's Secret lands in the customer cluster, not next to the key.
	key := &b2v1.ApplicationKey{
		ObjectMeta: metav1.ObjectMeta{Name: "uploads-writer", Namespace: ns},
		Spec: b2v1.ApplicationKeySpec{
			BucketRef:    &b2v1.LocalBucketReference{Name: "datasets"},
			Capabilities: []b2v1.Capability{"readFiles", "writeFiles"},
			DeliverTo:    &b2v1.DeliveryTarget{RemoteCluster: rcName, Namespace: storage},
		},
	}
	g.Expect(c.Create(ctx, key)).To(Succeed())
	waitReady(g, c, key, func() []metav1.Condition { return key.Status.Conditions })
	g.Expect(key.Status.DeliveredTo).To(Equal(rcName + "/" + storage))
	delivered := remoteSecret(g, remote, storage, "uploads-writer")
	g.Expect(string(delivered.Data[b2v1.SecretKeyB2KeyID])).To(Equal(key.Status.KeyID))
	g.Expect(delivered.Data).To(HaveKey(b2v1.SecretKeyAWSSecretAccessKey))
	g.Expect(delivered.Annotations).To(HaveKeyWithValue("b2.backblaze.com/owner-uid", string(key.UID)))
	g.Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "uploads-writer"}, &corev1.Secret{}))).To(BeTrue())

	// A spec change rotates the key, and the customer's Secret follows.
	oldID := key.Status.KeyID
	g.Eventually(func() error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(key), key); err != nil {
			return err
		}
		key.Spec.Capabilities = append(key.Spec.Capabilities, "listFiles")
		return c.Update(ctx, key)
	}, timeout, poll).Should(Succeed())
	g.Eventually(func(g Gomega) {
		g.Expect(string(remoteSecret(g, remote, storage, "uploads-writer").Data[b2v1.SecretKeyB2KeyID])).NotTo(Equal(oldID))
	}, timeout, poll).Should(Succeed())

	// A Secret the customer made is never overwritten.
	g.Expect(remote.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "theirs", Namespace: storage},
		StringData: map[string]string{"note": "created by the customer"},
	})).To(Succeed())
	clash := &b2v1.ApplicationKey{
		ObjectMeta: metav1.ObjectMeta{Name: "theirs", Namespace: ns},
		Spec: b2v1.ApplicationKeySpec{
			BucketRef:    &b2v1.LocalBucketReference{Name: "datasets"},
			Capabilities: []b2v1.Capability{"readFiles"},
			DeliverTo:    &b2v1.DeliveryTarget{RemoteCluster: rcName, Namespace: storage},
		},
	}
	g.Expect(c.Create(ctx, clash)).To(Succeed())
	g.Eventually(func(g Gomega) {
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(clash), clash)).To(Succeed())
		cond := meta.FindStatusCondition(clash.Status.Conditions, b2v1.ConditionReady)
		g.Expect(cond).NotTo(BeNil())
		g.Expect(cond.Reason).To(Equal(b2v1.ReasonSecretConflict))
	}, timeout, poll).Should(Succeed())
	g.Expect(remoteSecret(g, remote, storage, "theirs").Data).To(Equal(map[string][]byte{"note": []byte("created by the customer")}))
	g.Expect(c.Delete(ctx, clash)).To(Succeed())

	// Deleting the key removes the customer's copy; theirs is left alone.
	g.Expect(c.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(remote.Get(ctx, client.ObjectKey{Namespace: storage, Name: "uploads-writer"}, &corev1.Secret{}))
	}, timeout, poll).Should(BeTrue(), "delivered Secret not removed from the customer cluster")
	g.Consistently(func() error {
		return remote.Get(ctx, client.ObjectKey{Namespace: storage, Name: "theirs"}, &corev1.Secret{})
	}, 5*time.Second, poll).Should(Succeed())
}

func remoteSecret(g Gomega, c client.Client, namespace, name string) *corev1.Secret {
	s := &corev1.Secret{}
	g.Expect(c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, s)).To(Succeed())
	return s
}
