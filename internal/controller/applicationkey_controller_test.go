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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2/b2fake"
)

func keyConds(k *b2v1.ApplicationKey) func() []metav1.Condition {
	return func() []metav1.Condition { return k.Status.Conditions }
}

// readyBucket creates a Bucket and waits for it to be reconciled.
func readyBucket(g *WithT, ns, name string) *b2v1.Bucket {
	bkt := newBucket(ns, name, ns+"-"+name)
	g.Expect(k8s.Create(context.Background(), bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonReconciled)
	return bkt
}

func newKey(ns, name, bucketRef string, caps ...b2v1.Capability) *b2v1.ApplicationKey {
	k := &b2v1.ApplicationKey{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       b2v1.ApplicationKeySpec{Capabilities: caps},
	}
	if bucketRef != "" {
		k.Spec.BucketRef = &b2v1.LocalBucketReference{Name: bucketRef}
	}
	return k
}

func getSecret(g Gomega, ns, name string) *corev1.Secret {
	var s corev1.Secret
	g.Expect(k8s.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &s)).To(Succeed())
	return &s
}

func TestKeyScopedToBucketWithPrefix(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	bkt := readyBucket(g, ns, "reports")

	key := newKey(ns, "reader", "reports", "listFiles", "readFiles")
	key.Spec.NamePrefix = "2026/"
	key.Spec.ValidFor = &metav1.Duration{Duration: 48 * time.Hour}
	key.Spec.SecretTemplate = &b2v1.SecretTemplate{Labels: map[string]string{"app": "reports"}}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)

	b2key := fakeB2.Key(key.Status.KeyID)
	g.Expect(b2key).NotTo(BeNil())
	g.Expect(b2key.BucketIDs).To(ConsistOf(bkt.Status.BucketID))
	g.Expect(*b2key.NamePrefix).To(Equal("2026/"))
	g.Expect(b2key.Capabilities).To(ConsistOf("listFiles", "readFiles"))
	g.Expect(b2key.ExpirationTimestamp).NotTo(BeNil())
	g.Expect(key.Status.ExpiresAt).NotTo(BeNil())
	g.Expect(len(b2key.KeyName)).To(BeNumerically("<=", 100))

	s := getSecret(g, ns, "reader")
	g.Expect(metav1.IsControlledBy(s, key)).To(BeTrue())
	g.Expect(s.Labels).To(HaveKeyWithValue("app", "reports"))
	g.Expect(s.Labels).To(HaveKeyWithValue(LabelManagedBy, ManagedByValue))
	g.Expect(string(s.Data[b2v1.SecretKeyAWSAccessKeyID])).To(Equal(key.Status.KeyID))
	g.Expect(s.Data[b2v1.SecretKeyAWSSecretAccessKey]).NotTo(BeEmpty())
	g.Expect(string(s.Data[b2v1.SecretKeyAWSEndpointURL])).To(Equal(fakeB2.S3APIURL))
	g.Expect(string(s.Data[b2v1.SecretKeyAWSRegion])).To(Equal("us-west-004"))
	g.Expect(string(s.Data[b2v1.SecretKeyB2BucketName])).To(Equal(bkt.Spec.BucketName))
	g.Expect(string(s.Data[b2v1.SecretKeyB2NamePrefix])).To(Equal("2026/"))

	// The delivered credentials actually work.
	kc := b2.New(b2.Options{BaseURL: fakeB2.URL(), ApplicationKeyID: string(s.Data[b2v1.SecretKeyB2KeyID]), ApplicationKey: string(s.Data[b2v1.SecretKeyB2Key])})
	_, err := kc.Authorize(ctx)
	g.Expect(err).NotTo(HaveOccurred())
}

func TestKeyWaitsForBucket(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	key := newKey(ns, "early", "later", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonBucketNotFound)

	readyBucket(g, ns, "later")
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
}

func TestKeySpecChangeRotatesWithGracePeriod(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "rotate")
	key := newKey(ns, "app", "rotate", "readFiles")
	key.Spec.Rotation = &b2v1.KeyRotation{GracePeriod: &metav1.Duration{Duration: 3 * time.Second}}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	oldID := key.Status.KeyID

	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
	key.Spec.Capabilities = append(key.Spec.Capabilities, "writeFiles")
	g.Expect(k8s.Update(ctx, key)).To(Succeed())

	var newID string
	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
		g.Expect(key.Status.KeyID).NotTo(Equal(oldID))
		newID = key.Status.KeyID
	}, timeout, poll).Should(Succeed())

	// The Secret has the new key; the old one stays valid during the grace period.
	g.Expect(string(getSecret(g, ns, "app").Data[b2v1.SecretKeyB2KeyID])).To(Equal(newID))
	g.Expect(fakeB2.Key(oldID)).NotTo(BeNil(), "old key must survive the grace period")
	g.Expect(fakeB2.Key(newID).Capabilities).To(ConsistOf("readFiles", "writeFiles"))

	g.Eventually(func() *b2.ApplicationKey { return fakeB2.Key(oldID) }, timeout, poll).Should(BeNil())
	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
		g.Expect(key.Status.RetiringKeys).To(BeEmpty())
	}, timeout, poll).Should(Succeed())
}

func TestKeyReplacedWhenSecretDeleted(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "lost")
	key := newKey(ns, "app", "lost", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	oldID := key.Status.KeyID

	g.Expect(k8s.Delete(ctx, getSecret(g, ns, "app"))).To(Succeed())
	g.Eventually(func(g Gomega) {
		s := getSecret(g, ns, "app")
		g.Expect(string(s.Data[b2v1.SecretKeyB2KeyID])).NotTo(Equal(oldID))
		g.Expect(s.Data[b2v1.SecretKeyB2KeyID]).NotTo(BeEmpty())
	}, timeout, poll).Should(Succeed())
}

func TestKeyReplacedWhenRevokedOutsideKubernetes(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "revoked")
	key := newKey(ns, "app", "revoked", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	oldID := key.Status.KeyID

	fakeB2.DeleteKeyDirect(oldID)
	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
		g.Expect(key.Status.KeyID).NotTo(Equal(oldID))
		g.Expect(fakeB2.Key(key.Status.KeyID)).NotTo(BeNil())
	}, timeout, poll).Should(Succeed())
}

func TestKeyDoesNotOverwriteUnownedSecret(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "conflict")
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "precious", Namespace: ns},
		StringData: map[string]string{"password": "hunter2"},
	})).To(Succeed())
	before := len(fakeB2.Keys())

	key := newKey(ns, "app", "conflict", "readFiles")
	key.Spec.SecretName = "precious"
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonSecretConflict)

	g.Expect(string(getSecret(g, ns, "precious").Data["password"])).To(Equal("hunter2"))
	g.Expect(fakeB2.Keys()).To(HaveLen(before), "no key may be created for a conflicting Secret")

	// Deleting the key must not delete the Secret it never owned.
	g.Expect(k8s.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(key), &b2v1.ApplicationKey{}))
	}, timeout, poll).Should(BeTrue())
	getSecret(g, ns, "precious")
}

func TestKeyDeletionRevokesKey(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "cleanup")
	key := newKey(ns, "app", "cleanup", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	id := key.Status.KeyID

	g.Expect(k8s.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() *b2.ApplicationKey { return fakeB2.Key(id) }, timeout, poll).Should(BeNil())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "app"}, &corev1.Secret{}))
	}, timeout, poll).Should(BeTrue())
}

func TestKeyPolicyDenials(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "policy")

	accountWide := newKey(ns, "account-wide", "", "listBuckets")
	notAllowedCap := newKey(ns, "too-strong", "policy", "readFiles", "shareFiles")
	keyManagement := newKey(ns, "escalate", "policy", "writeKeys")
	external := newKey(ns, "external", "", "readFiles")
	external.Spec.BucketName = ns + "-policy"
	for _, k := range []*b2v1.ApplicationKey{accountWide, notAllowedCap, keyManagement, external} {
		g.Expect(k8s.Create(ctx, k)).To(Succeed())
	}
	for _, k := range []*b2v1.ApplicationKey{accountWide, notAllowedCap, keyManagement, external} {
		eventuallyReason(g, k, keyConds(k), b2v1.ReasonPolicyDenied)
		g.Expect(k.Status.KeyID).To(BeEmpty())
	}
	c := readyCondition(g, keyManagement, keyConds(keyManagement))
	g.Expect(c.Message).To(ContainSubstring("disabled by the operator"))
}

func TestKeyRevokedWhenPolicyNoLongerAllowsIt(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "tighten")
	key := newKey(ns, "app", "tighten", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	id := key.Status.KeyID

	// Remove the namespace from the tenant policy.
	var nsObj corev1.Namespace
	g.Expect(k8s.Get(ctx, client.ObjectKey{Name: ns}, &nsObj)).To(Succeed())
	delete(nsObj.Labels, tenantLabel)
	g.Expect(k8s.Update(ctx, &nsObj)).To(Succeed())

	// Revocation is scheduled, not immediate: the key survives a brief gap.
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonPolicyDenied)
	g.Expect(key.Status.ScheduledRevocation).NotTo(BeNil())
	g.Expect(fakeB2.Key(id)).NotTo(BeNil(), "key must not be revoked before the grace period")
	g.Eventually(func() *b2.ApplicationKey { return fakeB2.Key(id) }, timeout, poll).Should(BeNil())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "app"}, &corev1.Secret{}))
	}, timeout, poll).Should(BeTrue())
}

func TestKeyOrphanFromLostCreateResponseIsRevoked(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "orphan")

	// B2 creates the key but the response is lost.
	fakeB2.InjectFault("b2_create_key", b2fake.Fault{Status: 503, Code: "service_unavailable", AfterApply: true})
	key := newKey(ns, "app", "orphan", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)

	prefix := KeyNamePrefix + "-" + testClusterID + "-" + uid8(key.UID) + "-"
	var ours []string
	for _, k := range fakeB2.Keys() {
		if strings.HasPrefix(k.KeyName, prefix) {
			ours = append(ours, k.ApplicationKeyID)
		}
	}
	g.Expect(ours).To(ConsistOf(key.Status.KeyID), "the orphaned key from the lost response must be revoked")
}

func TestKeyValidationRules(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)

	both := newKey(ns, "both", "b", "readFiles")
	both.Spec.BucketName = "some-bucket"
	prefixOnly := newKey(ns, "prefix-only", "", "readFiles")
	prefixOnly.Spec.NamePrefix = "x/"
	tooLong := newKey(ns, "too-long", "b", "readFiles")
	tooLong.Spec.ValidFor = &metav1.Duration{Duration: 1001 * 24 * time.Hour}
	badCap := newKey(ns, "bad-cap", "b", "doEverything")
	for _, k := range []*b2v1.ApplicationKey{both, prefixOnly, tooLong, badCap} {
		err := k8s.Create(ctx, k)
		g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "%s: err = %v", k.Name, err)
	}

	ok := newKey(ns, "named", "b", "readFiles")
	ok.Spec.SecretName = "first"
	g.Expect(k8s.Create(ctx, ok)).To(Succeed())
	ok.Spec.SecretName = "second"
	err := k8s.Update(ctx, ok)
	g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "secretName change: err = %v", err)
}

func TestKeyPolicyGapIsTolerated(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "gap")
	key := newKey(ns, "app", "gap", "readFiles")
	key.Spec.Rotation = &b2v1.KeyRotation{GracePeriod: &metav1.Duration{Duration: time.Hour}}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	id := key.Status.KeyID

	var nsObj corev1.Namespace
	g.Expect(k8s.Get(ctx, client.ObjectKey{Name: ns}, &nsObj)).To(Succeed())
	delete(nsObj.Labels, tenantLabel)
	g.Expect(k8s.Update(ctx, &nsObj)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonPolicyDenied)

	nsObj.Labels[tenantLabel] = "true"
	g.Expect(k8s.Update(ctx, &nsObj)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	g.Expect(key.Status.KeyID).To(Equal(id), "the key must survive a policy gap shorter than its grace period")
	g.Expect(key.Status.ScheduledRevocation).To(BeNil())
	g.Expect(fakeB2.Key(id)).NotTo(BeNil())
}

func TestKeyRecoveredAfterInterruptedCreate(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "recover")
	key := newKey(ns, "app", "recover", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	id := key.Status.KeyID

	// Simulate the operator stopping after writing the Secret but before
	// recording the new key in status.
	base := key.DeepCopy()
	key.Status.PendingKeyName = key.Status.KeyName
	key.Status.KeyID = ""
	g.Expect(k8s.Status().Patch(ctx, key, client.MergeFrom(base))).To(Succeed())
	touch(g, key)

	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
		g.Expect(key.Status.PendingKeyName).To(BeEmpty())
		g.Expect(key.Status.KeyID).To(Equal(id))
	}, timeout, poll).Should(Succeed())
	g.Expect(fakeB2.Key(id)).NotTo(BeNil(), "the delivered key must be recovered, not revoked")
	g.Expect(string(getSecret(g, ns, "app").Data[b2v1.SecretKeyB2KeyID])).To(Equal(id))
}

func TestSweepRevokesKeysOfForceDeletedResources(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	readyBucket(g, ns, "sweep")
	key := newKey(ns, "app", "sweep", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	id := key.Status.KeyID
	other := fakeB2.Keys() // keys of live resources and foreign keys must survive

	// A tenant strips the finalizer and deletes the resource.
	base := key.DeepCopy()
	key.Finalizers = nil
	g.Expect(k8s.Patch(ctx, key, client.MergeFrom(base))).To(Succeed())
	g.Expect(k8s.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(key), &b2v1.ApplicationKey{}))
	}, timeout, poll).Should(BeTrue())
	g.Expect(fakeB2.Key(id)).NotTo(BeNil())
	foreignID, _ := fakeB2.AddKey("b2op-othrclst-deadbeef-1-x-y", []string{"readFiles"}, nil, "")

	g.Expect(sweeper.Sweep(ctx)).To(Succeed())
	g.Expect(fakeB2.Key(id)).To(BeNil(), "orphaned key must be revoked")
	g.Expect(fakeB2.Key(foreignID)).NotTo(BeNil(), "another cluster's key must not be touched")
	for _, k := range other {
		if k.ApplicationKeyID != id {
			g.Expect(fakeB2.Key(k.ApplicationKeyID)).NotTo(BeNil(), "key %s of a live resource was revoked", k.KeyName)
		}
	}
}
