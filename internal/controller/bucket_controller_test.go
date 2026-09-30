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

	b2v1 "github.com/backblaze-b2-samples/b2-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2"
)

func newBucket(ns, name, bucketName string) *b2v1.Bucket {
	return &b2v1.Bucket{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       b2v1.BucketSpec{BucketName: bucketName},
	}
}

func bucketConds(b *b2v1.Bucket) func() []metav1.Condition {
	return func() []metav1.Condition { return b.Status.Conditions }
}

func TestProviderConfigReady(t *testing.T) {
	g := requireEnv(t)
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	eventuallyReason(g, pc, func() []metav1.Condition { return pc.Status.Conditions }, b2v1.ReasonReconciled)
	g.Expect(pc.Status.AccountID).To(Equal(fakeB2.AccountID))
	g.Expect(pc.Status.S3Region).To(Equal("us-west-004"))
	g.Expect(pc.Status.Capabilities).To(ContainElement("writeKeys"))
}

func TestProviderConfigBadCredentials(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	suffix := newNamespace(t, false) // unique per run
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-creds-" + suffix, Namespace: operatorNS},
		StringData: map[string]string{"applicationKeyId": "nope", "applicationKey": "nope"},
	})).To(Succeed())
	bad := &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-" + suffix},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               fakeB2.URL(),
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "bad-creds-" + suffix},
		},
	}
	missing := &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-" + suffix},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               fakeB2.URL(),
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "does-not-exist"},
		},
	}
	g.Expect(k8s.Create(ctx, bad)).To(Succeed())
	g.Expect(k8s.Create(ctx, missing)).To(Succeed())
	eventuallyReason(g, bad, func() []metav1.Condition { return bad.Status.Conditions }, b2v1.ReasonInvalidCredentials)
	eventuallyReason(g, missing, func() []metav1.Condition { return missing.Status.Conditions }, b2v1.ReasonCredentialsNotFound)
}

func TestBucketDeniedWithoutPolicy(t *testing.T) {
	g := requireEnv(t)
	ns := newNamespace(t, false)
	bkt := newBucket(ns, "data", ns+"-data")
	g.Expect(k8s.Create(context.Background(), bkt)).To(Succeed())

	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonPolicyDenied)
	g.Expect(fakeB2.Bucket(ns + "-data")).To(BeNil())
}

func TestBucketNameOutsidePolicyPattern(t *testing.T) {
	g := requireEnv(t)
	ns := newNamespace(t, true)
	bkt := newBucket(ns, "sneaky", "someone-elses-bucket")
	g.Expect(k8s.Create(context.Background(), bkt)).To(Succeed())

	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonPolicyDenied)
	g.Expect(fakeB2.Bucket("someone-elses-bucket")).To(BeNil())
}

func TestBucketCreateUpdateAndDriftCorrection(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	name := ns + "-logs"
	bkt := newBucket(ns, "logs", name)
	bkt.Spec.BucketInfo = map[string]string{"team": "a"}
	bkt.Spec.LifecycleRules = []b2v1.LifecycleRule{{FileNamePrefix: "tmp/", DaysFromHidingToDeleting: ptr[int32](1)}}
	bkt.Spec.DefaultEncryption = &b2v1.DefaultEncryption{Mode: b2v1.EncryptionModeSSEB2}
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())

	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonReconciled)
	got := fakeB2.Bucket(name)
	g.Expect(got).NotTo(BeNil())
	g.Expect(bkt.Status.BucketID).To(Equal(got.BucketID))
	g.Expect(bkt.Status.S3Endpoint).To(Equal(fakeB2.S3APIURL))
	g.Expect(got.BucketType).To(Equal(b2.BucketTypeAllPrivate))
	g.Expect(got.BucketInfo).To(HaveKeyWithValue("team", "a"))
	g.Expect(got.BucketInfo).To(HaveKeyWithValue(OwnerInfoKey, string(bkt.UID)))
	g.Expect(got.LifecycleRules).To(HaveLen(1))
	g.Expect(*got.DefaultServerSideEncryption.Value.Mode).To(Equal(b2.SSEModeB2))

	// Spec change is applied.
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(bkt), bkt)).To(Succeed())
	bkt.Spec.LifecycleRules = nil
	bkt.Spec.CORSRules = []b2v1.CORSRule{{
		Name: "web-app", AllowedOrigins: []string{"https://app.example.com"},
		AllowedOperations: []b2v1.CORSOperation{"s3_get", "s3_head"}, MaxAgeSeconds: 600,
	}}
	g.Expect(k8s.Update(ctx, bkt)).To(Succeed())
	g.Eventually(func(g Gomega) {
		b := fakeB2.Bucket(name)
		g.Expect(b.LifecycleRules).To(BeEmpty())
		g.Expect(b.CORSRules).To(HaveLen(1))
	}, timeout, poll).Should(Succeed())

	// An out-of-band change (someone makes the bucket public) is reverted.
	fakeB2.MutateBucket(name, func(b *b2.Bucket) { b.BucketType = b2.BucketTypeAllPublic })
	touch(g, bkt)
	g.Eventually(func() string { return fakeB2.Bucket(name).BucketType }, timeout, poll).Should(Equal(b2.BucketTypeAllPrivate))
}

func TestBucketObjectLock(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	name := ns + "-vault"
	bkt := newBucket(ns, "vault", name)
	bkt.Spec.ObjectLock = &b2v1.ObjectLock{
		Enabled:          true,
		DefaultRetention: &b2v1.DefaultRetention{Mode: b2v1.RetentionModeGovernance, Duration: 30, Unit: "days"},
	}
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())

	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonReconciled)
	g.Expect(bkt.Status.ObjectLockEnabled).To(BeTrue())
	fl := fakeB2.Bucket(name).FileLockConfiguration.Value
	g.Expect(fl.IsFileLockEnabled).To(BeTrue())
	g.Expect(*fl.DefaultRetention.Mode).To(Equal("governance"))
	g.Expect(fl.DefaultRetention.Period.Duration).To(BeEquivalentTo(30))

	// Disabling Object Lock is rejected by the API server.
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(bkt), bkt)).To(Succeed())
	bkt.Spec.ObjectLock = &b2v1.ObjectLock{Enabled: false}
	err := k8s.Update(ctx, bkt)
	g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "err = %v", err)
}

func TestBucketComplianceRetentionNeedsPolicy(t *testing.T) {
	g := requireEnv(t)
	ns := newNamespace(t, true)
	bkt := newBucket(ns, "locked", ns+"-locked")
	bkt.Spec.ObjectLock = &b2v1.ObjectLock{
		Enabled:          true,
		DefaultRetention: &b2v1.DefaultRetention{Mode: b2v1.RetentionModeCompliance, Duration: 1, Unit: "years"},
	}
	g.Expect(k8s.Create(context.Background(), bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonPolicyDenied)
}

func TestBucketExistingIsNotTakenOverWithoutAdoption(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	name := ns + "-legacy"
	c := b2.New(b2.Options{BaseURL: fakeB2.URL(), ApplicationKeyID: fakeB2.MasterKeyID, ApplicationKey: fakeB2.MasterKey})
	_, err := c.CreateBucket(ctx, b2.CreateBucketRequest{
		BucketName: name, BucketType: b2.BucketTypeAllPrivate, BucketInfo: map[string]string{"owner": "ops"},
	})
	g.Expect(err).NotTo(HaveOccurred())

	bkt := newBucket(ns, "legacy", name)
	bkt.Spec.BucketType = b2v1.BucketTypeAllPrivate
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonBucketExists)
	g.Expect(fakeB2.Bucket(name).BucketInfo).NotTo(HaveKey(OwnerInfoKey))

	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(bkt), bkt)).To(Succeed())
	bkt.Spec.AdoptExisting = true
	bkt.Spec.BucketInfo = map[string]string{"owner": "ops"}
	g.Expect(k8s.Update(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonReconciled)
	g.Expect(fakeB2.Bucket(name).BucketInfo).To(HaveKeyWithValue(OwnerInfoKey, string(bkt.UID)))

	// A second resource cannot claim the same bucket, even with adoption.
	other := newBucket(ns, "legacy-2", name)
	other.Spec.AdoptExisting = true
	g.Expect(k8s.Create(ctx, other)).To(Succeed())
	eventuallyReason(g, other, bucketConds(other), b2v1.ReasonBucketOwnedElsewhere)
}

func TestBucketNameTakenByAnotherAccount(t *testing.T) {
	g := requireEnv(t)
	ns := newNamespace(t, true)
	name := ns + "-taken"
	fakeB2.ReserveBucketName(name)
	bkt := newBucket(ns, "taken", name)
	g.Expect(k8s.Create(context.Background(), bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonBucketNameUnavailable)
}

func TestBucketDeletionRetainReleasesOwnership(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	name := ns + "-keep"
	bkt := newBucket(ns, "keep", name)
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonReconciled)

	g.Expect(k8s.Delete(ctx, bkt)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(bkt), &b2v1.Bucket{}))
	}, timeout, poll).Should(BeTrue())
	got := fakeB2.Bucket(name)
	g.Expect(got).NotTo(BeNil(), "Retain must keep the bucket")
	g.Expect(got.BucketInfo).NotTo(HaveKey(OwnerInfoKey))
	g.Expect(got.BucketInfo).To(HaveKeyWithValue(ReleasedInfoKey, ns))
}

func TestBucketDeletionWaitsForItsKeys(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	bkt := readyBucket(g, ns, "haskeys")
	key := newKey(ns, "app", "haskeys", "readFiles")
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	eventuallyReason(g, key, keyConds(key), b2v1.ReasonReconciled)
	keyID := key.Status.KeyID

	g.Expect(k8s.Delete(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonDeletionBlocked)
	g.Expect(fakeB2.Bucket(bkt.Spec.BucketName).BucketInfo).To(HaveKey(OwnerInfoKey), "must not release the bucket while keys exist")

	g.Expect(k8s.Delete(ctx, key)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(bkt), &b2v1.Bucket{}))
	}, timeout, poll).Should(BeTrue())
	g.Expect(fakeB2.Key(keyID)).To(BeNil())
}

// Name patterns like "{namespace}-*" also match the names of namespaces that
// extend another's name ("a" vs "a-b"). Ownership in B2 must still keep
// each namespace out of the other's buckets.
func TestCrossNamespacePrefixOverlap(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	// The victim's namespace extends the attacker's name, so the victim's
	// buckets ("<attacker>-b-...") match the attacker's pattern "{namespace}-*".
	attacker := newNamespace(t, false)
	var ns corev1.Namespace
	g.Expect(k8s.Get(ctx, client.ObjectKey{Name: attacker}, &ns)).To(Succeed())
	ns.Labels = map[string]string{externalLabel: "true"}
	g.Expect(k8s.Update(ctx, &ns)).To(Succeed())
	victim := createNamespace(t, attacker+"-b", map[string]string{tenantLabel: "true"})
	victimBucket := readyBucket(g, victim, "data")
	name := victimBucket.Spec.BucketName

	// An external key for the victim's live bucket is refused.
	steal := newKey(attacker, "steal", "", "readFiles")
	steal.Spec.BucketName = name
	g.Expect(k8s.Create(ctx, steal)).To(Succeed())
	eventuallyReason(g, steal, keyConds(steal), b2v1.ReasonPolicyDenied)
	g.Expect(readyCondition(g, steal, keyConds(steal)).Message).To(ContainSubstring("outside namespace"))
	g.Expect(steal.Status.KeyID).To(BeEmpty())

	// Once the victim retains and releases it, the attacker cannot adopt it.
	g.Expect(k8s.Delete(ctx, victimBucket)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(victimBucket), &b2v1.Bucket{}))
	}, timeout, poll).Should(BeTrue())
	adopt := newBucket(attacker, "adopt", name)
	adopt.Spec.AdoptExisting = true
	g.Expect(k8s.Create(ctx, adopt)).To(Succeed())
	eventuallyReason(g, adopt, bucketConds(adopt), b2v1.ReasonBucketOwnedElsewhere)
	g.Expect(fakeB2.Bucket(name).BucketInfo).NotTo(HaveKey(OwnerInfoKey))

	// Nor can it get a key for the released bucket.
	touch(g, steal)
	g.Eventually(func(g Gomega) {
		c := readyCondition(g, steal, keyConds(steal))
		g.Expect(c.Message).To(ContainSubstring("released by namespace"))
	}, timeout, poll).Should(Succeed())
}

func TestBucketDeletionDeleteWaitsForEmptyBucket(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	name := ns + "-scratch"
	bkt := newBucket(ns, "scratch", name)
	bkt.Spec.DeletionPolicy = b2v1.DeletionPolicyDelete
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonReconciled)

	fakeB2.SetBucketHasFiles(name, true)
	g.Expect(k8s.Delete(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonDeletionBlocked)
	g.Expect(fakeB2.Bucket(name)).NotTo(BeNil())

	fakeB2.SetBucketHasFiles(name, false)
	touch(g, bkt)
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(bkt), &b2v1.Bucket{}))
	}, timeout, poll).Should(BeTrue())
	g.Expect(fakeB2.Bucket(name)).To(BeNil())
}

func TestDeniedBucketDeletesWithoutProviderConfig(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, false)
	bkt := newBucket(ns, "orphan-config", ns+"-x")
	bkt.Spec.ProviderConfigRef.Name = "does-not-exist"
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonProviderNotReady)

	g.Expect(k8s.Delete(ctx, bkt)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(bkt), &b2v1.Bucket{}))
	}, timeout, poll).Should(BeTrue())
}

func TestBucketNameIsImmutable(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	bkt := newBucket(ns, "fixed", ns+"-fixed")
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())
	bkt.Spec.BucketName = ns + "-renamed"
	err := k8s.Update(ctx, bkt)
	g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "err = %v", err)
}

func TestBucketNameValidation(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true)
	for _, name := range []string{"b2-reserved", "short", "has..dots", "192.168.1.10", "-leading"} {
		err := k8s.Create(ctx, newBucket(ns, "invalid", name))
		g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "bucket name %q: err = %v", name, err)
	}
}

func ptr[T any](v T) *T { return &v }
