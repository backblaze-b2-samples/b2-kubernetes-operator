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

package live

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/controller"
)

func newBucket(name, suffix string) *b2v1.Bucket {
	return &b2v1.Bucket{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tenantNS},
		Spec:       b2v1.BucketSpec{BucketName: bucketName(suffix), DeletionPolicy: b2v1.DeletionPolicyDelete},
	}
}

func deleteBucket(g *WithT, b *b2v1.Bucket) {
	g.Expect(client.IgnoreNotFound(k8s.Delete(context.Background(), b))).To(Succeed())
	waitGone(g, b)
	g.Expect(b2Bucket(g, b.Spec.BucketName)).To(BeNil(), "bucket %s still exists in B2", b.Spec.BucketName)
}

// expectStable resyncs twice and checks B2's revision did not move: the
// operator must see its own writes as in sync, or it would rewrite the
// bucket on every resync.
func expectStable(g *WithT, b *b2v1.Bucket) {
	rev := b2Bucket(g, b.Spec.BucketName).Revision
	resync(g, b)
	resync(g, b)
	g.Expect(b2Bucket(g, b.Spec.BucketName).Revision).To(Equal(rev),
		"the operator keeps updating bucket %s: its desired state does not match how B2 reports it", b.Spec.BucketName)
}

func TestLiveBucketSettings(t *testing.T) {
	g := requireLive(t)
	ctx := context.Background()
	b := newBucket("settings", "settings")
	b.Spec.BucketInfo = map[string]string{"team": "live-test"}
	b.Spec.LifecycleRules = []b2v1.LifecycleRule{
		{FileNamePrefix: "tmp/", DaysFromUploadingToHiding: ptr[int32](1), DaysFromHidingToDeleting: ptr[int32](1)},
		{FileNamePrefix: "", DaysFromStartingToCancelingUnfinishedLargeFiles: ptr[int32](7)},
	}
	b.Spec.CORSRules = []b2v1.CORSRule{{
		Name: "live-web", AllowedOrigins: []string{"https://example.com"},
		AllowedOperations: []b2v1.CORSOperation{"s3_get", "s3_head"}, AllowedHeaders: []string{"range"}, MaxAgeSeconds: 600,
	}}
	g.Expect(k8s.Create(ctx, b)).To(Succeed())
	t.Cleanup(func() { deleteBucket(NewWithT(t), b) })
	waitReady(g, b, bucketConds(b))

	got := b2Bucket(g, b.Spec.BucketName)
	g.Expect(got).NotTo(BeNil())
	g.Expect(got.BucketType).To(Equal(b2.BucketTypeAllPrivate))
	g.Expect(got.BucketInfo).To(HaveKeyWithValue("team", "live-test"))
	g.Expect(got.BucketInfo).To(HaveKeyWithValue(controller.OwnerInfoKey, string(b.UID)))
	g.Expect(got.LifecycleRules).To(HaveLen(2))
	g.Expect(got.CORSRules).To(HaveLen(1))
	g.Expect(got.CORSRules[0].AllowedOperations).To(ConsistOf("s3_get", "s3_head"))
	g.Expect(got.DefaultServerSideEncryption).NotTo(BeNil())
	g.Expect(got.DefaultServerSideEncryption.Value.Mode).To(HaveValue(Equal(b2.SSEModeB2)), "SSE-B2 must be on by default")
	expectStable(g, b)

	// Clearing lifecycle and CORS and turning encryption off are applied,
	// and B2's representation of "none" is recognised as in sync.
	update(g, b, func() {
		b.Spec.LifecycleRules = nil
		b.Spec.CORSRules = nil
		b.Spec.DefaultEncryption = &b2v1.DefaultEncryption{Mode: b2v1.EncryptionModeNone}
	})
	waitReady(g, b, bucketConds(b))
	got = b2Bucket(g, b.Spec.BucketName)
	g.Expect(got.LifecycleRules).To(BeEmpty())
	g.Expect(got.CORSRules).To(BeEmpty())
	if sse := got.DefaultServerSideEncryption; sse != nil && sse.Value != nil {
		g.Expect(sse.Value.Mode).To(Or(BeNil(), HaveValue(BeEmpty())), "encryption should be off")
	}
	if sse := got.DefaultServerSideEncryption; sse != nil {
		t.Logf("B2 reports disabled default encryption as: authorized=%v value=%+v", sse.IsClientAuthorizedToRead, sse.Value)
	}
	expectStable(g, b)

	// A change made outside Kubernetes is reverted.
	_, err := direct.UpdateBucket(ctx, b2.UpdateBucketRequest{BucketID: got.BucketID, BucketType: b2.BucketTypeAllPublic})
	g.Expect(err).NotTo(HaveOccurred())
	resync(g, b)
	g.Eventually(func() string { return b2Bucket(g, b.Spec.BucketName).BucketType }, timeout, poll).Should(Equal(b2.BucketTypeAllPrivate))
}

func TestLiveObjectLock(t *testing.T) {
	g := requireLive(t)
	ctx := context.Background()
	b := newBucket("locked", "locked")
	b.Spec.ObjectLock = &b2v1.ObjectLock{
		Enabled:          true,
		DefaultRetention: &b2v1.DefaultRetention{Mode: b2v1.RetentionModeGovernance, Duration: 1, Unit: "days"},
	}
	g.Expect(k8s.Create(ctx, b)).To(Succeed())
	t.Cleanup(func() { deleteBucket(NewWithT(t), b) })
	waitReady(g, b, bucketConds(b))

	fl := b2Bucket(g, b.Spec.BucketName).FileLockConfiguration
	g.Expect(fl.Value.IsFileLockEnabled).To(BeTrue())
	g.Expect(fl.Value.DefaultRetention.Mode).To(HaveValue(Equal("governance")))
	g.Expect(fl.Value.DefaultRetention.Period.Duration).To(BeEquivalentTo(1))
	expectStable(g, b)

	update(g, b, func() { b.Spec.ObjectLock.DefaultRetention.Duration = 2 })
	waitReady(g, b, bucketConds(b))
	g.Expect(b2Bucket(g, b.Spec.BucketName).FileLockConfiguration.Value.DefaultRetention.Period.Duration).To(BeEquivalentTo(2))

	// Clearing the default retention, and B2's representation of "none".
	update(g, b, func() { b.Spec.ObjectLock.DefaultRetention = nil })
	waitReady(g, b, bucketConds(b))
	dr := b2Bucket(g, b.Spec.BucketName).FileLockConfiguration.Value.DefaultRetention
	t.Logf("B2 reports cleared default retention as: %+v", dr)
	if dr != nil {
		g.Expect(dr.Mode).To(Or(BeNil(), HaveValue(BeEmpty())))
	}
	expectStable(g, b)
}

func TestLiveApplicationKeyLifecycle(t *testing.T) {
	g := requireLive(t)
	ctx := context.Background()
	b := newBucket("keys", "keys")
	g.Expect(k8s.Create(ctx, b)).To(Succeed())
	t.Cleanup(func() { deleteBucket(NewWithT(t), b) })
	waitReady(g, b, bucketConds(b))

	key := &b2v1.ApplicationKey{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: tenantNS},
		Spec: b2v1.ApplicationKeySpec{
			BucketRef:    &b2v1.LocalBucketReference{Name: "keys"},
			NamePrefix:   "data/",
			Capabilities: []b2v1.Capability{"listFiles", "readFiles", "writeFiles"},
			ValidFor:     &metav1.Duration{Duration: 24 * time.Hour},
			Rotation:     &b2v1.KeyRotation{GracePeriod: &metav1.Duration{Duration: grace}},
		},
	}
	g.Expect(k8s.Create(ctx, key)).To(Succeed())
	waitReady(g, key, keyConds(key))

	// The delivered credentials authorize, with exactly the requested scope.
	secret := &corev1.Secret{}
	g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: tenantNS, Name: "app"}, secret)).To(Succeed())
	auth := authorize(g, secret)
	allowed := auth.APIInfo.StorageAPI.Allowed
	g.Expect(allowed.Buckets).To(HaveLen(1))
	g.Expect(allowed.Buckets[0].ID).To(Equal(b.Status.BucketID))
	g.Expect(allowed.NamePrefix).To(HaveValue(Equal("data/")))
	g.Expect(allowed.Capabilities).To(ContainElements("listFiles", "readFiles", "writeFiles"))
	g.Expect(allowed.Capabilities).NotTo(ContainElement("deleteFiles"))
	g.Expect(auth.ApplicationKeyExpirationTimestamp).NotTo(BeNil(), "validFor must reach B2")
	g.Expect(string(secret.Data[b2v1.SecretKeyAWSEndpointURL])).To(Equal(auth.APIInfo.StorageAPI.S3APIURL))

	// Changing the spec rotates: new key in the Secret, old key valid for
	// the grace period, then revoked.
	oldSecret := secret.DeepCopy()
	oldID := key.Status.KeyID
	update(g, key, func() { key.Spec.Capabilities = append(key.Spec.Capabilities, "deleteFiles") })
	g.Eventually(func(g Gomega) {
		g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(key), key)).To(Succeed())
		g.Expect(key.Status.KeyID).NotTo(Equal(oldID))
	}, timeout, poll).Should(Succeed())
	authorize(g, oldSecret) // still valid during the grace period
	g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: tenantNS, Name: "app"}, secret)).To(Succeed())
	g.Expect(authorize(g, secret).APIInfo.StorageAPI.Allowed.Capabilities).To(ContainElement("deleteFiles"))
	g.Eventually(func() bool { return rejected(oldSecret) }, timeout, poll).Should(BeTrue(), "old key not revoked after the grace period")

	// How B2 answers a delete for a key that no longer exists. The operator
	// treats it as already revoked.
	err := direct.DeleteKey(ctx, oldID)
	t.Logf("b2_delete_key on a revoked key returns: %v", err)
	if err != nil {
		apiErr, ok := b2.AsAPIError(err)
		g.Expect(ok).To(BeTrue(), "unexpected non-API error: %v", err)
		g.Expect(apiErr.Status).To(Equal(400), "the operator expects a 400 for a missing key")
		exists, xerr := direct.KeyExists(ctx, oldID)
		g.Expect(xerr).NotTo(HaveOccurred())
		g.Expect(exists).To(BeFalse())
	}

	// Deleting the resource revokes the key and removes the Secret.
	g.Expect(k8s.Delete(ctx, key)).To(Succeed())
	waitGone(g, key)
	g.Eventually(func() bool { return rejected(secret) }, timeout, poll).Should(BeTrue(), "key still valid after deletion")
}

func TestLiveReplication(t *testing.T) {
	g := requireLive(t)
	ctx := context.Background()
	// Replication in both directions: each bucket is a source and a
	// destination, which B2 stores in one configuration per bucket.
	east := newBucket("repl-east", "repl-east")
	west := newBucket("repl-west", "repl-west")
	west.Spec.Replication = []b2v1.ReplicationRule{{
		Name: "west-to-east", DestinationBucketRef: b2v1.LocalBucketReference{Name: "repl-east"}, FileNamePrefix: "replicated/",
	}}
	east.Spec.Replication = []b2v1.ReplicationRule{{
		Name: "east-to-west", DestinationBucketRef: b2v1.LocalBucketReference{Name: "repl-west"},
	}}
	g.Expect(k8s.Create(ctx, east)).To(Succeed())
	g.Expect(k8s.Create(ctx, west)).To(Succeed())
	t.Cleanup(func() {
		// Rules first, so neither bucket waits on the other.
		for _, b := range []*b2v1.Bucket{west, east} {
			update(NewWithT(t), b, func() { b.Spec.Replication = nil })
		}
		deleteBucket(NewWithT(t), west)
		deleteBucket(NewWithT(t), east)
	})

	// Cloud Replication needs a verified email and payment history; an
	// account without them cannot run this test.
	notEligible := func(c *metav1.Condition) bool {
		return c != nil && (strings.Contains(c.Message, b2.CodeNoPaymentHistory) || strings.Contains(c.Message, b2.CodeEmailNotVerified))
	}
	for _, b := range []*b2v1.Bucket{west, east} {
		var cond *metav1.Condition
		g.Eventually(func(g Gomega) {
			g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(b), b)).To(Succeed())
			cond = meta.FindStatusCondition(b.Status.Conditions, b2v1.ConditionReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status == metav1.ConditionTrue || notEligible(cond)).To(BeTrue(), "%s: reason %s: %s", b.Name, cond.Reason, cond.Message)
		}, timeout, poll).Should(Succeed())
		if notEligible(cond) {
			t.Skipf("account cannot use Cloud Replication: %s", cond.Message)
		}
	}

	replication := func(b *b2v1.Bucket) *b2.ReplicationConfiguration {
		rc := b2Bucket(g, b.Spec.BucketName).ReplicationConfiguration
		g.Expect(rc.IsClientAuthorizedToRead).To(BeTrue())
		if rc.Value == nil {
			return &b2.ReplicationConfiguration{}
		}
		return rc.Value
	}
	ws, es := west.Status.Replication, east.Status.Replication
	g.Expect(ws).NotTo(BeNil())
	g.Expect(es).NotTo(BeNil())
	w, e := replication(west), replication(east)
	g.Expect(w.AsReplicationSource).NotTo(BeNil(), "west lost its rule")
	g.Expect(w.AsReplicationSource.ReplicationRules).To(HaveLen(1))
	g.Expect(w.AsReplicationSource.ReplicationRules[0].DestinationBucketID).To(Equal(east.Status.BucketID))
	g.Expect(w.AsReplicationSource.ReplicationRules[0].FileNamePrefix).To(Equal("replicated/"))
	g.Expect(w.AsReplicationSource.SourceApplicationKeyID).To(HaveValue(Equal(ws.SourceKeyID)))
	g.Expect(w.AsReplicationDestination).NotTo(BeNil(), "west lost the mapping for east's rule")
	g.Expect(w.AsReplicationDestination.SourceToDestinationKeyMapping).To(HaveKeyWithValue(es.SourceKeyID, es.Destinations[0].KeyID))
	g.Expect(e.AsReplicationSource).NotTo(BeNil(), "east lost its rule")
	g.Expect(e.AsReplicationDestination).NotTo(BeNil(), "east lost the mapping for west's rule")
	g.Expect(e.AsReplicationDestination.SourceToDestinationKeyMapping).To(HaveKeyWithValue(ws.SourceKeyID, ws.Destinations[0].KeyID))
	expectStable(g, west)
	expectStable(g, east)

	// Removing one direction keeps the other on both buckets, and revokes
	// that direction's keys.
	westSrc, westDst := ws.SourceKeyID, ws.Destinations[0].KeyID
	update(g, west, func() { west.Spec.Replication = nil })
	waitReady(g, west, bucketConds(west))
	w, e = replication(west), replication(east)
	t.Logf("after removing west's rule: west=%s east=%s", describe(w), describe(e))
	g.Expect(w.AsReplicationSource).To(BeNil())
	g.Expect(w.AsReplicationDestination).NotTo(BeNil(), "west must still receive from east")
	g.Expect(e.AsReplicationSource).NotTo(BeNil(), "east must still replicate to west")
	g.Expect(e.AsReplicationDestination).To(BeNil())
	for _, id := range []string{westSrc, westDst} {
		exists, err := direct.KeyExists(ctx, id)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(exists).To(BeFalse(), "replication key %s not revoked", id)
	}
	expectStable(g, west)
	expectStable(g, east)
}

func describe(rc *b2.ReplicationConfiguration) string {
	out := "none"
	if rc.AsReplicationSource != nil {
		out = fmt.Sprintf("source(%d rules)", len(rc.AsReplicationSource.ReplicationRules))
	}
	if rc.AsReplicationDestination != nil {
		out += fmt.Sprintf(" destination(%d mappings)", len(rc.AsReplicationDestination.SourceToDestinationKeyMapping))
	}
	return out
}

// TestLivePartnerAccount creates a real B2 account in a Partner API Group.
// Accounts cannot be deleted, so it only runs when explicitly enabled.
func TestLivePartnerAccount(t *testing.T) {
	g := requireLive(t)
	if env("B2_LIVE_PARTNER_CREATE_ACCOUNTS") != "yes" {
		t.Skip("set B2_LIVE_PARTNER_CREATE_ACCOUNTS=yes (and the other B2_LIVE_PARTNER_* variables) to create a real Partner API account")
	}
	for _, v := range []string{"B2_LIVE_PARTNER_KEY_ID", "B2_LIVE_PARTNER_KEY", "B2_LIVE_PARTNER_GROUP_ID", "B2_LIVE_PARTNER_EMAIL_DOMAIN"} {
		if env(v) == "" {
			t.Fatalf("%s is required for the Partner API test", v)
		}
	}
	ctx := context.Background()
	region := b2v1.Region(env("B2_LIVE_PARTNER_REGION"))
	if region == "" {
		region = "us-west"
	}
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: "b2-partner-admin"},
		StringData: map[string]string{"applicationKeyId": env("B2_LIVE_PARTNER_KEY_ID"), "applicationKey": env("B2_LIVE_PARTNER_KEY")},
	})).To(Succeed())
	partner := &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "partner"},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               env("B2_LIVE_API_URL"),
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "b2-partner-admin"},
			Partner: &b2v1.PartnerSettings{
				GroupID:             env("B2_LIVE_PARTNER_GROUP_ID"),
				MemberEmailTemplate: "{customer}-{region}@" + env("B2_LIVE_PARTNER_EMAIL_DOMAIN"),
			},
		},
	}
	g.Expect(k8s.Create(ctx, partner)).To(Succeed())
	waitReady(g, partner, func() []metav1.Condition { return partner.Status.Conditions })
	g.Expect(partner.Status.KeyType).To(Equal(b2v1.KeyTypeMaster))

	deletion := b2v1.AccountDeletionPolicyRetain
	if env("B2_LIVE_PARTNER_EJECT") == "yes" {
		deletion = b2v1.AccountDeletionPolicyEject
	}
	acct := &b2v1.B2Account{
		ObjectMeta: metav1.ObjectMeta{Name: runID},
		Spec: b2v1.B2AccountSpec{
			PartnerConfigRef:     b2v1.ProviderConfigReference{Name: "partner"},
			Customer:             runID,
			Region:               region,
			CredentialsSecretRef: b2v1.AccountSecretReference{Namespace: operatorNS},
			DeletionPolicy:       deletion,
		},
	}
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	waitReady(g, acct, func() []metav1.Condition { return acct.Status.Conditions })
	t.Logf("created account %s (%s) in %s; it remains in the Group or, with eject, as an independent account", acct.Status.AccountID, acct.Status.Email, region)

	admin := b2.New(b2.Options{BaseURL: env("B2_LIVE_API_URL"), ApplicationKeyID: env("B2_LIVE_PARTNER_KEY_ID"), ApplicationKey: env("B2_LIVE_PARTNER_KEY")})
	member, err := admin.FindGroupMember(ctx, env("B2_LIVE_PARTNER_GROUP_ID"), acct.Status.Email)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(member).NotTo(BeNil())
	g.Expect(member.AccountID).To(Equal(acct.Status.AccountID))

	// What b2_create_group_member returned, which the docs do not specify.
	creds := &corev1.Secret{}
	g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: operatorNS, Name: acct.SecretNameOrDefault()}, creds)).To(Succeed())
	returned := string(creds.Data[b2v1.AccountSecretKeyID])
	t.Logf("b2_create_group_member returned the account's master key: %v (key ID %s, account %s)", b2.IsMasterKey(returned, acct.Status.AccountID), returned, acct.Status.AccountID)

	// The account's provider config runs on the operator's application key.
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: acct.ProviderConfigNameOrDefault()}}
	waitReady(g, pc, func() []metav1.Condition { return pc.Status.Conditions })
	g.Expect(pc.Status.KeyType).To(Equal(b2v1.KeyTypeApplication))
	g.Expect(pc.Status.AccountID).To(Equal(acct.Status.AccountID))

	// A bucket lands in the customer's account.
	b := newBucket("partner-bucket", "partner")
	b.Spec.ProviderConfigRef.Name = pc.Name
	g.Expect(k8s.Create(ctx, b)).To(Succeed())
	waitReady(g, b, bucketConds(b))
	ops := b2.New(b2.Options{BaseURL: env("B2_LIVE_API_URL"),
		ApplicationKeyID: string(creds.Data[b2v1.AccountSecretOperationsKeyID]), ApplicationKey: string(creds.Data[b2v1.AccountSecretOperationsKey])})
	inMember, err := ops.GetBucketByName(ctx, b.Spec.BucketName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(inMember).NotTo(BeNil(), "bucket not found in the customer account")

	g.Expect(k8s.Delete(ctx, b)).To(Succeed())
	waitGone(g, b)
	g.Expect(k8s.Delete(ctx, acct)).To(Succeed())
	waitGone(g, acct)
	if deletion == b2v1.AccountDeletionPolicyEject {
		member, err := admin.FindGroupMember(ctx, env("B2_LIVE_PARTNER_GROUP_ID"), acct.Status.Email)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(member).To(BeNil(), "account still in the Group after eject")
	}
}

// authorize authorizes with the credentials in a key Secret.
func authorize(g *WithT, s *corev1.Secret) *b2.Authorization {
	auth, err := keyClient(s).Authorize(context.Background())
	g.Expect(err).NotTo(HaveOccurred())
	return auth
}

// rejected reports whether B2 refuses the credentials in a key Secret.
func rejected(s *corev1.Secret) bool {
	_, err := keyClient(s).Authorize(context.Background())
	var credErr *b2.CredentialsError
	return errors.As(err, &credErr)
}

func keyClient(s *corev1.Secret) *b2.Client {
	return b2.New(b2.Options{
		BaseURL:          env("B2_LIVE_API_URL"),
		ApplicationKeyID: string(s.Data[b2v1.SecretKeyB2KeyID]),
		ApplicationKey:   string(s.Data[b2v1.SecretKeyB2Key]),
	})
}
