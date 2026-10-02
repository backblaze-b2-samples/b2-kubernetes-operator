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
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

// The tests in this file create real B2 accounts in a Partner API Group.
// Accounts cannot be deleted, so they only run when explicitly enabled.

const partnerConfigName = "partner"

// requirePartner skips unless Partner tests are enabled, and makes sure the
// Group admin's provider config exists and is ready.
func requirePartner(t *testing.T) *WithT {
	t.Helper()
	g := requireLive(t)
	if env("B2_LIVE_PARTNER_CREATE_ACCOUNTS") != "yes" {
		t.Skip("set B2_LIVE_PARTNER_CREATE_ACCOUNTS=yes (and the other B2_LIVE_PARTNER_* variables) to create real Partner API accounts")
	}
	for _, v := range []string{"B2_LIVE_PARTNER_KEY_ID", "B2_LIVE_PARTNER_KEY", "B2_LIVE_PARTNER_GROUP_ID", "B2_LIVE_PARTNER_EMAIL_DOMAIN"} {
		if env(v) == "" {
			t.Fatalf("%s is required for the Partner API tests", v)
		}
	}
	ctx := context.Background()
	for _, o := range []client.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: "b2-partner-admin"},
			StringData: map[string]string{"applicationKeyId": env("B2_LIVE_PARTNER_KEY_ID"), "applicationKey": env("B2_LIVE_PARTNER_KEY")},
		},
		&b2v1.ClusterProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: partnerConfigName},
			Spec: b2v1.ClusterProviderConfigSpec{
				APIURL:               env("B2_LIVE_API_URL"),
				CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "b2-partner-admin"},
				Partner: &b2v1.PartnerSettings{
					GroupID:             env("B2_LIVE_PARTNER_GROUP_ID"),
					MemberEmailTemplate: "{customer}-{region}@" + env("B2_LIVE_PARTNER_EMAIL_DOMAIN"),
				},
			},
		},
	} {
		if err := k8s.Create(ctx, o); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating %s: %v", o.GetName(), err)
		}
	}
	partner := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: partnerConfigName}}
	waitReady(g, partner, func() []metav1.Condition { return partner.Status.Conditions })
	g.Expect(partner.Status.KeyType).To(Equal(b2v1.KeyTypeMaster))
	return g
}

// partnerAccount is a B2Account the test created, with clients for it.
type partnerAccount struct {
	*b2v1.B2Account
	providerConfig string
	creds          *corev1.Secret
	ops            *b2.Client // the operator's operations key
	files          *files     // the key B2 returned at creation
}

// newPartnerAccount creates a B2Account and waits until its provider config
// is ready to use.
func newPartnerAccount(t *testing.T, g *WithT, customer string, region b2v1.Region, deletion b2v1.AccountDeletionPolicy) *partnerAccount {
	t.Helper()
	ctx := context.Background()
	acct := &b2v1.B2Account{
		ObjectMeta: metav1.ObjectMeta{Name: customer + "-" + string(region)},
		Spec: b2v1.B2AccountSpec{
			PartnerConfigRef:     b2v1.ProviderConfigReference{Name: partnerConfigName},
			Customer:             customer,
			Region:               region,
			CredentialsSecretRef: b2v1.AccountSecretReference{Namespace: operatorNS},
			DeletionPolicy:       deletion,
		},
	}
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	waitReady(g, acct, func() []metav1.Condition { return acct.Status.Conditions })
	t.Logf("created account %s (%s) in %s", acct.Status.AccountID, acct.Status.Email, region)

	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: acct.ProviderConfigNameOrDefault()}}
	waitReady(g, pc, func() []metav1.Condition { return pc.Status.Conditions })
	g.Expect(pc.Status.KeyType).To(Equal(b2v1.KeyTypeApplication))
	g.Expect(pc.Status.AccountID).To(Equal(acct.Status.AccountID))

	creds := &corev1.Secret{}
	g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: operatorNS, Name: acct.SecretNameOrDefault()}, creds)).To(Succeed())
	fs, err := newFiles(ctx, string(creds.Data[b2v1.AccountSecretKeyID]), string(creds.Data[b2v1.AccountSecretKey]))
	g.Expect(err).NotTo(HaveOccurred())
	return &partnerAccount{
		B2Account:      acct,
		providerConfig: pc.Name,
		creds:          creds,
		ops:            accountClient(creds, b2v1.AccountSecretOperationsKeyID, b2v1.AccountSecretOperationsKey),
		files:          fs,
	}
}

func accountClient(creds *corev1.Secret, idField, keyField string) *b2.Client {
	return b2.New(b2.Options{BaseURL: env("B2_LIVE_API_URL"), ApplicationKeyID: string(creds.Data[idField]), ApplicationKey: string(creds.Data[keyField])})
}

func groupAdmin() *b2.Client {
	return b2.New(b2.Options{BaseURL: env("B2_LIVE_API_URL"), ApplicationKeyID: env("B2_LIVE_PARTNER_KEY_ID"), ApplicationKey: env("B2_LIVE_PARTNER_KEY")})
}

// bucketIn reads a bucket from a customer account.
func (a *partnerAccount) bucketIn(g *WithT, name string) *b2.Bucket {
	b, err := a.ops.GetBucketByName(context.Background(), name)
	g.Expect(err).NotTo(HaveOccurred())
	return b
}

// TestLivePartnerAccount creates an account, puts a bucket in it, and then
// either leaves it in the Group or, with B2_LIVE_PARTNER_EJECT=yes, ejects it.
func TestLivePartnerAccount(t *testing.T) {
	g := requirePartner(t)
	ctx := context.Background()
	region := b2v1.Region(env("B2_LIVE_PARTNER_REGION"))
	if region == "" {
		region = "us-west"
	}
	deletion := b2v1.AccountDeletionPolicyRetain
	if env("B2_LIVE_PARTNER_EJECT") == "yes" {
		deletion = b2v1.AccountDeletionPolicyEject
	}
	acct := newPartnerAccount(t, g, runID, region, deletion)

	groupID := env("B2_LIVE_PARTNER_GROUP_ID")
	member, err := groupAdmin().FindGroupMember(ctx, groupID, acct.Status.Email)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(member).NotTo(BeNil())
	g.Expect(member.AccountID).To(Equal(acct.Status.AccountID))

	returned := string(acct.creds.Data[b2v1.AccountSecretKeyID])
	t.Logf("b2_create_group_member returned the account's master key: %v (key ID %s, account %s)", b2.IsMasterKey(returned, acct.Status.AccountID), returned, acct.Status.AccountID)

	b := newBucket("partner-bucket", "partner")
	b.Spec.ProviderConfigRef.Name = acct.providerConfig
	g.Expect(k8s.Create(ctx, b)).To(Succeed())
	waitReady(g, b, bucketConds(b))
	g.Expect(acct.bucketIn(g, b.Spec.BucketName)).NotTo(BeNil(), "bucket not found in the customer account")

	g.Expect(k8s.Delete(ctx, b)).To(Succeed())
	waitGone(g, b)
	g.Expect(acct.bucketIn(g, b.Spec.BucketName)).To(BeNil(), "bucket still in the customer account")

	opsKeyID := string(acct.creds.Data[b2v1.AccountSecretOperationsKeyID])
	g.Expect(k8s.Delete(ctx, acct.B2Account)).To(Succeed())
	waitGone(g, acct.B2Account)
	if deletion != b2v1.AccountDeletionPolicyEject {
		t.Logf("account %s stays in the Group", acct.Status.AccountID)
		return
	}

	// Eject: the account leaves the Group and is the customer's alone. The
	// operator revokes its operations key and keeps the account key, which
	// is the only way back into the account.
	member, err = groupAdmin().FindGroupMember(ctx, groupID, acct.Status.Email)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(member).To(BeNil(), "account still in the Group after eject")

	creds := &corev1.Secret{}
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(acct.creds), creds)).To(Succeed())
	g.Expect(creds.Annotations).To(HaveKey("b2.backblaze.com/ejected-at"))
	g.Expect(creds.Data).NotTo(HaveKey(b2v1.AccountSecretOperationsKeyID))
	g.Expect(creds.Data[b2v1.AccountSecretKeyID]).To(Equal(acct.creds.Data[b2v1.AccountSecretKeyID]))

	accountKey := accountClient(creds, b2v1.AccountSecretKeyID, b2v1.AccountSecretKey)
	auth, err := accountKey.Authorize(ctx)
	g.Expect(err).NotTo(HaveOccurred(), "the stored account key no longer works after eject")
	g.Expect(auth.AccountID).To(Equal(acct.Status.AccountID))
	exists, err := accountKey.KeyExists(ctx, opsKeyID)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(exists).To(BeFalse(), "operations key %s not revoked on eject", opsKeyID)
	t.Logf("ejected account %s; it is now independent of the Group", acct.Status.AccountID)
}

// TestLivePartnerMultiRegion is the "multi-region bucket" a hosting provider
// offers its customers: an account per region, a bucket in each, and Cloud
// Replication from one to the other. A file written in one region must
// arrive in the other.
func TestLivePartnerMultiRegion(t *testing.T) {
	g := requirePartner(t)
	ctx := context.Background()
	customer := runID + "-mr"
	west := newPartnerAccount(t, g, customer, "us-west", b2v1.AccountDeletionPolicyRetain)
	east := newPartnerAccount(t, g, customer, "us-east", b2v1.AccountDeletionPolicyRetain)

	dst := newBucket("mr-east", "mr-east")
	dst.Spec.ProviderConfigRef.Name = east.providerConfig
	src := newBucket("mr-west", "mr-west")
	src.Spec.ProviderConfigRef.Name = west.providerConfig
	src.Spec.Replication = []b2v1.ReplicationRule{{
		Name: "west-to-east", DestinationBucketRef: b2v1.LocalBucketReference{Name: dst.Name},
	}}
	g.Expect(k8s.Create(ctx, dst)).To(Succeed())
	g.Expect(k8s.Create(ctx, src)).To(Succeed())
	t.Cleanup(func() {
		g := NewWithT(t)
		update(g, src, func() { src.Spec.Replication = nil })
		waitReady(g, src, bucketConds(src))
		for _, p := range []struct {
			acct *partnerAccount
			b    *b2v1.Bucket
		}{{west, src}, {east, dst}} {
			if p.b.Status.BucketID != "" {
				if err := p.acct.files.empty(ctx, p.b.Status.BucketID); err != nil {
					t.Errorf("emptying %s: %v", p.b.Spec.BucketName, err)
				}
			}
			g.Expect(client.IgnoreNotFound(k8s.Delete(ctx, p.b))).To(Succeed())
			waitGone(g, p.b)
			g.Expect(p.acct.bucketIn(g, p.b.Spec.BucketName)).To(BeNil(), "bucket %s still exists", p.b.Spec.BucketName)
		}
		for _, a := range []*partnerAccount{west, east} {
			g.Expect(k8s.Delete(ctx, a.B2Account)).To(Succeed())
			waitGone(g, a.B2Account)
		}
		t.Logf("accounts %s and %s stay in the Group", west.Status.AccountID, east.Status.AccountID)
	})
	waitReplicating(t, g, src, dst)

	// The rule is in the west account and the key mapping in the east one,
	// each through that account's own operations key.
	rs := src.Status.Replication
	g.Expect(rs).NotTo(BeNil())
	g.Expect(rs.Destinations).To(HaveLen(1))
	s := west.bucketIn(g, src.Spec.BucketName).ReplicationConfiguration.Value
	g.Expect(s).NotTo(BeNil())
	g.Expect(s.AsReplicationSource).NotTo(BeNil())
	g.Expect(s.AsReplicationSource.ReplicationRules).To(HaveLen(1))
	g.Expect(s.AsReplicationSource.ReplicationRules[0].DestinationBucketID).To(Equal(dst.Status.BucketID))
	d := east.bucketIn(g, dst.Spec.BucketName).ReplicationConfiguration.Value
	g.Expect(d).NotTo(BeNil())
	g.Expect(d.AsReplicationDestination).NotTo(BeNil())
	g.Expect(d.AsReplicationDestination.SourceToDestinationKeyMapping).To(HaveKeyWithValue(rs.SourceKeyID, rs.Destinations[0].KeyID))
	expectStableIn(g, west, src)
	expectStableIn(g, east, dst)

	// A file written in us-west shows up in us-east.
	const name = "hello/multi-region.txt"
	g.Expect(west.files.upload(ctx, src.Status.BucketID, name, []byte("written in us-west\n"))).To(Succeed())
	start := time.Now()
	wait := 15 * time.Minute
	if v, err := time.ParseDuration(env("B2_LIVE_REPLICATION_WAIT")); err == nil {
		wait = v
	}
	g.Eventually(func(g Gomega) {
		vs, err := east.files.versions(ctx, dst.Status.BucketID)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(vs).To(ContainElement(HaveField("FileName", name)))
	}, wait, 10*time.Second).Should(Succeed(), "file not replicated to us-east within %s", wait)
	t.Logf("file replicated from us-west to us-east in %s", time.Since(start).Round(time.Second))

	g.Eventually(func(g Gomega) {
		vs, err := west.files.versions(ctx, src.Status.BucketID)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(vs).To(ContainElement(And(HaveField("FileName", name), HaveField("ReplicationStatus", HaveValue(Equal("completed"))))))
	}, 2*time.Minute, 5*time.Second).Should(Succeed(), "source file never reported replicationStatus completed")
}

// expectStableIn is expectStable for a bucket in a customer account.
func expectStableIn(g *WithT, a *partnerAccount, b *b2v1.Bucket) {
	rev := a.bucketIn(g, b.Spec.BucketName).Revision
	resync(g, b)
	resync(g, b)
	g.Expect(a.bucketIn(g, b.Spec.BucketName).Revision).To(Equal(rev),
		"the operator keeps updating bucket %s: its desired state does not match how B2 reports it", b.Spec.BucketName)
}
