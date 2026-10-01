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
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

// uniqueCustomer makes customer IDs (and so emails and names) unique per
// test run, since runs share one API server and fake B2.
func uniqueCustomer(base string) string {
	b := make([]byte, 2)
	_, _ = rand.Read(b)
	return base + hex.EncodeToString(b)
}

func accountConds(a *b2v1.B2Account) func() []metav1.Condition {
	return func() []metav1.Condition { return a.Status.Conditions }
}

// newAccount returns a B2Account that grants namespaces labelled
// customer=<customer> use of it.
func newAccount(name, customer string, region b2v1.Region) *b2v1.B2Account {
	return &b2v1.B2Account{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: b2v1.B2AccountSpec{
			PartnerConfigRef:     b2v1.ProviderConfigReference{Name: partnerConfig},
			Customer:             customer,
			Region:               region,
			CredentialsSecretRef: b2v1.AccountSecretReference{Namespace: operatorNS},
			Access: &b2v1.AccountAccess{
				NamespaceSelector: metav1.LabelSelector{MatchLabels: map[string]string{"customer": customer}},
				Buckets:           b2v1.BucketPolicy{NamePatterns: []string{"{namespace}-*"}, AllowReplication: true, AllowDeletion: true},
				Keys:              b2v1.KeyPolicy{AllowedCapabilities: []b2v1.Capability{"listFiles", "readFiles", "writeFiles"}},
			},
		},
	}
}

func TestB2AccountProvisionsAccountAndStoresKey(t *testing.T) {
	g := requireEnv(t)
	c1001 := uniqueCustomer("cust1001")
	ctx := context.Background()
	acct := newAccount(c1001+"-eu", c1001, "eu-central")
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, accountConds(acct), b2v1.ReasonReconciled)

	email := c1001 + "-eu-central@hosting.example.com"
	g.Expect(acct.Status.Email).To(Equal(email))
	g.Expect(acct.Status.AccountID).To(Equal(fakeB2.AccountByEmail(email)))
	g.Expect(fakeB2.GroupMembers(groupID)).To(ContainElement(acct.Status.AccountID))
	g.Expect(acct.Status.S3Endpoint).To(ContainSubstring("eu-central"))

	// The key B2 returned once is stored, and it works.
	s := getSecret(g, operatorNS, "b2-account-"+c1001+"-eu")
	g.Expect(string(s.Data[b2v1.AccountSecretAccountID])).To(Equal(acct.Status.AccountID))
	g.Expect(string(s.Data[b2v1.AccountSecretEmail])).To(Equal(email))
	g.Expect(s.OwnerReferences).To(BeEmpty(), "the credentials Secret must not be garbage-collected with the resource")
	kc := b2.New(b2.Options{BaseURL: fakeB2.URL(), ApplicationKeyID: string(s.Data[b2v1.AccountSecretKeyID]), ApplicationKey: string(s.Data[b2v1.AccountSecretKey])})
	auth, err := kc.Authorize(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(auth.AccountID).To(Equal(acct.Status.AccountID))

	// A provider config and access policy are published for the account.
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: c1001 + "-eu"}}
	eventuallyReason(g, pc, func() []metav1.Condition { return pc.Status.Conditions }, b2v1.ReasonReconciled)
	g.Expect(pc.Status.AccountID).To(Equal(acct.Status.AccountID))
	g.Expect(pc.Status.S3Region).To(Equal("eu-central-003"))
	var policy b2v1.B2AccessPolicy
	g.Expect(k8s.Get(ctx, client.ObjectKey{Name: "b2account-" + c1001 + "-eu"}, &policy)).To(Succeed())
	g.Expect(policy.Spec.ProviderConfigs).To(ConsistOf(c1001 + "-eu"))

	// The customer's namespace creates a bucket in its own account,
	// encrypted by default.
	ns := createNamespace(t, c1001+"-web", map[string]string{"customer": c1001})
	bkt := newBucket(ns, "assets", ns+"-assets")
	bkt.Spec.ProviderConfigRef.Name = c1001 + "-eu"
	g.Expect(k8s.Create(ctx, bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonReconciled)
	g.Expect(fakeB2.AccountOfBucket(bkt.Spec.BucketName)).To(Equal(acct.Status.AccountID))
	g.Expect(bkt.Spec.DefaultEncryption.Mode).To(Equal(b2v1.EncryptionModeSSEB2))
	g.Expect(*fakeB2.Bucket(bkt.Spec.BucketName).DefaultServerSideEncryption.Value.Mode).To(Equal(b2.SSEModeB2))

	// The account cannot be deleted while it is in use.
	g.Expect(k8s.Delete(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, accountConds(acct), b2v1.ReasonDeletionBlocked)

	// Retain: the account stays in the Group and its key stays stored.
	g.Expect(k8s.Delete(ctx, bkt)).To(Succeed())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(acct), &b2v1.B2Account{}))
	}, timeout, poll).Should(BeTrue())
	g.Expect(fakeB2.GroupMembers(groupID)).To(ContainElement(acct.Status.AccountID))
	getSecret(g, operatorNS, "b2-account-"+c1001+"-eu")
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKey{Name: c1001 + "-eu"}, &b2v1.ClusterProviderConfig{}))
	}, timeout, poll).Should(BeTrue())
}

func TestB2AccountEjectKeepsCredentials(t *testing.T) {
	g := requireEnv(t)
	c1002 := uniqueCustomer("cust1002")
	ctx := context.Background()
	acct := newAccount(c1002+"-us", c1002, "us-east")
	acct.Spec.DeletionPolicy = b2v1.AccountDeletionPolicyEject
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, accountConds(acct), b2v1.ReasonReconciled)
	id := acct.Status.AccountID

	g.Expect(k8s.Delete(ctx, acct)).To(Succeed())
	g.Eventually(func() []string { return fakeB2.GroupMembers(groupID) }, timeout, poll).ShouldNot(ContainElement(id))
	// The Secret is annotated just after the eject call returns.
	g.Eventually(func(g Gomega) {
		s := getSecret(g, operatorNS, "b2-account-"+c1002+"-us")
		g.Expect(s.Annotations).To(HaveKey(annotationEjectedAt))
		g.Expect(s.Data[b2v1.AccountSecretKey]).NotTo(BeEmpty(), "ejecting must not discard the stored key")
	}, timeout, poll).Should(Succeed())
}

func TestB2AccountSameCustomerAndRegionConflicts(t *testing.T) {
	g := requireEnv(t)
	c1003 := uniqueCustomer("cust1003")
	ctx := context.Background()
	first := newAccount(c1003+"-a", c1003, "us-west")
	g.Expect(k8s.Create(ctx, first)).To(Succeed())
	eventuallyReason(g, first, accountConds(first), b2v1.ReasonReconciled)

	dup := newAccount(c1003+"-b", c1003, "us-west")
	g.Expect(k8s.Create(ctx, dup)).To(Succeed())
	eventuallyReason(g, dup, accountConds(dup), b2v1.ReasonAccountConflict)
	g.Expect(dup.Status.AccountID).To(BeEmpty())

	// Same customer in another region is a separate account.
	other := newAccount(c1003+"-ca", c1003, "ca-east")
	g.Expect(k8s.Create(ctx, other)).To(Succeed())
	eventuallyReason(g, other, accountConds(other), b2v1.ReasonReconciled)
	g.Expect(other.Status.Email).To(Equal(c1003 + "-ca-east@hosting.example.com"))
	g.Expect(other.Status.AccountID).NotTo(Equal(first.Status.AccountID))
}

func TestB2AccountRefusesToOverwriteForeignCredentials(t *testing.T) {
	g := requireEnv(t)
	c1004 := uniqueCustomer("cust1004")
	ctx := context.Background()
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: "b2-account-" + c1004},
		StringData: map[string]string{b2v1.AccountSecretKeyID: "someone-elses", b2v1.AccountSecretKey: "key"},
	})).To(Succeed())
	acct := newAccount(c1004, c1004, "us-west")
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, accountConds(acct), b2v1.ReasonAccountConflict)
	g.Expect(fakeB2.AccountByEmail(c1004+"-us-west@hosting.example.com")).To(BeEmpty(), "no account may be created when its key could not be stored")
}

func TestB2AccountAdoptsExistingMember(t *testing.T) {
	g := requireEnv(t)
	c1005 := uniqueCustomer("cust1005")
	ctx := context.Background()
	admin := b2.New(b2.Options{BaseURL: fakeB2.URL(), ApplicationKeyID: fakeB2.MasterKeyID, ApplicationKey: fakeB2.MasterKey})
	created, err := admin.CreateGroupMember(ctx, groupID, c1005+"-us-west@hosting.example.com", "us-west")
	g.Expect(err).NotTo(HaveOccurred())

	acct := newAccount(c1005, c1005, "us-west")
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, accountConds(acct), b2v1.ReasonCredentialsMissing)

	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: "b2-account-" + c1005},
		StringData: map[string]string{b2v1.AccountSecretKeyID: created.ApplicationKeyID, b2v1.AccountSecretKey: created.ApplicationKey},
	})).To(Succeed())
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(acct), acct)).To(Succeed())
	acct.Spec.AdoptExisting = true
	g.Expect(k8s.Update(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, accountConds(acct), b2v1.ReasonReconciled)
	g.Expect(acct.Status.AccountID).To(Equal(created.GroupMember.AccountID))
}

func TestPartnerAPINotEnabled(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	name := uniqueCustomer("not-partner-")
	// Only master keys of Group admins get the Groups API in the fake.
	id, secret := fakeB2.AddKey("not-partner", []string{"listBuckets", "writeKeys"}, nil, "")
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: name},
		StringData: map[string]string{"applicationKeyId": id, "applicationKey": secret},
	})).To(Succeed())
	pc := &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               fakeB2.URL(),
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: name},
			Partner:              &b2v1.PartnerSettings{GroupID: groupID, MemberEmailTemplate: "{customer}-{region}@hosting.example.com"},
		},
	}
	g.Expect(k8s.Create(ctx, pc)).To(Succeed())
	eventuallyReason(g, pc, func() []metav1.Condition { return pc.Status.Conditions }, b2v1.ReasonPartnerAPINotEnabled)
	g.Expect(readyCondition(g, pc, func() []metav1.Condition { return pc.Status.Conditions }).Message).To(ContainSubstring("sales"))
}

func TestPartnerEmailTemplateValidation(t *testing.T) {
	g := requireEnv(t)
	for _, tpl := range []string{"{customer}@hosting.example.com", "{customer}-{region}", "{customer}-{region}@localhost"} {
		pc := &b2v1.ClusterProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-template"},
			Spec: b2v1.ClusterProviderConfigSpec{
				CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "x"},
				Partner:              &b2v1.PartnerSettings{GroupID: "g", MemberEmailTemplate: tpl},
			},
		}
		err := k8s.Create(context.Background(), pc)
		g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "template %q: err = %v", tpl, err)
	}
}

func TestCrossAccountReplication(t *testing.T) {
	g := requireEnv(t)
	c2002 := uniqueCustomer("cust2002")
	ctx := context.Background()
	primary := newAccount(c2002+"-west", c2002, "us-west")
	replica := newAccount(c2002+"-eu", c2002, "eu-central")
	for _, a := range []*b2v1.B2Account{primary, replica} {
		g.Expect(k8s.Create(ctx, a)).To(Succeed())
	}
	for _, a := range []*b2v1.B2Account{primary, replica} {
		eventuallyReason(g, a, accountConds(a), b2v1.ReasonReconciled)
	}
	pcReady := func(name string) {
		pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: name}}
		eventuallyReason(g, pc, func() []metav1.Condition { return pc.Status.Conditions }, b2v1.ReasonReconciled)
	}
	pcReady(c2002 + "-west")
	pcReady(c2002 + "-eu")

	ns := createNamespace(t, c2002+"-app", map[string]string{"customer": c2002})
	dst := newBucket(ns, "backup", ns+"-backup")
	dst.Spec.ProviderConfigRef.Name = c2002 + "-eu"
	dst.Spec.ObjectLock = &b2v1.ObjectLock{Enabled: true, DefaultRetention: &b2v1.DefaultRetention{Mode: b2v1.RetentionModeGovernance, Duration: 30, Unit: "days"}}
	src := newBucket(ns, "data", ns+"-data")
	src.Spec.ProviderConfigRef.Name = c2002 + "-west"
	src.Spec.LifecycleRules = []b2v1.LifecycleRule{{DaysFromHidingToDeleting: ptr[int32](30)}}
	src.Spec.Replication = []b2v1.ReplicationRule{{Name: "to-eu", DestinationBucketRef: b2v1.LocalBucketReference{Name: "backup"}, IncludeExistingFiles: true}}
	g.Expect(k8s.Create(ctx, src)).To(Succeed())
	g.Expect(k8s.Create(ctx, dst)).To(Succeed())

	eventuallyReason(g, dst, bucketConds(dst), b2v1.ReasonReconciled)
	eventuallyReason(g, src, bucketConds(src), b2v1.ReasonReconciled)
	g.Expect(fakeB2.AccountOfBucket(src.Spec.BucketName)).To(Equal(primary.Status.AccountID))
	g.Expect(fakeB2.AccountOfBucket(dst.Spec.BucketName)).To(Equal(replica.Status.AccountID))

	rs := src.Status.Replication
	g.Expect(rs).NotTo(BeNil())
	g.Expect(rs.Destinations).To(HaveLen(1))
	srcRC := fakeB2.Bucket(src.Spec.BucketName).ReplicationConfiguration.Value.AsReplicationSource
	g.Expect(srcRC.ReplicationRules).To(HaveLen(1))
	g.Expect(srcRC.ReplicationRules[0].DestinationBucketID).To(Equal(dst.Status.BucketID))
	g.Expect(srcRC.ReplicationRules[0].IncludeExistingFiles).To(BeTrue())
	g.Expect(*srcRC.SourceApplicationKeyID).To(Equal(rs.SourceKeyID))
	dstRC := fakeB2.Bucket(dst.Spec.BucketName).ReplicationConfiguration.Value.AsReplicationDestination
	g.Expect(dstRC.SourceToDestinationKeyMapping).To(HaveKeyWithValue(rs.SourceKeyID, rs.Destinations[0].KeyID))
	g.Expect(fakeB2.Key(rs.SourceKeyID).Capabilities).To(ConsistOf(sourceKeyCapabilities))
	g.Expect(fakeB2.Key(rs.Destinations[0].KeyID).Capabilities).To(ConsistOf(destinationKeyCapabilities))
	g.Expect(fakeB2.Key(rs.Destinations[0].KeyID).AccountID).To(Equal(replica.Status.AccountID))

	// The destination cannot be deleted while it is replicated into.
	g.Expect(k8s.Delete(ctx, dst)).To(Succeed())
	eventuallyReason(g, dst, bucketConds(dst), b2v1.ReasonDeletionBlocked)

	// Removing the rule unmaps and revokes both keys; the destination can go.
	srcKey, dstKey := rs.SourceKeyID, rs.Destinations[0].KeyID
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(src), src)).To(Succeed())
	src.Spec.Replication = nil
	g.Expect(k8s.Update(ctx, src)).To(Succeed())
	g.Eventually(func() *b2.ApplicationKey { return fakeB2.Key(srcKey) }, timeout, poll).Should(BeNil())
	g.Eventually(func() *b2.ApplicationKey { return fakeB2.Key(dstKey) }, timeout, poll).Should(BeNil())
	g.Expect(fakeB2.Bucket(src.Spec.BucketName).ReplicationConfiguration.Value.AsReplicationSource).To(BeNil())
	g.Eventually(func() bool {
		return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(dst), &b2v1.Bucket{}))
	}, timeout, poll).Should(BeTrue())
}

func TestReplicationNeedsPolicy(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true) // tenant policy does not allow replication
	readyBucket(g, ns, "b")
	src := newBucket(ns, "a", ns+"-a")
	src.Spec.Replication = []b2v1.ReplicationRule{{Name: "r", DestinationBucketRef: b2v1.LocalBucketReference{Name: "b"}}}
	g.Expect(k8s.Create(ctx, src)).To(Succeed())
	eventuallyReason(g, src, bucketConds(src), b2v1.ReasonPolicyDenied)
}

func TestUnencryptedBucketNeedsPolicy(t *testing.T) {
	g := requireEnv(t)
	ns := newNamespace(t, true)
	bkt := newBucket(ns, "plain", ns+"-plain")
	bkt.Spec.DefaultEncryption = &b2v1.DefaultEncryption{Mode: b2v1.EncryptionModeNone}
	g.Expect(k8s.Create(context.Background(), bkt)).To(Succeed())
	eventuallyReason(g, bkt, bucketConds(bkt), b2v1.ReasonPolicyDenied)
	g.Expect(fakeB2.Bucket(ns + "-plain")).To(BeNil())
}
