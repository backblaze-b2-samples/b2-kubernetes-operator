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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

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
		eventuallyReason(g, a, b2v1.ReasonReconciled)
	}
	pcReady := func(name string) {
		pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: name}}
		eventuallyReason(g, pc, b2v1.ReasonReconciled)
	}
	pcReady(c2002 + "-west")
	pcReady(c2002 + "-eu")

	ns := createNamespace(t, c2002+"-app", map[string]string{"customer": c2002})
	dst := newBucket(ns, "backup", ns+"-backup")
	dst.Spec.ProviderConfigRef.Name = c2002 + "-eu"
	dst.Spec.ObjectLock = &b2v1.ObjectLock{Enabled: true, DefaultRetention: &b2v1.DefaultRetention{Mode: b2v1.RetentionModeGovernance, Duration: 30, Unit: "days"}}
	src := newBucket(ns, "data", ns+"-data")
	src.Spec.ProviderConfigRef.Name = c2002 + "-west"
	src.Spec.LifecycleRules = []b2v1.LifecycleRule{{DaysFromHidingToDeleting: b2.Ptr[int32](30)}}
	src.Spec.Replication = []b2v1.ReplicationRule{{Name: "to-eu-central", DestinationBucketRef: b2v1.LocalBucketReference{Name: "backup"}, IncludeExistingFiles: true}}
	g.Expect(k8s.Create(ctx, src)).To(Succeed())
	g.Expect(k8s.Create(ctx, dst)).To(Succeed())

	eventuallyReason(g, dst, b2v1.ReasonReconciled)
	eventuallyReason(g, src, b2v1.ReasonReconciled)
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
	eventuallyReason(g, dst, b2v1.ReasonDeletionBlocked)

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
	// The retained destination was released only after the source's key
	// mapping was removed from it.
	if released := fakeB2.Bucket(dst.Spec.BucketName).ReplicationConfiguration.Value.AsReplicationDestination; released != nil {
		g.Expect(released.SourceToDestinationKeyMapping).NotTo(HaveKey(srcKey))
	}
}

// Each bucket is both a replication source and a destination. B2 replaces a
// bucket's whole replication configuration on every update, so updating one
// side must carry the other along.
func TestBidirectionalReplicationKeepsBothSides(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	c := uniqueCustomer("cust4004")
	acct := newAccount(c, c, "us-west")
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, b2v1.ReasonReconciled)
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: c}}
	eventuallyReason(g, pc, b2v1.ReasonReconciled)

	ns := createNamespace(t, c+"-app", map[string]string{"customer": c})
	west := newBucket(ns, "west", ns+"-west")
	east := newBucket(ns, "east", ns+"-east")
	for _, b := range []*b2v1.Bucket{west, east} {
		b.Spec.ProviderConfigRef.Name = c
	}
	west.Spec.Replication = []b2v1.ReplicationRule{{Name: "west-to-east", DestinationBucketRef: b2v1.LocalBucketReference{Name: "east"}}}
	east.Spec.Replication = []b2v1.ReplicationRule{{Name: "east-to-west", DestinationBucketRef: b2v1.LocalBucketReference{Name: "west"}}}
	g.Expect(k8s.Create(ctx, west)).To(Succeed())
	g.Expect(k8s.Create(ctx, east)).To(Succeed())

	bothSides := func(g Gomega, name string) {
		rc := fakeB2.Bucket(name).ReplicationConfiguration.Value
		g.Expect(rc).NotTo(BeNil())
		g.Expect(rc.AsReplicationSource).NotTo(BeNil(), "%s lost its replication rule", name)
		g.Expect(rc.AsReplicationSource.ReplicationRules).To(HaveLen(1))
		g.Expect(rc.AsReplicationDestination).NotTo(BeNil(), "%s lost its destination key mapping", name)
		g.Expect(rc.AsReplicationDestination.SourceToDestinationKeyMapping).To(HaveLen(1))
	}
	g.Eventually(func(g Gomega) {
		for _, b := range []*b2v1.Bucket{west, east} {
			g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(b), b)).To(Succeed())
			g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, b2v1.ConditionReady)).To(BeTrue())
		}
		bothSides(g, west.Spec.BucketName)
		bothSides(g, east.Spec.BucketName)
	}, timeout, poll).Should(Succeed())

	// Dropping one direction keeps the other intact on both buckets.
	g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(east), east)).To(Succeed())
	east.Spec.Replication = nil
	g.Expect(k8s.Update(ctx, east)).To(Succeed())
	g.Eventually(func(g Gomega) {
		e := fakeB2.Bucket(east.Spec.BucketName).ReplicationConfiguration.Value
		g.Expect(e.AsReplicationSource).To(BeNil())
		g.Expect(e.AsReplicationDestination).NotTo(BeNil(), "east must still receive from west")
		w := fakeB2.Bucket(west.Spec.BucketName).ReplicationConfiguration.Value
		g.Expect(w.AsReplicationSource).NotTo(BeNil(), "west must still replicate to east")
		g.Expect(w.AsReplicationDestination).To(BeNil())
	}, timeout, poll).Should(Succeed())
}

func TestReplicationRuleNameNeedsSixCharacters(t *testing.T) {
	g := requireEnv(t)
	ns := newNamespace(t, true)
	b := newBucket(ns, "short-rule", ns+"-short-rule")
	b.Spec.Replication = []b2v1.ReplicationRule{{Name: "short", DestinationBucketRef: b2v1.LocalBucketReference{Name: "x"}}}
	err := k8s.Create(context.Background(), b)
	g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "err = %v", err)
}

func TestReplicationNeedsPolicy(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	ns := newNamespace(t, true) // tenant policy does not allow replication
	readyBucket(g, ns, "b")
	src := newBucket(ns, "a", ns+"-a")
	src.Spec.Replication = []b2v1.ReplicationRule{{Name: "replicate", DestinationBucketRef: b2v1.LocalBucketReference{Name: "b"}}}
	g.Expect(k8s.Create(ctx, src)).To(Succeed())
	eventuallyReason(g, src, b2v1.ReasonPolicyDenied)
}
