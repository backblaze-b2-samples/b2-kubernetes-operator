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
	"cmp"
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

var (
	sourceKeyCapabilities      = []string{"readFiles", "readFileLegalHolds", "readFileRetentions"}
	destinationKeyCapabilities = []string{"writeFiles", "writeFileLegalHolds", "writeFileRetentions"}
)

// Cloud Replication is configured from the source Bucket. B2 needs a key in
// the source account that can read the source bucket (one per bucket, shared
// by its rules) and, per destination bucket, a key in the destination account
// that can write to it, registered in the destination bucket's key mapping.
//
// Replication keys are only referenced by ID inside B2, so their secret
// halves are discarded. Their names are deterministic
// (b2op-<cluster>-<bucket uid>-r-...), so a key created just before a crash
// is found and reused rather than duplicated, and the orphan sweep revokes
// them once the Bucket is gone.

// replicationTarget is one resolved destination bucket.
type replicationTarget struct {
	bucket   *b2v1.Bucket
	account  *provider.Account
	pcName   string
	bucketID string
}

// reconcileReplication brings the bucket's replication in B2 to the spec and
// returns the updated source bucket.
func (r *BucketReconciler) reconcileReplication(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket, observed *b2.Bucket) (*b2.Bucket, *stageError) {
	rc := observed.ReplicationConfiguration
	if rc != nil && !rc.IsClientAuthorizedToRead && (len(bkt.Spec.Replication) > 0 || bkt.Status.Replication != nil) {
		return nil, waitFor(b2v1.ReasonReplicationNotReady, 10*time.Minute, "cannot read replication settings; the operator's key needs readBuckets")
	}

	targets := map[string]*replicationTarget{} // by destination Bucket name
	for _, rule := range bkt.Spec.Replication {
		name := rule.DestinationBucketRef.Name
		if _, ok := targets[name]; ok {
			continue
		}
		if name == bkt.Name {
			return nil, waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "replication rule %q points at the bucket itself", rule.Name)
		}
		var dst b2v1.Bucket
		if err := r.Client.Get(ctx, client.ObjectKey{Namespace: bkt.Namespace, Name: name}, &dst); err != nil {
			return nil, waitFor(b2v1.ReasonReplicationNotReady, 30*time.Second, "destination Bucket %q: %v", name, client.IgnoreNotFound(err))
		}
		if !dst.DeletionTimestamp.IsZero() || dst.Status.BucketID == "" || !meta.IsStatusConditionTrue(dst.Status.Conditions, b2v1.ConditionReady) {
			return nil, waitFor(b2v1.ReasonReplicationNotReady, 30*time.Second, "waiting for destination Bucket %q to be ready", name)
		}
		pcName := dst.Spec.ProviderConfigRef.ProviderConfigName()
		dstAcct, se := r.resolveAccount(ctx, pcName)
		if se != nil {
			return nil, se
		}
		targets[name] = &replicationTarget{bucket: &dst, account: dstAcct, pcName: pcName, bucketID: dst.Status.BucketID}
	}

	st := bkt.Status.Replication
	if st == nil {
		st = &b2v1.ReplicationStatus{}
	}

	// Source key.
	if len(targets) > 0 {
		id, err := r.ensureReplicationKey(ctx, acct, st.SourceKeyID, r.replicationKeyName(bkt, "src"), sourceKeyCapabilities, observed.BucketID)
		if err != nil {
			return nil, providerError("creating the replication source key", err)
		}
		st.SourceKeyID = id
	}

	// Destination keys and key mappings.
	var dests []b2v1.ReplicationDestinationStatus
	for _, t := range sortedTargets(targets) {
		prevID := ""
		if i := slices.IndexFunc(st.Destinations, func(d b2v1.ReplicationDestinationStatus) bool { return d.BucketID == t.bucketID }); i >= 0 {
			prevID = st.Destinations[i].KeyID
		}
		keyID, err := r.ensureReplicationKey(ctx, t.account, prevID, r.replicationKeyName(bkt, uid8(t.bucket.UID)), destinationKeyCapabilities, t.bucketID)
		if err != nil {
			return nil, providerError(fmt.Sprintf("creating the replication key for Bucket %q", t.bucket.Name), err)
		}
		if se := r.setKeyMapping(ctx, t.account, t.bucketID, st.SourceKeyID, keyID); se != nil {
			return nil, se
		}
		dests = append(dests, b2v1.ReplicationDestinationStatus{Bucket: t.bucket.Name, BucketID: t.bucketID, ProviderConfig: t.pcName, KeyID: keyID})
	}

	// Source rules.
	want := desiredReplicationSource(bkt, targets, st.SourceKeyID)
	var current *b2.ReplicationSource
	if rc != nil && rc.Value != nil {
		current = rc.Value.AsReplicationSource
	}
	if !replicationSourceEqual(current, want) {
		updated, err := acct.Client.UpdateBucket(ctx, b2.UpdateBucketRequest{
			BucketID:                 observed.BucketID,
			IfRevisionIs:             observed.Revision,
			ReplicationConfiguration: &b2.ReplicationConfiguration{AsReplicationSource: want},
		})
		if err != nil {
			if b2.HasCode(err, b2.CodeConflict) {
				return nil, waitFor(b2v1.ReasonReconciling, time.Second, "bucket changed concurrently")
			}
			return nil, providerError("updating replication rules", err)
		}
		observed = updated
		r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, "ReplicationUpdated", "Update", "Replication now has %d rule(s)", len(want.ReplicationRules))
	}

	// Tear down destinations that are no longer used.
	for _, old := range st.Destinations {
		if slices.ContainsFunc(dests, func(d b2v1.ReplicationDestinationStatus) bool { return d.BucketID == old.BucketID }) {
			continue
		}
		if err := r.removeDestination(ctx, st.SourceKeyID, old); err != nil {
			return nil, providerError(fmt.Sprintf("removing replication to Bucket %q", old.Bucket), err)
		}
	}
	st.Destinations = dests

	if len(targets) == 0 && st.SourceKeyID != "" {
		if err := deleteKeyIfExists(ctx, acct.Client, st.SourceKeyID); err != nil {
			return nil, providerError("revoking the replication source key", err)
		}
		st.SourceKeyID = ""
	}
	if st.SourceKeyID == "" && len(st.Destinations) == 0 {
		bkt.Status.Replication = nil
	} else {
		bkt.Status.Replication = st
	}
	return observed, nil
}

func (r *BucketReconciler) replicationKeyName(bkt *b2v1.Bucket, suffix string) string {
	return fmt.Sprintf("%s-%s-%s-r-%s", KeyNamePrefix, r.Options.ClusterID, uid8(bkt.UID), suffix)
}

// ensureReplicationKey returns a live key with the given name, reusing the
// recorded one or one left by an interrupted pass, and creating it otherwise.
func (r *BucketReconciler) ensureReplicationKey(ctx context.Context, acct *provider.Account, knownID, name string, caps []string, bucketID string) (string, error) {
	if knownID != "" {
		ok, err := acct.Client.KeyExists(ctx, knownID)
		if err != nil || ok {
			return knownID, err
		}
	}
	found, err := acct.Client.FindKeys(ctx, func(k b2.ApplicationKey) bool { return k.KeyName == name })
	if err != nil {
		return "", err
	}
	if len(found) > 0 {
		for _, extra := range found[1:] {
			if err := deleteKeyIfExists(ctx, acct.Client, extra.ApplicationKeyID); err != nil {
				return "", err
			}
		}
		return found[0].ApplicationKeyID, nil
	}
	k, err := acct.Client.CreateKey(ctx, b2.CreateKeyRequest{KeyName: name, Capabilities: caps, BucketIDs: []string{bucketID}})
	if err != nil {
		return "", err
	}
	log.FromContext(ctx).Info("created replication key", "keyID", k.ApplicationKeyID, "keyName", name)
	return k.ApplicationKeyID, nil
}

// setKeyMapping maps sourceKeyID to destKeyID on the destination bucket, and
// drops stale mappings to destKeyID from earlier source keys.
func (r *BucketReconciler) setKeyMapping(ctx context.Context, acct *provider.Account, bucketID, sourceKeyID, destKeyID string) *stageError {
	b, err := acct.Client.GetBucketByID(ctx, bucketID)
	if err != nil {
		return providerError("reading the destination bucket", err)
	}
	if b == nil {
		return waitFor(b2v1.ReasonReplicationNotReady, 30*time.Second, "destination bucket %s no longer exists", bucketID)
	}
	current := map[string]string{}
	if rc := b.ReplicationConfiguration; rc != nil && rc.Value != nil && rc.Value.AsReplicationDestination != nil {
		current = rc.Value.AsReplicationDestination.SourceToDestinationKeyMapping
	}
	want := maps.Clone(current)
	if want == nil {
		want = map[string]string{}
	}
	for src, dst := range want {
		if dst == destKeyID && src != sourceKeyID {
			delete(want, src)
		}
	}
	want[sourceKeyID] = destKeyID
	if maps.Equal(current, want) {
		return nil
	}
	_, err = acct.Client.UpdateBucket(ctx, b2.UpdateBucketRequest{
		BucketID:     bucketID,
		IfRevisionIs: b.Revision,
		ReplicationConfiguration: &b2.ReplicationConfiguration{
			AsReplicationDestination: &b2.ReplicationDestination{SourceToDestinationKeyMapping: want},
		},
	})
	if b2.HasCode(err, b2.CodeConflict) {
		return waitFor(b2v1.ReasonReconciling, time.Second, "destination bucket changed concurrently")
	}
	if err != nil {
		return providerError("updating the destination key mapping", err)
	}
	return nil
}

// removeDestination unmaps and revokes the key for a destination that is no
// longer replicated to.
func (r *BucketReconciler) removeDestination(ctx context.Context, sourceKeyID string, d b2v1.ReplicationDestinationStatus) error {
	dstAcct, se := r.resolveAccount(ctx, d.ProviderConfig)
	if se != nil {
		return se
	}
	b, err := dstAcct.Client.GetBucketByID(ctx, d.BucketID)
	if err != nil {
		return err
	}
	if b != nil && b.ReplicationConfiguration != nil && b.ReplicationConfiguration.Value != nil && b.ReplicationConfiguration.Value.AsReplicationDestination != nil {
		mapping := maps.Clone(b.ReplicationConfiguration.Value.AsReplicationDestination.SourceToDestinationKeyMapping)
		for src, dst := range mapping {
			if dst == d.KeyID || src == sourceKeyID {
				delete(mapping, src)
			}
		}
		if mapping == nil {
			mapping = map[string]string{}
		}
		_, err := dstAcct.Client.UpdateBucket(ctx, b2.UpdateBucketRequest{
			BucketID:                 d.BucketID,
			IfRevisionIs:             b.Revision,
			ReplicationConfiguration: &b2.ReplicationConfiguration{AsReplicationDestination: &b2.ReplicationDestination{SourceToDestinationKeyMapping: mapping}},
		})
		if err != nil {
			return err
		}
	}
	return deleteKeyIfExists(ctx, dstAcct.Client, d.KeyID)
}

func desiredReplicationSource(bkt *b2v1.Bucket, targets map[string]*replicationTarget, sourceKeyID string) *b2.ReplicationSource {
	src := &b2.ReplicationSource{ReplicationRules: []b2.ReplicationRule{}}
	for _, rule := range bkt.Spec.Replication {
		enabled := rule.Enabled == nil || *rule.Enabled
		priority := rule.Priority
		if priority == 0 {
			priority = 1
		}
		src.ReplicationRules = append(src.ReplicationRules, b2.ReplicationRule{
			ReplicationRuleName:  rule.Name,
			DestinationBucketID:  targets[rule.DestinationBucketRef.Name].bucketID,
			FileNamePrefix:       rule.FileNamePrefix,
			IncludeExistingFiles: rule.IncludeExistingFiles,
			IsEnabled:            enabled,
			Priority:             priority,
		})
	}
	if len(src.ReplicationRules) > 0 {
		src.SourceApplicationKeyID = b2.Ptr(sourceKeyID)
	}
	return src
}

func replicationSourceEqual(current, want *b2.ReplicationSource) bool {
	rules := func(s *b2.ReplicationSource) []b2.ReplicationRule {
		if s == nil || len(s.ReplicationRules) == 0 {
			return nil
		}
		out := slices.Clone(s.ReplicationRules)
		slices.SortFunc(out, func(a, b b2.ReplicationRule) int { return cmp.Compare(a.ReplicationRuleName, b.ReplicationRuleName) })
		return out
	}
	key := func(s *b2.ReplicationSource) string {
		if s == nil || s.SourceApplicationKeyID == nil || len(s.ReplicationRules) == 0 {
			return ""
		}
		return *s.SourceApplicationKeyID
	}
	return reflect.DeepEqual(rules(current), rules(want)) && key(current) == key(want)
}

func sortedTargets(m map[string]*replicationTarget) []*replicationTarget {
	out := slices.Collect(maps.Values(m))
	slices.SortFunc(out, func(a, b *replicationTarget) int { return cmp.Compare(a.bucket.Name, b.bucket.Name) })
	return out
}

func deleteKeyIfExists(ctx context.Context, c *b2.Client, id string) error {
	err := c.DeleteKey(ctx, id)
	if err == nil {
		return nil
	}
	if apiErr, ok := b2.AsAPIError(err); ok && apiErr.Status == 400 {
		if exists, xerr := c.KeyExists(ctx, id); xerr == nil && !exists {
			return nil
		}
	}
	return err
}
