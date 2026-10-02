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

// Cloud Replication is configured from the source Bucket. B2 needs a key in
// the source account that can read the source bucket (one per bucket, shared
// by its rules), and per destination bucket a key in the destination account
// that can write to it, registered in the destination's key mapping.
//
// Replication keys are only referenced by ID inside B2, so their secret
// halves are discarded. Their names are deterministic
// (b2op-<cluster>-<bucket uid>-r-...), so a key created just before a crash
// is found and reused rather than duplicated, and the orphan sweep revokes
// them once the Bucket is gone.
//
// B2 replaces a bucket's whole replicationConfiguration on every update,
// whatever the API reference says about omitted sides (observed on the live
// API: sending one side clears the other, and null clears both). Every
// update therefore sends both sides, copying the untouched one from B2, and
// omits empty sides, since B2 rejects empty rule lists and mappings.

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// replicationTarget is a destination bucket, resolved to its B2 account.
type replicationTarget struct {
	bucket         *b2v1.Bucket
	account        *provider.Account
	providerConfig string
}

func (t *replicationTarget) bucketID() string { return t.bucket.Status.BucketID }

// reconcileReplication brings the bucket's replication in B2 to the spec,
// recording the keys in status, and returns the updated source bucket.
func (r *BucketReconciler) reconcileReplication(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket, observed *b2.Bucket) (*b2.Bucket, *stageError) {
	if rc := observed.ReplicationConfiguration; rc != nil && !rc.IsClientAuthorizedToRead && (len(bkt.Spec.Replication) > 0 || bkt.Status.Replication != nil) {
		return nil, waitFor(b2v1.ReasonReplicationNotReady, 10*time.Minute,
			"cannot read replication settings; the operator's key needs the readBucketReplications and writeBucketReplications capabilities")
	}
	targets, se := r.replicationTargets(ctx, bkt)
	if se != nil {
		return nil, se
	}
	status := bkt.Status.Replication
	if status == nil {
		status = &b2v1.ReplicationStatus{}
	}

	if len(targets) > 0 {
		id, err := r.ensureReplicationKey(ctx, acct, status.SourceKeyID, r.replicationKeyName(bkt, "src"), sourceKeyCapabilities, observed.BucketID)
		if err != nil {
			return nil, providerError("creating the replication source key", err)
		}
		status.SourceKeyID = id
	}
	dests, se := r.ensureDestinations(ctx, bkt, status, targets)
	if se != nil {
		return nil, se
	}
	if observed, se = r.applySourceRules(ctx, acct, bkt, observed, targets, status.SourceKeyID); se != nil {
		return nil, se
	}
	if se := r.removeStaleDestinations(ctx, status, dests); se != nil {
		return nil, se
	}
	status.Destinations = dests

	if len(targets) == 0 && status.SourceKeyID != "" {
		if err := acct.Client.DeleteKeyIfExists(ctx, status.SourceKeyID); err != nil {
			return nil, providerError("revoking the replication source key", err)
		}
		status.SourceKeyID = ""
	}
	bkt.Status.Replication = status
	if status.SourceKeyID == "" && len(status.Destinations) == 0 {
		bkt.Status.Replication = nil
	}
	return observed, nil
}

// replicationTargets resolves each destination Bucket named by the rules.
// A destination only needs to exist in B2; waiting for it to be Ready would
// deadlock bidirectional replication, since Ready includes its own rules.
func (r *BucketReconciler) replicationTargets(ctx context.Context, bkt *b2v1.Bucket) (map[string]*replicationTarget, *stageError) {
	targets := map[string]*replicationTarget{}
	for _, rule := range bkt.Spec.Replication {
		name := rule.DestinationBucketRef.Name
		if _, done := targets[name]; done {
			continue
		}
		if name == bkt.Name {
			return nil, waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "replication rule %q points at the bucket itself", rule.Name)
		}
		var dst b2v1.Bucket
		if err := r.Client.Get(ctx, client.ObjectKey{Namespace: bkt.Namespace, Name: name}, &dst); err != nil {
			return nil, waitFor(b2v1.ReasonReplicationNotReady, 30*time.Second, "destination Bucket %q: %v", name, client.IgnoreNotFound(err))
		}
		if !dst.DeletionTimestamp.IsZero() || dst.Status.BucketID == "" {
			return nil, waitFor(b2v1.ReasonReplicationNotReady, 30*time.Second, "waiting for destination Bucket %q to be created", name)
		}
		pcName := dst.Spec.ProviderConfigRef.NameOrDefault()
		dstAcct, se := r.resolveAccount(ctx, pcName)
		if se != nil {
			return nil, se
		}
		targets[name] = &replicationTarget{bucket: &dst, account: dstAcct, providerConfig: pcName}
	}
	return targets, nil
}

// ensureDestinations makes sure each destination has a write key mapped to
// the source key, and returns the destinations' status.
func (r *BucketReconciler) ensureDestinations(ctx context.Context, bkt *b2v1.Bucket, status *b2v1.ReplicationStatus,
	targets map[string]*replicationTarget) ([]b2v1.ReplicationDestinationStatus, *stageError) {
	sorted := slices.SortedFunc(maps.Values(targets), func(a, b *replicationTarget) int { return cmp.Compare(a.bucket.Name, b.bucket.Name) })
	dests := make([]b2v1.ReplicationDestinationStatus, 0, len(sorted))
	for _, t := range sorted {
		knownID := ""
		if i := slices.IndexFunc(status.Destinations, func(d b2v1.ReplicationDestinationStatus) bool { return d.BucketID == t.bucketID() }); i >= 0 {
			knownID = status.Destinations[i].KeyID
		}
		keyID, err := r.ensureReplicationKey(ctx, t.account, knownID, r.replicationKeyName(bkt, uid8(t.bucket.UID)), destinationKeyCapabilities, t.bucketID())
		if err != nil {
			return nil, providerError(fmt.Sprintf("creating the replication key for Bucket %q", t.bucket.Name), err)
		}
		if se := setKeyMapping(ctx, t.account, t.bucketID(), status.SourceKeyID, keyID); se != nil {
			return nil, se
		}
		dests = append(dests, b2v1.ReplicationDestinationStatus{Bucket: t.bucket.Name, BucketID: t.bucketID(), ProviderConfig: t.providerConfig, KeyID: keyID})
	}
	return dests, nil
}

// applySourceRules sets the source bucket's rules, if they differ from B2.
func (r *BucketReconciler) applySourceRules(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket, observed *b2.Bucket,
	targets map[string]*replicationTarget, sourceKeyID string) (*b2.Bucket, *stageError) {
	want := desiredReplicationSource(bkt, targets, sourceKeyID)
	var current *b2.ReplicationSource
	if rc := currentReplication(observed); rc != nil {
		current = rc.AsReplicationSource
	}
	if replicationSourceEqual(current, want) {
		return observed, nil
	}
	updated, err := acct.Client.UpdateBucket(ctx, b2.UpdateBucketRequest{
		BucketID:                 observed.BucketID,
		IfRevisionIs:             observed.Revision,
		ReplicationConfiguration: withSource(observed, want),
	})
	if b2.HasCode(err, b2.CodeConflict) {
		return nil, waitFor(b2v1.ReasonReconciling, time.Second, "bucket changed concurrently")
	}
	if err != nil {
		return nil, providerError("updating replication rules", err)
	}
	r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, EventReplicationUpdated, "Update", "Replication now has %d rule(s)", len(bkt.Spec.Replication))
	return updated, nil
}

// removeStaleDestinations unmaps and revokes the keys of destinations that
// are no longer replicated to.
func (r *BucketReconciler) removeStaleDestinations(ctx context.Context, status *b2v1.ReplicationStatus, current []b2v1.ReplicationDestinationStatus) *stageError {
	for _, old := range status.Destinations {
		if slices.ContainsFunc(current, func(d b2v1.ReplicationDestinationStatus) bool { return d.BucketID == old.BucketID }) {
			continue
		}
		dstAcct, se := r.resolveAccount(ctx, old.ProviderConfig)
		if se != nil {
			return se
		}
		err := unmapDestination(ctx, dstAcct, old, status.SourceKeyID)
		if b2.HasCode(err, b2.CodeConflict) {
			return waitFor(b2v1.ReasonReconciling, time.Second, "destination bucket changed concurrently")
		}
		if err != nil {
			return providerError(fmt.Sprintf("removing replication to Bucket %q", old.Bucket), err)
		}
	}
	return nil
}

func (r *BucketReconciler) replicationKeyName(bkt *b2v1.Bucket, suffix string) string {
	return fmt.Sprintf("%s-%s-%s-r-%s", KeyNamePrefix, r.Options.ClusterID, uid8(bkt.UID), suffix)
}

// ensureReplicationKey returns a live key with the given name: the recorded
// one, one left by an interrupted pass, or a new one.
func (r *BucketReconciler) ensureReplicationKey(ctx context.Context, acct *provider.Account, knownID, name string, caps []string, bucketID string) (string, error) {
	if knownID != "" {
		if exists, err := acct.Client.KeyExists(ctx, knownID); err != nil || exists {
			return knownID, err
		}
	}
	found, err := acct.Client.FindKeys(ctx, func(k b2.ApplicationKey) bool { return k.KeyName == name })
	if err != nil {
		return "", err
	}
	if len(found) > 0 {
		for _, duplicate := range found[1:] {
			if err := acct.Client.DeleteKeyIfExists(ctx, duplicate.ApplicationKeyID); err != nil {
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
func setKeyMapping(ctx context.Context, acct *provider.Account, bucketID, sourceKeyID, destKeyID string) *stageError {
	b, err := acct.Client.GetBucketByID(ctx, bucketID)
	if err != nil {
		return providerError("reading the destination bucket", err)
	}
	if b == nil {
		return waitFor(b2v1.ReasonReplicationNotReady, 30*time.Second, "destination bucket %s no longer exists", bucketID)
	}
	current := keyMapping(b)
	want := maps.Clone(current)
	maps.DeleteFunc(want, func(src, dst string) bool { return dst == destKeyID && src != sourceKeyID })
	want[sourceKeyID] = destKeyID
	if maps.Equal(current, want) {
		return nil
	}
	_, err = acct.Client.UpdateBucket(ctx, b2.UpdateBucketRequest{BucketID: bucketID, IfRevisionIs: b.Revision, ReplicationConfiguration: withDestination(b, want)})
	if b2.HasCode(err, b2.CodeConflict) {
		return waitFor(b2v1.ReasonReconciling, time.Second, "destination bucket changed concurrently")
	}
	if err != nil {
		return providerError("updating the destination key mapping", err)
	}
	return nil
}

// unmapDestination removes the source's mapping from a destination bucket
// and revokes the destination key.
func unmapDestination(ctx context.Context, acct *provider.Account, d b2v1.ReplicationDestinationStatus, sourceKeyID string) error {
	b, err := acct.Client.GetBucketByID(ctx, d.BucketID)
	if err != nil {
		return err
	}
	if b != nil {
		mapping := keyMapping(b)
		maps.DeleteFunc(mapping, func(src, dst string) bool { return dst == d.KeyID || src == sourceKeyID })
		if !maps.Equal(mapping, keyMapping(b)) {
			req := b2.UpdateBucketRequest{BucketID: d.BucketID, IfRevisionIs: b.Revision, ReplicationConfiguration: withDestination(b, mapping)}
			if _, err := acct.Client.UpdateBucket(ctx, req); err != nil {
				return err
			}
		}
	}
	return acct.Client.DeleteKeyIfExists(ctx, d.KeyID)
}

func desiredReplicationSource(bkt *b2v1.Bucket, targets map[string]*replicationTarget, sourceKeyID string) *b2.ReplicationSource {
	if len(bkt.Spec.Replication) == 0 {
		return nil
	}
	src := &b2.ReplicationSource{SourceApplicationKeyID: b2.Ptr(sourceKeyID)}
	for _, rule := range bkt.Spec.Replication {
		priority := rule.Priority
		if priority == 0 {
			priority = 1
		}
		src.ReplicationRules = append(src.ReplicationRules, b2.ReplicationRule{
			ReplicationRuleName:  rule.Name,
			DestinationBucketID:  targets[rule.DestinationBucketRef.Name].bucketID(),
			FileNamePrefix:       rule.FileNamePrefix,
			IncludeExistingFiles: rule.IncludeExistingFiles,
			IsEnabled:            rule.Enabled == nil || *rule.Enabled,
			Priority:             priority,
		})
	}
	return src
}

// withSource is b's replication configuration with its source side
// replaced by src (nil removes it).
func withSource(b *b2.Bucket, src *b2.ReplicationSource) *b2.ReplicationConfiguration {
	rc := &b2.ReplicationConfiguration{AsReplicationSource: src}
	if mapping := keyMapping(b); len(mapping) > 0 {
		rc.AsReplicationDestination = &b2.ReplicationDestination{SourceToDestinationKeyMapping: mapping}
	}
	return rc
}

// withDestination is b's replication configuration with its key mapping
// replaced by mapping (empty removes the destination side).
func withDestination(b *b2.Bucket, mapping map[string]string) *b2.ReplicationConfiguration {
	rc := &b2.ReplicationConfiguration{}
	if len(mapping) > 0 {
		rc.AsReplicationDestination = &b2.ReplicationDestination{SourceToDestinationKeyMapping: mapping}
	}
	if cur := currentReplication(b); cur != nil && cur.AsReplicationSource != nil && len(cur.AsReplicationSource.ReplicationRules) > 0 {
		rc.AsReplicationSource = cur.AsReplicationSource
	}
	return rc
}

func currentReplication(b *b2.Bucket) *b2.ReplicationConfiguration {
	if b == nil || b.ReplicationConfiguration == nil {
		return nil
	}
	return b.ReplicationConfiguration.Value
}

// keyMapping is a copy of b's destination key mapping (never nil).
func keyMapping(b *b2.Bucket) map[string]string {
	if rc := currentReplication(b); rc != nil && rc.AsReplicationDestination != nil {
		if m := maps.Clone(rc.AsReplicationDestination.SourceToDestinationKeyMapping); m != nil {
			return m
		}
	}
	return map[string]string{}
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
