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
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/policy"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

// BucketReconciler keeps B2 buckets in sync with Bucket resources.
//
// Every pass reads the bucket from B2, compares it with the spec, and
// corrects it in one revision-checked update. Ownership is recorded in the
// bucket's own bucketInfo, so the operator never takes over or deletes a
// bucket it did not create unless adoption was requested and allowed.
type BucketReconciler struct {
	Deps
}

// bucketOrigin is how the reconciler came to manage the bucket on this pass.
type bucketOrigin int

const (
	bucketOwned   bucketOrigin = iota // already managed by this resource
	bucketCreated                     // created on this pass
	bucketAdopted                     // existing bucket taken over on this pass
)

// +kubebuilder:rbac:groups=b2.backblaze.com,resources=buckets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=buckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=buckets/finalizers,verbs=update
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=b2accesspolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

func (r *BucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var bkt b2v1.Bucket
	if err := r.Client.Get(ctx, req.NamespacedName, &bkt); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !bkt.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &bkt)
	}
	if err := r.ensureFinalizer(ctx, &bkt); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := bkt.DeepCopy()
	res, err := r.reconcile(ctx, &bkt)
	bkt.Status.ObservedGeneration = bkt.Generation
	return r.commitStatus(ctx, &bkt, orig, res, err)
}

func (r *BucketReconciler) reconcile(ctx context.Context, bkt *b2v1.Bucket) (ctrl.Result, error) {
	notReady := r.notReady(bkt)
	acct, se := r.resolveAccount(ctx, bkt.Spec.ProviderConfigRef.NameOrDefault())
	if se != nil {
		return result(se, notReady)
	}
	if se := r.checkPolicy(ctx, bkt, false); se != nil {
		return result(se, notReady)
	}
	observed, origin, se := r.observeOrCreate(ctx, acct, bkt)
	if se != nil {
		return result(se, notReady)
	}

	upd, changed, warnings := diffBucket(bkt, observed)
	if len(changed) > 0 {
		updated, err := acct.Client.UpdateBucket(ctx, upd)
		if b2.HasCode(err, b2.CodeConflict) {
			return ctrl.Result{RequeueAfter: time.Second}, nil // changed in B2 meanwhile; re-read
		}
		if err != nil {
			return result(providerError("updating bucket", err), notReady)
		}
		observed = updated
		r.recordUpdate(ctx, bkt, origin, changed)
	}

	bkt.Status.BucketID = observed.BucketID
	bkt.Status.S3Endpoint = acct.S3Endpoint
	bkt.Status.S3Region = acct.S3Region
	bkt.Status.ObjectLockEnabled = fileLockEnabled(observed)

	observed, se = r.reconcileReplication(ctx, acct, bkt, observed)
	if se != nil {
		return result(se, notReady)
	}

	now := metav1.NewTime(r.now())
	bkt.Status.Revision = int64(observed.Revision)
	bkt.Status.LastSyncTime = &now
	msg := "Bucket is in sync with B2"
	if len(warnings) > 0 {
		msg += "; " + strings.Join(warnings, "; ")
	}
	markReady(bkt, msg)
	return ctrl.Result{RequeueAfter: r.Options.ResyncPeriod}, nil
}

// recordUpdate emits the event for an update. An update without a spec
// change since the last successful pass is a drift correction.
func (r *BucketReconciler) recordUpdate(ctx context.Context, bkt *b2v1.Bucket, origin bucketOrigin, changed []string) {
	fields := strings.Join(changed, ", ")
	switch {
	case origin == bucketCreated:
		// The Created event already covers it.
	case origin == bucketAdopted:
		r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, EventAdopted, "Adopt", "Adopted existing bucket %s and applied %s", bkt.Spec.BucketName, fields)
	case bkt.Status.ObservedGeneration == bkt.Generation:
		log.FromContext(ctx).Info("corrected drift", "fields", changed)
		r.Recorder.Eventf(bkt, nil, corev1.EventTypeWarning, EventDriftCorrected, "Update", "Bucket %s was changed outside Kubernetes; restored %s", bkt.Spec.BucketName, fields)
	default:
		r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, EventUpdated, "Update", "Updated %s", fields)
	}
}

func (r *BucketReconciler) checkPolicy(ctx context.Context, bkt *b2v1.Bucket, adopt bool) *stageError {
	d, err := r.Policy.CheckBucket(ctx, bucketPolicyRequest(bkt, adopt))
	if err != nil {
		return &stageError{reason: b2v1.ReasonPolicyDenied, message: err.Error(), err: err}
	}
	if !d.Allowed {
		return &stageError{reason: b2v1.ReasonPolicyDenied, message: d.Reason} // policy changes re-trigger
	}
	return nil
}

func bucketPolicyRequest(bkt *b2v1.Bucket, adopt bool) policy.BucketRequest {
	ol := bkt.Spec.ObjectLock
	return policy.BucketRequest{
		Namespace:           bkt.Namespace,
		ProviderConfig:      bkt.Spec.ProviderConfigRef.NameOrDefault(),
		BucketName:          bkt.Spec.BucketName,
		Public:              bkt.Spec.BucketType == b2v1.BucketTypeAllPublic,
		Adopt:               adopt,
		Delete:              bkt.Spec.DeletionPolicy == b2v1.DeletionPolicyDelete,
		ComplianceRetention: ol != nil && ol.DefaultRetention != nil && ol.DefaultRetention.Mode == b2v1.RetentionModeCompliance,
		Unencrypted:         bkt.Spec.DefaultEncryption != nil && bkt.Spec.DefaultEncryption.Mode == b2v1.EncryptionModeNone,
		Replication:         len(bkt.Spec.Replication) > 0,
	}
}

// observeOrCreate finds the bucket in B2 and checks this resource owns it
// (adopting it if requested and allowed), or creates it.
func (r *BucketReconciler) observeOrCreate(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket) (*b2.Bucket, bucketOrigin, *stageError) {
	observed, se := r.lookup(ctx, acct, bkt)
	if se != nil {
		return nil, 0, se
	}
	if observed == nil {
		return r.create(ctx, acct, bkt)
	}

	owner, releasedBy := observed.BucketInfo[OwnerInfoKey], observed.BucketInfo[ReleasedInfoKey]
	switch {
	case owner == string(bkt.UID):
		return observed, bucketOwned, nil
	case owner != "":
		return nil, 0, waitFor(b2v1.ReasonBucketOwnedElsewhere, 10*time.Minute,
			"bucket %q is managed by another resource (owner UID %s); remove bucketInfo key %q from the bucket to release it",
			bkt.Spec.BucketName, owner, OwnerInfoKey)
	case releasedBy != "" && releasedBy != bkt.Namespace:
		return nil, 0, waitFor(b2v1.ReasonBucketOwnedElsewhere, 10*time.Minute,
			"bucket %q was released by namespace %s; a cluster administrator must remove bucketInfo key %q before another namespace can adopt it",
			bkt.Spec.BucketName, releasedBy, ReleasedInfoKey)
	case !bkt.Spec.AdoptExisting:
		return nil, 0, waitFor(b2v1.ReasonBucketAlreadyExists, 10*time.Minute,
			"bucket %q already exists in the account; set spec.adoptExisting: true to manage it", bkt.Spec.BucketName)
	}
	if se := r.checkPolicy(ctx, bkt, true); se != nil {
		return nil, 0, se
	}
	return observed, bucketAdopted, nil
}

// lookup reads the bucket from B2 by its recorded ID, falling back to its
// name (for a new resource, or one whose bucket was deleted outside
// Kubernetes). It returns nil if there is no such bucket.
func (r *BucketReconciler) lookup(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket) (*b2.Bucket, *stageError) {
	if bkt.Status.BucketID != "" {
		observed, err := acct.Client.GetBucketByID(ctx, bkt.Status.BucketID)
		if err != nil {
			return nil, providerError("reading bucket", err)
		}
		if observed != nil {
			return observed, nil
		}
		r.Recorder.Eventf(bkt, nil, corev1.EventTypeWarning, EventBucketMissing, "Reconcile",
			"Bucket %s (%s) was deleted outside Kubernetes; recreating it", bkt.Spec.BucketName, bkt.Status.BucketID)
		bkt.Status.BucketID = ""
	}
	observed, err := acct.Client.GetBucketByName(ctx, bkt.Spec.BucketName)
	if err != nil {
		return nil, providerError("looking up bucket", err)
	}
	return observed, nil
}

func (r *BucketReconciler) create(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket) (*b2.Bucket, bucketOrigin, *stageError) {
	created, err := acct.Client.CreateBucket(ctx, newBucketRequest(bkt))
	switch {
	case b2.HasCode(err, b2.CodeDuplicateBucketName):
		return nil, 0, waitFor(b2v1.ReasonBucketNameUnavailable, 10*time.Minute,
			"bucket name %q is already taken by another B2 account; bucket names are global, so choose another", bkt.Spec.BucketName)
	case b2.HasCode(err, b2.CodeTooManyBuckets):
		return nil, 0, waitFor(b2v1.ReasonProviderError, 10*time.Minute, "the B2 account has reached its bucket limit")
	case err != nil:
		return nil, 0, providerError("creating bucket", err)
	}
	log.FromContext(ctx).Info("created bucket", "bucket", created.BucketName, "bucketID", created.BucketID)
	r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, EventCreated, "Create", "Created bucket %s (%s)", created.BucketName, created.BucketID)
	return created, bucketCreated, nil
}

// finalize tears down replication from the bucket, then deletes or releases
// it according to deletionPolicy. It waits while any key or replication
// rule still depends on the bucket.
func (r *BucketReconciler) finalize(ctx context.Context, bkt *b2v1.Bucket) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(bkt, Finalizer) {
		return ctrl.Result{}, nil
	}
	orig := bkt.DeepCopy()
	fail := func(se *stageError) (ctrl.Result, error) { return r.failDeletion(ctx, bkt, orig, se) }

	acct, se := r.resolveAccount(ctx, bkt.Spec.ProviderConfigRef.NameOrDefault())
	if se != nil {
		if bkt.Status.BucketID == "" && !reachedB2(bkt) {
			// Nothing was ever created in B2 (e.g. the bucket was denied by
			// policy), so there is nothing to clean up.
			return r.releaseFinalizer(ctx, bkt)
		}
		se.message = "cannot delete: " + se.message + " (remove the finalizer to abandon the bucket)"
		return fail(se)
	}

	observed, se := r.lookup(ctx, acct, bkt)
	if se != nil {
		return fail(se)
	}
	if observed == nil || observed.BucketInfo[OwnerInfoKey] != string(bkt.UID) {
		return r.releaseFinalizer(ctx, bkt) // gone, or never ours
	}
	if se := r.deletionBlocker(ctx, bkt, observed); se != nil {
		return fail(se)
	}

	teardown := bkt.DeepCopy()
	teardown.Spec.Replication = nil
	observed, se = r.reconcileReplication(ctx, acct, teardown, observed)
	if se != nil {
		return fail(se)
	}
	bkt.Status.Replication = teardown.Status.Replication

	if bkt.Spec.DeletionPolicy == b2v1.DeletionPolicyDelete {
		denied := r.checkPolicy(ctx, bkt, false)
		if denied == nil {
			if se := r.deleteBucket(ctx, acct, bkt, observed); se != nil {
				return fail(se)
			}
			return r.releaseFinalizer(ctx, bkt)
		}
		r.Recorder.Eventf(bkt, nil, corev1.EventTypeWarning, b2v1.ReasonPolicyDenied, "Delete",
			"Not deleting bucket %s: %s; it is retained instead", bkt.Spec.BucketName, denied.message)
	}
	if se := r.releaseBucket(ctx, acct, bkt, observed); se != nil {
		return fail(se)
	}
	return r.releaseFinalizer(ctx, bkt)
}

// deletionBlocker reports what still depends on the bucket: keys holding
// credentials for it, or replication into it. No credentials may outlive
// the resource that granted them.
func (r *BucketReconciler) deletionBlocker(ctx context.Context, bkt *b2v1.Bucket, observed *b2.Bucket) *stageError {
	var keys b2v1.ApplicationKeyList
	if err := r.Client.List(ctx, &keys, client.InNamespace(bkt.Namespace), client.MatchingFields{indexBucketRef: bkt.Name}); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	// A key that was never issued (e.g. denied by policy) has nothing to revoke.
	live := slices.DeleteFunc(keys.Items, func(k b2v1.ApplicationKey) bool {
		return k.Status.KeyID == "" && k.Status.PendingKeyName == "" && len(k.Status.RetiringKeys) == 0
	})
	if len(live) > 0 {
		return waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
			"%d ApplicationKey(s) with live credentials still reference this bucket (e.g. %s); delete them first", len(live), live[0].Name)
	}

	var peers b2v1.BucketList
	if err := r.Client.List(ctx, &peers, client.InNamespace(bkt.Namespace)); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	for _, p := range peers.Items {
		if slices.ContainsFunc(p.Spec.Replication, func(rule b2v1.ReplicationRule) bool { return rule.DestinationBucketRef.Name == bkt.Name }) {
			return waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
				"Bucket %s replicates into this bucket; remove its replication rule first", p.Name)
		}
		// A source whose rule was just removed may still be revoking its key.
		if p.Status.Replication != nil && slices.ContainsFunc(p.Status.Replication.Destinations,
			func(d b2v1.ReplicationDestinationStatus) bool { return d.BucketID == observed.BucketID }) {
			return waitFor(b2v1.ReasonDeletionBlocked, 30*time.Second,
				"Bucket %s is still removing its replication into this bucket", p.Name)
		}
	}
	return nil
}

func (r *BucketReconciler) deleteBucket(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket, observed *b2.Bucket) *stageError {
	err := acct.Client.DeleteBucket(ctx, observed.BucketID)
	if b2.HasCode(err, b2.CodeCannotDeleteNonEmptyBucket) {
		return waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
			"bucket %s is not empty; B2 only deletes empty buckets. Delete all file versions, or set deletionPolicy: Retain", bkt.Spec.BucketName)
	}
	if err != nil {
		return providerError("deleting bucket", err)
	}
	r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, EventDeleted, "Delete", "Deleted bucket %s", bkt.Spec.BucketName)
	return nil
}

// releaseBucket leaves the bucket in B2, replacing the ownership mark with
// a note of the releasing namespace, which alone may adopt it again.
func (r *BucketReconciler) releaseBucket(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket, observed *b2.Bucket) *stageError {
	info := maps.Clone(observed.BucketInfo)
	if info == nil {
		info = map[string]string{}
	}
	delete(info, OwnerInfoKey)
	info[ReleasedInfoKey] = bkt.Namespace
	_, err := acct.Client.UpdateBucket(ctx, b2.UpdateBucketRequest{BucketID: observed.BucketID, BucketInfo: &info, IfRevisionIs: observed.Revision})
	switch {
	case b2.HasCode(err, b2.CodeConflict):
		return waitFor(b2v1.ReasonReconciling, time.Second, "bucket changed concurrently")
	case err != nil && !b2.HasCode(err, b2.CodeBadBucketID):
		return providerError("releasing bucket", err)
	}
	r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, EventRetained, "Delete", "Released bucket %s; it and its data remain in B2", bkt.Spec.BucketName)
	return nil
}

// reachedB2 reports whether reconciliation ever got as far as calling B2.
// It errs towards true, which only makes deletion wait for the account.
func reachedB2(bkt *b2v1.Bucket) bool {
	c := meta.FindStatusCondition(bkt.Status.Conditions, b2v1.ConditionReady)
	return c != nil && (c.Status == metav1.ConditionTrue || c.Reason == b2v1.ReasonProviderError || c.Reason == b2v1.ReasonReconciling)
}

// SetupWithManager registers the controller and its watches.
func (r *BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	indexer := mgr.GetFieldIndexer()
	if err := indexer.IndexField(context.Background(), &b2v1.Bucket{}, indexProviderConfig, func(o client.Object) []string {
		return []string{o.(*b2v1.Bucket).Spec.ProviderConfigRef.NameOrDefault()}
	}); err != nil {
		return err
	}
	if err := indexer.IndexField(context.Background(), &b2v1.Bucket{}, indexReplicationDest, func(o client.Object) []string {
		rules := o.(*b2v1.Bucket).Spec.Replication
		dests := make([]string, 0, len(rules))
		for _, rule := range rules {
			dests = append(dests, rule.DestinationBucketRef.Name)
		}
		return dests
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.Bucket{}, builder.WithPredicates(specOrDeletionChanged())).
		Watches(&b2v1.Bucket{}, handler.EnqueueRequestsFromMapFunc(r.replicationPeers), builder.WithPredicates(bucketChanged())).
		Watches(&b2v1.ApplicationKey{}, handler.EnqueueRequestsFromMapFunc(referencedBucket), builder.WithPredicates(onlyDeletes())).
		Watches(&b2v1.ClusterProviderConfig{}, handler.EnqueueRequestsFromMapFunc(r.bucketsUsingProviderConfig), builder.WithPredicates(readinessChanged())).
		Watches(&b2v1.B2AccessPolicy{}, handler.EnqueueRequestsFromMapFunc(r.allBuckets)).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.bucketsInNamespace), builder.WithPredicates(predicate.LabelChangedPredicate{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Named("bucket").
		Complete(r)
}

// referencedBucket maps a deleted ApplicationKey to its Bucket, which may be
// waiting for its keys to go before it can be deleted.
func referencedBucket(_ context.Context, o client.Object) []reconcile.Request {
	k, ok := o.(*b2v1.ApplicationKey)
	if !ok || k.Spec.BucketRef == nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: k.Namespace, Name: k.Spec.BucketRef.Name}}}
}

// replicationPeers maps a Bucket to the buckets it replicates (or used to
// replicate) to, and to the buckets replicating into it.
func (r *BucketReconciler) replicationPeers(ctx context.Context, o client.Object) []reconcile.Request {
	bkt, ok := o.(*b2v1.Bucket)
	if !ok {
		return nil
	}
	peer := func(name string) reconcile.Request {
		return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: bkt.Namespace, Name: name}}
	}
	var out []reconcile.Request
	for _, rule := range bkt.Spec.Replication {
		out = append(out, peer(rule.DestinationBucketRef.Name))
	}
	if rs := bkt.Status.Replication; rs != nil {
		for _, d := range rs.Destinations {
			out = append(out, peer(d.Bucket))
		}
	}
	return append(out, listRequests(ctx, r.Client, &b2v1.BucketList{}, client.InNamespace(bkt.Namespace), client.MatchingFields{indexReplicationDest: bkt.Name})...)
}

func (r *BucketReconciler) bucketsUsingProviderConfig(ctx context.Context, o client.Object) []reconcile.Request {
	return listRequests(ctx, r.Client, &b2v1.BucketList{}, client.MatchingFields{indexProviderConfig: o.GetName()})
}

func (r *BucketReconciler) allBuckets(ctx context.Context, _ client.Object) []reconcile.Request {
	return listRequests(ctx, r.Client, &b2v1.BucketList{})
}

func (r *BucketReconciler) bucketsInNamespace(ctx context.Context, o client.Object) []reconcile.Request {
	return listRequests(ctx, r.Client, &b2v1.BucketList{}, client.InNamespace(o.GetName()))
}
