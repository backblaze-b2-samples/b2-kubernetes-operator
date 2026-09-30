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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
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
// B2 is treated as the source of observed state on every pass: the bucket is
// read from B2, compared with the spec, and corrected in a single
// revision-checked update. Ownership is recorded in the bucket's own
// bucketInfo, so the operator never takes over or deletes a bucket it did
// not create unless adoption was explicitly requested and allowed.
type BucketReconciler struct {
	Deps
}

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
	if !controllerutil.ContainsFinalizer(&bkt, Finalizer) {
		base := bkt.DeepCopy()
		controllerutil.AddFinalizer(&bkt, Finalizer)
		if err := r.Client.Patch(ctx, &bkt, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner)); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}

	orig := bkt.DeepCopy()
	res, err := r.reconcile(ctx, &bkt)
	bkt.Status.ObservedGeneration = bkt.Generation
	if perr := patchStatus(ctx, r.Client, &bkt, orig); perr != nil {
		if apierrors.IsConflict(perr) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, client.IgnoreNotFound(perr)
	}
	return res, err
}

func (r *BucketReconciler) reconcile(ctx context.Context, bkt *b2v1.Bucket) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	prev := meta.FindStatusCondition(bkt.Status.Conditions, b2v1.ConditionReady)
	setReady := func(reason, msg string) {
		if prev == nil || prev.Reason != reason || prev.Message != msg {
			r.Recorder.Eventf(bkt, nil, eventType(reason), reason, "Reconcile", "%s", msg)
		}
		setCondition(&bkt.Status.Conditions, bkt.Generation, metav1.ConditionFalse, reason, msg)
	}

	acct, se := r.resolveAccount(ctx, bkt.Spec.ProviderConfigRef.ProviderConfigName())
	if se != nil {
		return result(se, setReady)
	}
	if se := r.checkPolicy(ctx, bkt, false); se != nil {
		return result(se, setReady)
	}

	observed, justCreated, adopting, se := r.observeOrCreate(ctx, acct, bkt)
	if se != nil {
		return result(se, setReady)
	}

	upd, changed, warnings := diffBucket(bkt, observed)
	if len(changed) > 0 {
		updated, err := acct.Client.UpdateBucket(ctx, upd)
		if err != nil {
			if b2.HasCode(err, b2.CodeConflict) {
				// Changed concurrently in B2; re-read and try again.
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return result(providerError("updating bucket", err), setReady)
		}
		observed = updated
		switch {
		case justCreated:
		case adopting:
			r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, "Adopted", "Adopt", "Adopted existing bucket %s and applied %s", bkt.Spec.BucketName, strings.Join(changed, ", "))
		case bkt.Status.BucketID != "" && bkt.Status.ObservedGeneration == bkt.Generation:
			logger.Info("corrected drift", "fields", changed)
			r.Recorder.Eventf(bkt, nil, corev1.EventTypeWarning, "DriftCorrected", "Update", "Bucket %s was changed outside Kubernetes; restored %s", bkt.Spec.BucketName, strings.Join(changed, ", "))
		default:
			r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, "Updated", "Update", "Updated %s", strings.Join(changed, ", "))
		}
	}

	bkt.Status.BucketID = observed.BucketID
	bkt.Status.S3Endpoint = acct.S3Endpoint
	bkt.Status.S3Region = acct.S3Region
	bkt.Status.ObjectLockEnabled = fileLockEnabled(observed)

	observed, se = r.reconcileReplication(ctx, acct, bkt, observed)
	if se != nil {
		return result(se, setReady)
	}

	now := metav1.NewTime(r.now())
	bkt.Status.Revision = int64(observed.Revision)
	bkt.Status.LastSyncTime = &now
	msg := "Bucket is in sync with B2"
	if len(warnings) > 0 {
		msg += "; " + strings.Join(warnings, "; ")
	}
	setCondition(&bkt.Status.Conditions, bkt.Generation, metav1.ConditionTrue, b2v1.ReasonReconciled, msg)
	return ctrl.Result{RequeueAfter: r.Options.ResyncPeriod}, nil
}

func (r *BucketReconciler) checkPolicy(ctx context.Context, bkt *b2v1.Bucket, adopt bool) *stageError {
	d, err := r.Policy.CheckBucket(ctx, bucketPolicyRequest(bkt, adopt))
	if err != nil {
		return &stageError{reason: b2v1.ReasonPolicyDenied, message: err.Error(), err: err}
	}
	if !d.Allowed {
		// Policy and namespace changes re-trigger reconciliation.
		return &stageError{reason: b2v1.ReasonPolicyDenied, message: d.Reason}
	}
	return nil
}

func bucketPolicyRequest(bkt *b2v1.Bucket, adopt bool) policy.BucketRequest {
	return policy.BucketRequest{
		Namespace:      bkt.Namespace,
		ProviderConfig: bkt.Spec.ProviderConfigRef.ProviderConfigName(),
		BucketName:     bkt.Spec.BucketName,
		Public:         bkt.Spec.BucketType == b2v1.BucketTypeAllPublic,
		Adopt:          adopt,
		Delete:         bkt.Spec.DeletionPolicy == b2v1.DeletionPolicyDelete,
		ComplianceRetention: bkt.Spec.ObjectLock != nil && bkt.Spec.ObjectLock.DefaultRetention != nil &&
			bkt.Spec.ObjectLock.DefaultRetention.Mode == b2v1.RetentionModeCompliance,
		Unencrypted: bkt.Spec.DefaultEncryption != nil && bkt.Spec.DefaultEncryption.Mode == b2v1.EncryptionModeNone,
		Replication: len(bkt.Spec.Replication) > 0,
	}
}

// observeOrCreate finds the bucket in B2, verifying this resource owns it
// (adopting it if requested and allowed), or creates it.
func (r *BucketReconciler) observeOrCreate(ctx context.Context, acct *provider.Account, bkt *b2v1.Bucket) (observed *b2.Bucket, created, adopting bool, se *stageError) {
	var err error
	if bkt.Status.BucketID != "" {
		observed, err = acct.Client.GetBucketByID(ctx, bkt.Status.BucketID)
		if err != nil {
			return nil, false, false, providerError("reading bucket", err)
		}
		if observed == nil {
			r.Recorder.Eventf(bkt, nil, corev1.EventTypeWarning, "BucketMissing", "Reconcile", "Bucket %s (%s) was deleted outside Kubernetes; recreating it", bkt.Spec.BucketName, bkt.Status.BucketID)
			bkt.Status.BucketID = ""
		}
	}
	if observed == nil {
		observed, err = acct.Client.GetBucketByName(ctx, bkt.Spec.BucketName)
		if err != nil {
			return nil, false, false, providerError("looking up bucket", err)
		}
	}

	if observed != nil {
		switch owner := observed.BucketInfo[OwnerInfoKey]; {
		case owner == string(bkt.UID):
			return observed, false, false, nil
		case owner != "":
			return nil, false, false, waitFor(b2v1.ReasonBucketOwnedElsewhere, 10*time.Minute,
				"bucket %q is managed by another resource (owner UID %s); remove bucketInfo key %q from the bucket to release it",
				bkt.Spec.BucketName, owner, OwnerInfoKey)
		case observed.BucketInfo[ReleasedInfoKey] != "" && observed.BucketInfo[ReleasedInfoKey] != bkt.Namespace:
			return nil, false, false, waitFor(b2v1.ReasonBucketOwnedElsewhere, 10*time.Minute,
				"bucket %q was released by namespace %s; a cluster administrator must remove bucketInfo key %q before another namespace can adopt it",
				bkt.Spec.BucketName, observed.BucketInfo[ReleasedInfoKey], ReleasedInfoKey)
		case !bkt.Spec.AdoptExisting:
			return nil, false, false, waitFor(b2v1.ReasonBucketExists, 10*time.Minute,
				"bucket %q already exists in the account; set spec.adoptExisting: true to manage it", bkt.Spec.BucketName)
		}
		if se := r.checkPolicy(ctx, bkt, true); se != nil {
			return nil, false, false, se
		}
		return observed, false, true, nil
	}

	req := b2.CreateBucketRequest{
		BucketName:     bkt.Spec.BucketName,
		BucketType:     bucketType(bkt),
		BucketInfo:     desiredInfo(bkt),
		LifecycleRules: desiredLifecycle(bkt),
		CORSRules:      desiredCORS(bkt),
	}
	if ol := bkt.Spec.ObjectLock; ol != nil && ol.Enabled {
		req.FileLockEnabled = true
	}
	if e := bkt.Spec.DefaultEncryption; e != nil && e.Mode == b2v1.EncryptionModeSSEB2 {
		req.DefaultServerSideEncryption = &b2.ServerSideEncryption{Mode: b2.Ptr(b2.SSEModeB2), Algorithm: b2.Ptr(b2.SSEAlgorithmAES)}
	}
	observed, err = acct.Client.CreateBucket(ctx, req)
	switch {
	case b2.HasCode(err, b2.CodeDuplicateBucketName):
		return nil, false, false, waitFor(b2v1.ReasonBucketNameUnavailable, 10*time.Minute,
			"bucket name %q is already taken by another B2 account; bucket names are global, so choose another", bkt.Spec.BucketName)
	case b2.HasCode(err, b2.CodeTooManyBuckets):
		return nil, false, false, waitFor(b2v1.ReasonProviderError, 10*time.Minute, "the B2 account has reached its bucket limit")
	case err != nil:
		return nil, false, false, providerError("creating bucket", err)
	}
	log.FromContext(ctx).Info("created bucket", "bucket", observed.BucketName, "bucketID", observed.BucketID)
	r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, "Created", "Create", "Created bucket %s (%s)", observed.BucketName, observed.BucketID)
	return observed, true, false, nil
}

// diffBucket builds the update that brings observed to the spec. It returns
// the names of changed fields and warnings about differences that cannot be
// corrected.
func diffBucket(bkt *b2v1.Bucket, observed *b2.Bucket) (b2.UpdateBucketRequest, []string, []string) {
	upd := b2.UpdateBucketRequest{BucketID: observed.BucketID, IfRevisionIs: observed.Revision}
	var changed, warnings []string

	if want := bucketType(bkt); observed.BucketType != want {
		upd.BucketType = want
		changed = append(changed, "bucketType")
	}
	if want := desiredInfo(bkt); !maps.Equal(observed.BucketInfo, want) {
		upd.BucketInfo = &want
		changed = append(changed, "bucketInfo")
	}
	if want := desiredLifecycle(bkt); !reflect.DeepEqual(normalizeLifecycle(observed.LifecycleRules), normalizeLifecycle(want)) {
		rules := want
		if rules == nil {
			rules = []b2.LifecycleRule{}
		}
		upd.LifecycleRules = &rules
		changed = append(changed, "lifecycleRules")
	}
	if want := desiredCORS(bkt); !reflect.DeepEqual(normalizeCORS(observed.CORSRules), normalizeCORS(want)) {
		rules := want
		if rules == nil {
			rules = []b2.CORSRule{}
		}
		upd.CORSRules = &rules
		changed = append(changed, "corsRules")
	}

	if e := bkt.Spec.DefaultEncryption; e != nil {
		sse := observed.DefaultServerSideEncryption
		switch {
		case sse == nil || !sse.IsClientAuthorizedToRead:
			warnings = append(warnings, "cannot read default encryption (operator key lacks readBucketEncryption)")
		default:
			current := ""
			if sse.Value != nil && sse.Value.Mode != nil {
				current = *sse.Value.Mode
			}
			if e.Mode == b2v1.EncryptionModeSSEB2 && current != b2.SSEModeB2 {
				upd.DefaultServerSideEncryption = &b2.ServerSideEncryption{Mode: b2.Ptr(b2.SSEModeB2), Algorithm: b2.Ptr(b2.SSEAlgorithmAES)}
				changed = append(changed, "defaultEncryption")
			} else if e.Mode == b2v1.EncryptionModeNone && current != "" {
				upd.DefaultServerSideEncryption = &b2.ServerSideEncryption{}
				changed = append(changed, "defaultEncryption")
			}
		}
	}

	if ol := bkt.Spec.ObjectLock; ol != nil {
		fl := observed.FileLockConfiguration
		switch {
		case fl == nil || !fl.IsClientAuthorizedToRead:
			warnings = append(warnings, "cannot read Object Lock settings (operator key lacks readBucketRetentions)")
		default:
			enabled := fl.Value != nil && fl.Value.IsFileLockEnabled
			if ol.Enabled && !enabled {
				upd.FileLockEnabled = b2.Ptr(true)
				changed = append(changed, "objectLock.enabled")
				enabled = true
			}
			if !ol.Enabled && enabled {
				warnings = append(warnings, "Object Lock is enabled in B2 and cannot be disabled")
			}
			if enabled && ol.Enabled {
				var current *b2.DefaultRetention
				if fl.Value != nil {
					current = fl.Value.DefaultRetention
				}
				if want := desiredRetention(ol); !retentionEqual(current, want) {
					upd.DefaultRetention = want
					changed = append(changed, "objectLock.defaultRetention")
				}
			}
		}
	}
	return upd, changed, warnings
}

func (r *BucketReconciler) finalize(ctx context.Context, bkt *b2v1.Bucket) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(bkt, Finalizer) {
		return ctrl.Result{}, nil
	}
	logger := log.FromContext(ctx)
	orig := bkt.DeepCopy()
	setReady := func(reason, msg string) {
		setCondition(&bkt.Status.Conditions, bkt.Generation, metav1.ConditionFalse, reason, msg)
	}
	fail := func(se *stageError) (ctrl.Result, error) {
		res, err := result(se, setReady)
		if perr := patchStatus(ctx, r.Client, bkt, orig); perr != nil && !apierrors.IsNotFound(perr) {
			logger.Error(perr, "updating status during deletion")
		}
		return res, err
	}

	acct, se := r.resolveAccount(ctx, bkt.Spec.ProviderConfigRef.ProviderConfigName())
	if se != nil {
		if bkt.Status.BucketID == "" && !everReady(bkt.Status.Conditions) {
			// Nothing was ever created in B2 for this resource (for
			// example it was denied by policy), so there is nothing to
			// clean up and no reason to block deletion on the account.
			return r.removeFinalizer(ctx, bkt)
		}
		se.message = "cannot delete: " + se.message + " (remove the finalizer to abandon the bucket)"
		return fail(se)
	}

	var observed *b2.Bucket
	var err error
	if bkt.Status.BucketID != "" {
		observed, err = acct.Client.GetBucketByID(ctx, bkt.Status.BucketID)
	} else {
		// The bucket may have been created without status being recorded.
		observed, err = acct.Client.GetBucketByName(ctx, bkt.Spec.BucketName)
	}
	if err != nil {
		return fail(providerError("reading bucket", err))
	}

	if observed != nil && observed.BucketInfo[OwnerInfoKey] == string(bkt.UID) {
		// Keys scoped to the bucket are revoked before it is released or
		// deleted, so no credentials outlive the resource that granted them.
		var keys b2v1.ApplicationKeyList
		if err := r.Client.List(ctx, &keys, client.InNamespace(bkt.Namespace), client.MatchingFields{indexBucketRef: bkt.Name}); err != nil {
			return ctrl.Result{}, err
		}
		// Only keys that hold (or may hold) credentials block deletion; one
		// that was never issued, e.g. denied by policy, has nothing to revoke.
		live := slices.DeleteFunc(keys.Items, func(k b2v1.ApplicationKey) bool {
			return k.Status.KeyID == "" && k.Status.PendingKeyName == "" && len(k.Status.RetiringKeys) == 0
		})
		if n := len(live); n > 0 {
			return fail(waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
				"%d ApplicationKey(s) with live credentials still reference this bucket (e.g. %s); delete them first", n, live[0].Name))
		}
		var sources b2v1.BucketList
		if err := r.Client.List(ctx, &sources, client.InNamespace(bkt.Namespace), client.MatchingFields{indexReplicationDest: bkt.Name}); err != nil {
			return ctrl.Result{}, err
		}
		if n := len(sources.Items); n > 0 {
			return fail(waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
				"%d Bucket(s) replicate into this bucket (e.g. %s); remove their replication rules first", n, sources.Items[0].Name))
		}
		// Stop replicating from this bucket and revoke its replication keys.
		teardown := bkt.DeepCopy()
		teardown.Spec.Replication = nil
		updated, se := r.reconcileReplication(ctx, acct, teardown, observed)
		if se != nil {
			return fail(se)
		}
		bkt.Status.Replication = teardown.Status.Replication
		observed = updated
		deleteBucket := bkt.Spec.DeletionPolicy == b2v1.DeletionPolicyDelete
		if deleteBucket {
			if se := r.checkPolicy(ctx, bkt, false); se != nil {
				r.Recorder.Eventf(bkt, nil, corev1.EventTypeWarning, b2v1.ReasonPolicyDenied, "Delete", "Not deleting bucket %s: %s; it is retained instead", bkt.Spec.BucketName, se.message)
				deleteBucket = false
			}
		}
		if deleteBucket {
			if err := acct.Client.DeleteBucket(ctx, observed.BucketID); err != nil {
				if b2.HasCode(err, b2.CodeCannotDeleteNonEmptyBucket) {
					return fail(waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
						"bucket %s is not empty; B2 only deletes empty buckets. Delete all file versions, or set deletionPolicy: Retain", bkt.Spec.BucketName))
				}
				return fail(providerError("deleting bucket", err))
			}
			r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, "Deleted", "Delete", "Deleted bucket %s", bkt.Spec.BucketName)
		} else {
			info := maps.Clone(observed.BucketInfo)
			if info == nil {
				info = map[string]string{}
			}
			delete(info, OwnerInfoKey)
			info[ReleasedInfoKey] = bkt.Namespace
			_, err := acct.Client.UpdateBucket(ctx, b2.UpdateBucketRequest{BucketID: observed.BucketID, BucketInfo: &info, IfRevisionIs: observed.Revision})
			if err != nil && !b2.HasCode(err, b2.CodeBadBucketID) {
				if b2.HasCode(err, b2.CodeConflict) {
					return ctrl.Result{RequeueAfter: time.Second}, nil
				}
				return fail(providerError("releasing bucket", err))
			}
			r.Recorder.Eventf(bkt, nil, corev1.EventTypeNormal, "Retained", "Delete", "Released bucket %s; it and its data remain in B2", bkt.Spec.BucketName)
		}
	}

	return r.removeFinalizer(ctx, bkt)
}

func (r *BucketReconciler) removeFinalizer(ctx context.Context, bkt *b2v1.Bucket) (ctrl.Result, error) {
	base := bkt.DeepCopy()
	controllerutil.RemoveFinalizer(bkt, Finalizer)
	if err := r.Client.Patch(ctx, bkt, client.MergeFrom(base), client.FieldOwner(FieldOwner)); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
}

// everReady reports whether the resource has ever been reconciled with B2.
// A Ready condition that was True at any point leaves a BucketID behind, so
// this only needs to distinguish "never got as far as B2".
func everReady(conds []metav1.Condition) bool {
	c := meta.FindStatusCondition(conds, b2v1.ConditionReady)
	return c != nil && (c.Status == metav1.ConditionTrue || c.Reason == b2v1.ReasonProviderError || c.Reason == b2v1.ReasonReconciling)
}

func bucketType(bkt *b2v1.Bucket) string {
	if bkt.Spec.BucketType == b2v1.BucketTypeAllPublic {
		return b2.BucketTypeAllPublic
	}
	return b2.BucketTypeAllPrivate
}

func desiredInfo(bkt *b2v1.Bucket) map[string]string {
	info := make(map[string]string, len(bkt.Spec.BucketInfo)+1)
	for k, v := range bkt.Spec.BucketInfo {
		info[strings.ToLower(k)] = v
	}
	info[OwnerInfoKey] = string(bkt.UID)
	return info
}

func desiredLifecycle(bkt *b2v1.Bucket) []b2.LifecycleRule {
	out := make([]b2.LifecycleRule, 0, len(bkt.Spec.LifecycleRules))
	for _, r := range bkt.Spec.LifecycleRules {
		out = append(out, b2.LifecycleRule{
			FileNamePrefix:                                  r.FileNamePrefix,
			DaysFromUploadingToHiding:                       r.DaysFromUploadingToHiding,
			DaysFromHidingToDeleting:                        r.DaysFromHidingToDeleting,
			DaysFromStartingToCancelingUnfinishedLargeFiles: r.DaysFromStartingToCancelingUnfinishedLargeFiles,
		})
	}
	return out
}

func desiredCORS(bkt *b2v1.Bucket) []b2.CORSRule {
	out := make([]b2.CORSRule, 0, len(bkt.Spec.CORSRules))
	for _, r := range bkt.Spec.CORSRules {
		ops := make([]string, 0, len(r.AllowedOperations))
		for _, op := range r.AllowedOperations {
			ops = append(ops, string(op))
		}
		out = append(out, b2.CORSRule{
			CORSRuleName:      r.Name,
			AllowedOrigins:    r.AllowedOrigins,
			AllowedOperations: ops,
			AllowedHeaders:    r.AllowedHeaders,
			ExposeHeaders:     r.ExposeHeaders,
			MaxAgeSeconds:     r.MaxAgeSeconds,
		})
	}
	return out
}

func desiredRetention(ol *b2v1.ObjectLock) *b2.DefaultRetention {
	if ol.DefaultRetention == nil {
		return &b2.DefaultRetention{}
	}
	return &b2.DefaultRetention{
		Mode:   b2.Ptr(string(ol.DefaultRetention.Mode)),
		Period: &b2.RetentionPeriod{Duration: ol.DefaultRetention.Duration, Unit: string(ol.DefaultRetention.Unit)},
	}
}

func retentionEqual(a, b *b2.DefaultRetention) bool {
	mode := func(r *b2.DefaultRetention) string {
		if r == nil || r.Mode == nil {
			return ""
		}
		return *r.Mode
	}
	if mode(a) != mode(b) {
		return false
	}
	if mode(a) == "" {
		return true
	}
	return reflect.DeepEqual(a.Period, b.Period)
}

func fileLockEnabled(b *b2.Bucket) bool {
	fl := b.FileLockConfiguration
	return fl != nil && fl.Value != nil && fl.Value.IsFileLockEnabled
}

func normalizeLifecycle(rules []b2.LifecycleRule) []b2.LifecycleRule {
	if len(rules) == 0 {
		return nil
	}
	out := slices.Clone(rules)
	key := func(r b2.LifecycleRule) string {
		d := func(p *int32) string {
			if p == nil {
				return "-"
			}
			return fmt.Sprint(*p)
		}
		return r.FileNamePrefix + "\x00" + d(r.DaysFromUploadingToHiding) + d(r.DaysFromHidingToDeleting) + d(r.DaysFromStartingToCancelingUnfinishedLargeFiles)
	}
	slices.SortFunc(out, func(a, b b2.LifecycleRule) int { return cmp.Compare(key(a), key(b)) })
	return out
}

func normalizeCORS(rules []b2.CORSRule) []b2.CORSRule {
	if len(rules) == 0 {
		return nil
	}
	norm := func(s []string) []string {
		if len(s) == 0 {
			return nil
		}
		return slices.Sorted(slices.Values(s))
	}
	out := make([]b2.CORSRule, 0, len(rules))
	for _, r := range rules {
		r.AllowedOrigins = norm(r.AllowedOrigins)
		r.AllowedOperations = norm(r.AllowedOperations)
		r.AllowedHeaders = norm(r.AllowedHeaders)
		r.ExposeHeaders = norm(r.ExposeHeaders)
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b b2.CORSRule) int { return cmp.Compare(a.CORSRuleName, b.CORSRuleName) })
	return out
}

// SetupWithManager registers the controller and its watches.
func (r *BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &b2v1.Bucket{}, indexProviderConfig, func(o client.Object) []string {
		return []string{o.(*b2v1.Bucket).Spec.ProviderConfigRef.ProviderConfigName()}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &b2v1.Bucket{}, indexReplicationDest, func(o client.Object) []string {
		rules := o.(*b2v1.Bucket).Spec.Replication
		out := make([]string, 0, len(rules))
		for _, rule := range rules {
			out = append(out, rule.DestinationBucketRef.Name)
		}
		return out
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.Bucket{}, builder.WithPredicates(specOrDeletionChanged())).
		Watches(&b2v1.ClusterProviderConfig{},
			handler.EnqueueRequestsFromMapFunc(r.bucketsForProviderConfig),
			builder.WithPredicates(readinessChanged())).
		Watches(&b2v1.B2AccessPolicy{}, handler.EnqueueRequestsFromMapFunc(r.allBuckets)).
		Watches(&b2v1.Bucket{}, handler.EnqueueRequestsFromMapFunc(r.replicationPeers),
			builder.WithPredicates(predicate.Or[client.Object](specOrDeletionChanged(), bucketReadinessChanged()))).
		Watches(&b2v1.ApplicationKey{}, handler.EnqueueRequestsFromMapFunc(referencedBucket),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc: func(event.CreateEvent) bool { return false },
				UpdateFunc: func(event.UpdateEvent) bool { return false },
			})).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.bucketsInNamespace),
			builder.WithPredicates(predicate.LabelChangedPredicate{})).
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

// replicationPeers maps a Bucket to the buckets it replicates to (which may
// be waiting for the rule to go before they can be deleted) and the buckets
// replicating into it (which may be waiting for it to become ready).
func (r *BucketReconciler) replicationPeers(ctx context.Context, o client.Object) []reconcile.Request {
	bkt, ok := o.(*b2v1.Bucket)
	if !ok {
		return nil
	}
	var out []reconcile.Request
	for _, rule := range bkt.Spec.Replication {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: bkt.Namespace, Name: rule.DestinationBucketRef.Name}})
	}
	return append(out, r.listRequests(ctx, client.InNamespace(bkt.Namespace), client.MatchingFields{indexReplicationDest: bkt.Name})...)
}

func (r *BucketReconciler) bucketsForProviderConfig(ctx context.Context, o client.Object) []reconcile.Request {
	return r.listRequests(ctx, client.MatchingFields{indexProviderConfig: o.GetName()})
}

func (r *BucketReconciler) allBuckets(ctx context.Context, _ client.Object) []reconcile.Request {
	return r.listRequests(ctx)
}

func (r *BucketReconciler) bucketsInNamespace(ctx context.Context, o client.Object) []reconcile.Request {
	return r.listRequests(ctx, client.InNamespace(o.GetName()))
}

func (r *BucketReconciler) listRequests(ctx context.Context, opts ...client.ListOption) []reconcile.Request {
	var list b2v1.BucketList
	if err := r.Client.List(ctx, &list, opts...); err != nil {
		log.FromContext(ctx).Error(err, "listing Buckets for watch")
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for _, b := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&b)})
	}
	return out
}

// specOrDeletionChanged passes creates, spec changes, annotation changes (a
// manual re-sync trigger) and deletion, but not status-only updates.
func specOrDeletionChanged() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
		predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
			return !e.ObjectNew.GetDeletionTimestamp().IsZero()
		}},
	)
}

// readinessChanged passes provider config events that change readiness.
func readinessChanged() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPC, ok1 := e.ObjectOld.(*b2v1.ClusterProviderConfig)
			newPC, ok2 := e.ObjectNew.(*b2v1.ClusterProviderConfig)
			if !ok1 || !ok2 {
				return true
			}
			return meta.IsStatusConditionTrue(oldPC.Status.Conditions, b2v1.ConditionReady) !=
				meta.IsStatusConditionTrue(newPC.Status.Conditions, b2v1.ConditionReady) ||
				oldPC.Generation != newPC.Generation
		},
	}
}
