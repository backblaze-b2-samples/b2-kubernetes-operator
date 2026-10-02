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
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/policy"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/remote"
)

// ApplicationKeyReconciler creates scoped B2 application keys and delivers
// them to Secrets, locally or in a RemoteCluster. Rotation is in
// applicationkey_rotation.go and Secret handling in applicationkey_secret.go.
type ApplicationKeyReconciler struct {
	Deps
	Remote *remote.Registry
}

// keyScope is the bucket a key is restricted to, if any.
type keyScope struct {
	bucketID   string
	bucketName string
	// external is true for a bucket that is not a Bucket resource in the
	// key's namespace (spec.bucketName).
	external bool
}

// +kubebuilder:rbac:groups=b2.backblaze.com,resources=applicationkeys,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=applicationkeys/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=applicationkeys/finalizers,verbs=update
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=buckets,verbs=get;list;watch

func (r *ApplicationKeyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var key b2v1.ApplicationKey
	if err := r.Client.Get(ctx, req.NamespacedName, &key); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !key.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &key)
	}
	if err := r.ensureFinalizer(ctx, &key); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := key.DeepCopy()
	res, err := r.reconcile(ctx, &key, orig)
	key.Status.ObservedGeneration = key.Generation
	return r.commitStatus(ctx, &key, orig, res, err)
}

// reconcile brings the key to the spec. orig tracks the last persisted
// status, which key creation updates mid-way.
func (r *ApplicationKeyReconciler) reconcile(ctx context.Context, key, orig *b2v1.ApplicationKey) (ctrl.Result, error) {
	notReady := r.notReady(key)
	now := r.now()

	acct, se := r.resolveAccount(ctx, key.Spec.ProviderConfigRef.NameOrDefault())
	if se != nil {
		return result(se, notReady)
	}
	// Policy is evaluated before waiting on the bucket, so a key that is no
	// longer allowed is revoked even if the same change made its bucket
	// unavailable.
	scope, bkt, se := r.scopeFromSpec(ctx, key)
	if se != nil {
		if se.reason == b2v1.ReasonBucketNotFound && key.Status.KeyID != "" {
			return r.denied(ctx, acct, key, se.message, notReady, nil)
		}
		return result(se, notReady)
	}
	d, err := r.Policy.CheckKey(ctx, keyPolicyRequest(key, scope))
	if err != nil {
		return result(&stageError{reason: b2v1.ReasonPolicyDenied, message: err.Error(), err: err}, notReady)
	}
	if !d.Allowed {
		return r.denied(ctx, acct, key, d.Reason, notReady, func() (bool, error) {
			// Confirm against the API server, so cache lag or a policy being
			// replaced cannot cause a revocation.
			d, err := r.Policy.WithReader(r.APIReader).CheckKey(ctx, keyPolicyRequest(key, scope))
			return !d.Allowed, err
		})
	}
	if se := r.resolveBucketID(ctx, acct, key, bkt, &scope); se != nil {
		if se.reason == b2v1.ReasonPolicyDenied {
			return r.denied(ctx, acct, key, se.message, notReady, nil)
		}
		return result(se, notReady)
	}
	if key.Status.ScheduledRevocation != nil {
		r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, EventRevocationCancelled, "Reconcile", "Access was restored; the key will not be revoked")
		key.Status.ScheduledRevocation = nil
	}

	store, se := r.store(ctx, key)
	if se != nil {
		return result(se, notReady)
	}
	sec, se := r.ownedSecret(ctx, key, store)
	if se != nil {
		return result(se, notReady)
	}
	if key.Status.PendingKeyName != "" {
		// Resolving a pending key may revoke keys, so it must not act on a
		// cached status that has not yet recorded a just-created key.
		if fresh, err := r.isFresh(ctx, key); err != nil || !fresh {
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
		if err := r.resolvePendingKey(ctx, acct, key, store); err != nil {
			return result(providerError("cleaning up an interrupted key creation", err), notReady)
		}
	}

	specHash := keySpecHash(key, scope)
	if why := r.needsReplacement(ctx, acct, key, sec, specHash, now); why.needed() {
		if se := r.createAndSwap(ctx, acct, key, orig, store, sec, scope, specHash, why, now); se != nil {
			return result(se, notReady)
		}
	} else if err := r.syncSecret(ctx, acct, key, store, sec, scope); err != nil {
		return result(store.failed(err), notReady)
	}
	key.Status.DeliveredTo = store.where()

	revokeAt, err := r.revokeRetired(ctx, acct, key, now)
	if err != nil {
		log.FromContext(ctx).Error(err, "revoking retired keys; will retry")
	}
	location := "Secret " + key.SecretNameOrDefault()
	if store.remote != "" {
		location = fmt.Sprintf("Secret %s/%s in RemoteCluster %s", store.namespace, key.SecretNameOrDefault(), store.remote)
	}
	markReady(key, fmt.Sprintf("Key %s is current and stored in %s", key.Status.KeyID, location))
	return ctrl.Result{RequeueAfter: r.nextWake(key, now, revokeAt)}, nil
}

// scopeFromSpec determines the bucket name a key is restricted to without
// requiring the bucket to be ready. For a bucketRef it also returns the
// Bucket.
func (r *ApplicationKeyReconciler) scopeFromSpec(ctx context.Context, key *b2v1.ApplicationKey) (keyScope, *b2v1.Bucket, *stageError) {
	switch {
	case key.Spec.BucketRef != nil:
		var bkt b2v1.Bucket
		if err := r.Client.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Spec.BucketRef.Name}, &bkt); err != nil {
			if apierrors.IsNotFound(err) {
				return keyScope{}, nil, waitFor(b2v1.ReasonBucketNotFound, time.Minute, "Bucket %q not found in namespace %s", key.Spec.BucketRef.Name, key.Namespace)
			}
			return keyScope{}, nil, &stageError{reason: b2v1.ReasonBucketNotReady, message: err.Error(), err: err}
		}
		if bkt.Spec.ProviderConfigRef.NameOrDefault() != key.Spec.ProviderConfigRef.NameOrDefault() {
			return keyScope{}, nil, waitFor(b2v1.ReasonInvalidSpec, 5*time.Minute, "Bucket %q uses provider config %q, but the key uses %q",
				bkt.Name, bkt.Spec.ProviderConfigRef.NameOrDefault(), key.Spec.ProviderConfigRef.NameOrDefault())
		}
		return keyScope{bucketName: bkt.Spec.BucketName}, &bkt, nil
	case key.Spec.BucketName != "":
		return keyScope{bucketName: key.Spec.BucketName, external: true}, nil, nil
	}
	return keyScope{}, nil, nil
}

// resolveBucketID fills in the B2 bucket ID, waiting for a referenced Bucket
// to be ready or looking up an external bucket. An external bucket managed
// by (or released from) another namespace is refused with
// ReasonPolicyDenied whatever the name patterns say, because patterns such
// as "acme-{namespace}-*" also match names of namespaces sharing a prefix.
func (r *ApplicationKeyReconciler) resolveBucketID(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, bkt *b2v1.Bucket, scope *keyScope) *stageError {
	switch {
	case bkt != nil:
		if !bkt.DeletionTimestamp.IsZero() || bkt.Status.BucketID == "" || !isReady(bkt) {
			return waitFor(b2v1.ReasonBucketNotReady, time.Minute, "waiting for Bucket %q to be ready", bkt.Name)
		}
		scope.bucketID = bkt.Status.BucketID
	case scope.bucketName != "":
		b, err := acct.Client.GetBucketByName(ctx, scope.bucketName)
		if err != nil {
			return providerError("looking up bucket", err)
		}
		if b == nil {
			return waitFor(b2v1.ReasonBucketNotFound, 5*time.Minute, "bucket %q does not exist in the B2 account", scope.bucketName)
		}
		if releasedBy := b.BucketInfo[ReleasedInfoKey]; releasedBy != "" && releasedBy != key.Namespace {
			return waitFor(b2v1.ReasonPolicyDenied, 10*time.Minute, "bucket %q was released by namespace %s", scope.bucketName, releasedBy)
		}
		if owner := b.BucketInfo[OwnerInfoKey]; owner != "" {
			var local b2v1.BucketList
			if err := r.Client.List(ctx, &local, client.InNamespace(key.Namespace)); err != nil {
				return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
			}
			if !slices.ContainsFunc(local.Items, func(b b2v1.Bucket) bool { return string(b.UID) == owner }) {
				return waitFor(b2v1.ReasonPolicyDenied, 10*time.Minute, "bucket %q is managed by a Bucket outside namespace %s", scope.bucketName, key.Namespace)
			}
		}
		scope.bucketID = b.BucketID
	}
	return nil
}

// denied handles a key that is no longer allowed. The current key is revoked
// (and its Secret deleted) only after its grace period, and only if confirm
// (when set) still reports a denial against uncached data, so a momentary
// gap, such as a policy being replaced, does not break workloads.
func (r *ApplicationKeyReconciler) denied(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, msg string,
	notReady func(string, string), confirm func() (bool, error)) (ctrl.Result, error) {
	if !r.Options.RevokeOnPolicyViolation || key.Status.KeyID == "" {
		return result(&stageError{reason: b2v1.ReasonPolicyDenied, message: msg}, notReady)
	}
	now := r.now()
	if key.Status.ScheduledRevocation == nil {
		at := metav1.NewTime(now.Add(r.gracePeriod(key)))
		key.Status.ScheduledRevocation = &at
		r.Recorder.Eventf(key, nil, corev1.EventTypeWarning, EventRevocationScheduled, "Reconcile",
			"Key %s is no longer allowed and will be revoked at %s unless access is restored: %s", key.Status.KeyID, at.UTC().Format(time.RFC3339), msg)
	}
	at := key.Status.ScheduledRevocation.Time
	if now.Before(at) {
		return result(waitFor(b2v1.ReasonPolicyDenied, max(at.Sub(now), time.Second),
			"%s; key %s will be revoked at %s unless access is restored", msg, key.Status.KeyID, at.UTC().Format(time.RFC3339)), notReady)
	}
	if confirm != nil {
		stillDenied, err := confirm()
		if err != nil {
			return ctrl.Result{}, err
		}
		if !stillDenied {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}
	revoked := key.Status.KeyID
	if err := r.revokeAll(ctx, acct, key); err != nil {
		return result(providerError("revoking a key that is no longer allowed", err), notReady)
	}
	if err := r.deleteOwnedSecret(ctx, key); err != nil {
		return ctrl.Result{}, err
	}
	key.Status.ScheduledRevocation = nil
	r.Recorder.Eventf(key, nil, corev1.EventTypeWarning, EventKeyRevoked, "Revoke", "Revoked key %s: %s", revoked, msg)
	return result(&stageError{reason: b2v1.ReasonPolicyDenied, message: msg}, notReady)
}

// isFresh reports whether key matches the latest version in the API server.
func (r *ApplicationKeyReconciler) isFresh(ctx context.Context, key *b2v1.ApplicationKey) (bool, error) {
	var live b2v1.ApplicationKey
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(key), &live); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return live.ResourceVersion == key.ResourceVersion, nil
}

func keyPolicyRequest(key *b2v1.ApplicationKey, scope keyScope) policy.KeyRequest {
	req := policy.KeyRequest{
		Namespace:      key.Namespace,
		ProviderConfig: key.Spec.ProviderConfigRef.NameOrDefault(),
		BucketName:     scope.bucketName,
		External:       scope.external,
		Capabilities:   key.Spec.Capabilities,
		DeliverTo:      key.Spec.DeliverTo,
	}
	if key.Spec.ValidFor != nil {
		req.ValidFor = key.Spec.ValidFor.Duration
	}
	return req
}

// finalize revokes every key the resource holds in B2, then deletes its
// Secret.
func (r *ApplicationKeyReconciler) finalize(ctx context.Context, key *b2v1.ApplicationKey) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(key, Finalizer) {
		return ctrl.Result{}, nil
	}
	orig := key.DeepCopy()
	fail := func(se *stageError) (ctrl.Result, error) { return r.failDeletion(ctx, key, orig, se) }

	if key.Status.KeyID != "" || key.Status.PendingKeyName != "" || len(key.Status.RetiringKeys) > 0 {
		acct, se := r.resolveAccount(ctx, key.Spec.ProviderConfigRef.NameOrDefault())
		if se != nil {
			se.message = "cannot revoke key: " + se.message + " (remove the finalizer to abandon the key in B2)"
			return fail(se)
		}
		if err := r.revokeAll(ctx, acct, key); err != nil {
			return fail(providerError("revoking key", err))
		}
	}
	if err := r.deleteOwnedSecret(ctx, key); err != nil {
		var se *stageError
		if errors.As(err, &se) {
			se.message = "cannot delete the Secret: " + se.message + " (remove the finalizer to leave it behind)"
			return fail(se)
		}
		return ctrl.Result{}, err
	}
	return r.releaseFinalizer(ctx, key)
}

// SetupWithManager registers the controller and its watches.
func (r *ApplicationKeyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	indexes := map[string]func(*b2v1.ApplicationKey) []string{
		indexProviderConfig: func(k *b2v1.ApplicationKey) []string { return []string{k.Spec.ProviderConfigRef.NameOrDefault()} },
		indexBucketRef: func(k *b2v1.ApplicationKey) []string {
			if k.Spec.BucketRef == nil {
				return nil
			}
			return []string{k.Spec.BucketRef.Name}
		},
		indexRemoteCluster: func(k *b2v1.ApplicationKey) []string {
			if k.Spec.DeliverTo == nil {
				return nil
			}
			return []string{k.Spec.DeliverTo.RemoteCluster}
		},
	}
	for field, extract := range indexes {
		if err := mgr.GetFieldIndexer().IndexField(context.Background(), &b2v1.ApplicationKey{}, field, func(o client.Object) []string {
			return extract(o.(*b2v1.ApplicationKey))
		}); err != nil {
			return err
		}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.ApplicationKey{}, builder.WithPredicates(specOrDeletionChanged())).
		Owns(&corev1.Secret{}).
		Watches(&b2v1.Bucket{}, handler.EnqueueRequestsFromMapFunc(r.keysForBucket), builder.WithPredicates(bucketChanged())).
		Watches(&b2v1.ClusterProviderConfig{}, handler.EnqueueRequestsFromMapFunc(r.keysMatching(indexProviderConfig)), builder.WithPredicates(readinessChanged())).
		Watches(&b2v1.RemoteCluster{}, handler.EnqueueRequestsFromMapFunc(r.keysMatching(indexRemoteCluster)), builder.WithPredicates(readinessChanged())).
		Watches(&b2v1.B2AccessPolicy{}, handler.EnqueueRequestsFromMapFunc(r.allKeys)).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.keysInNamespace), builder.WithPredicates(predicate.LabelChangedPredicate{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Named("applicationkey").
		Complete(r)
}

func (r *ApplicationKeyReconciler) keysForBucket(ctx context.Context, o client.Object) []reconcile.Request {
	return listRequests(ctx, r.Client, &b2v1.ApplicationKeyList{}, client.InNamespace(o.GetNamespace()), client.MatchingFields{indexBucketRef: o.GetName()})
}

// keysMatching maps a cluster-scoped object to the keys whose index field
// names it.
func (r *ApplicationKeyReconciler) keysMatching(field string) handler.MapFunc {
	return func(ctx context.Context, o client.Object) []reconcile.Request {
		return listRequests(ctx, r.Client, &b2v1.ApplicationKeyList{}, client.MatchingFields{field: o.GetName()})
	}
}

func (r *ApplicationKeyReconciler) allKeys(ctx context.Context, _ client.Object) []reconcile.Request {
	return listRequests(ctx, r.Client, &b2v1.ApplicationKeyList{})
}

func (r *ApplicationKeyReconciler) keysInNamespace(ctx context.Context, o client.Object) []reconcile.Request {
	return listRequests(ctx, r.Client, &b2v1.ApplicationKeyList{}, client.InNamespace(o.GetName()))
}
