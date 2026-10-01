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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/remote"
)

const (
	annotationKeyID     = "b2.backblaze.com/key-id"
	annotationExpiresAt = "b2.backblaze.com/expires-at"
	annotationSpecHash  = "b2.backblaze.com/spec-hash"
	annotationCreatedAt = "b2.backblaze.com/created-at"
	labelApplicationKey = "b2.backblaze.com/application-key"

	defaultGracePeriod = 15 * time.Minute
	maxRenewBefore     = 7 * 24 * time.Hour
)

var invalidKeyNameChars = regexp.MustCompile(`[^A-Za-z0-9-]+`)

// ApplicationKeyReconciler creates scoped B2 application keys and delivers
// them to Secrets.
//
// A key's secret half is only returned by B2 once, at creation, so the
// Secret is the only copy. Whenever a new key is needed (spec change, lost
// Secret, rotation, approaching expiry, or revocation outside Kubernetes)
// the operator creates the new key, updates the Secret, and only then
// schedules the old key for revocation after a grace period. Key creation is
// bracketed by status.pendingKeyName so that a key orphaned by a crash
// between creation and recording can be found and revoked.
type ApplicationKeyReconciler struct {
	Deps
	// APIReader reads Secrets that are not in the operator's label-filtered
	// cache, to avoid overwriting Secrets the operator does not own.
	APIReader client.Reader
	// Remote builds clients for RemoteClusters (spec.deliverTo). Nil
	// disables remote delivery.
	Remote *remote.Registry
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
	if !controllerutil.ContainsFinalizer(&key, Finalizer) {
		base := key.DeepCopy()
		controllerutil.AddFinalizer(&key, Finalizer)
		if err := r.Client.Patch(ctx, &key, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner)); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}

	orig := key.DeepCopy()
	res, err := r.reconcile(ctx, &key, &orig)
	key.Status.ObservedGeneration = key.Generation
	if perr := patchStatus(ctx, r.Client, &key, orig); perr != nil {
		if apierrors.IsConflict(perr) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, client.IgnoreNotFound(perr)
	}
	return res, err
}

// keyTarget is the resolved scope of a key.
type keyTarget struct {
	bucketID   string
	bucketName string
	external   bool
}

func (r *ApplicationKeyReconciler) reconcile(ctx context.Context, key *b2v1.ApplicationKey, orig **b2v1.ApplicationKey) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	prev := meta.FindStatusCondition(key.Status.Conditions, b2v1.ConditionReady)
	setReady := func(reason, msg string) {
		if prev == nil || prev.Reason != reason || prev.Message != msg {
			r.Recorder.Eventf(key, nil, eventType(reason), reason, "Reconcile", "%s", msg)
		}
		setCondition(&key.Status.Conditions, key.Generation, metav1.ConditionFalse, reason, msg)
	}
	now := r.now()

	acct, se := r.resolveAccount(ctx, key.Spec.ProviderConfigRef.ProviderConfigName())
	if se != nil {
		return result(se, setReady)
	}
	// Policy is evaluated from the spec before waiting on the bucket, so a
	// key that is no longer allowed is revoked even if its bucket is not
	// ready (for example because the same policy change denied the bucket).
	target, bkt, se := r.targetFromSpec(ctx, key)
	if se != nil {
		if se.reason == b2v1.ReasonBucketNotFound && key.Status.KeyID != "" {
			return r.denied(ctx, acct, key, se.message, setReady, nil)
		}
		return result(se, setReady)
	}

	d, err := r.Policy.CheckKey(ctx, keyPolicyRequest(key, target))
	if err != nil {
		return result(&stageError{reason: b2v1.ReasonPolicyDenied, message: err.Error(), err: err}, setReady)
	}
	if !d.Allowed {
		return r.denied(ctx, acct, key, d.Reason, setReady, func() (bool, error) {
			// Confirm against the API server so cache lag or a policy being
			// replaced cannot revoke keys.
			fresh := &policy.Evaluator{Reader: r.APIReader, AllowKeyManagement: r.Policy.AllowKeyManagement}
			d, err := fresh.CheckKey(ctx, keyPolicyRequest(key, target))
			return !d.Allowed, err
		})
	}
	if se := r.resolveBucketID(ctx, acct, key, bkt, &target); se != nil {
		if se.reason == b2v1.ReasonPolicyDenied {
			return r.denied(ctx, acct, key, se.message, setReady, nil)
		}
		return result(se, setReady)
	}
	if key.Status.ScheduledRevocation != nil {
		r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, "RevocationCancelled", "Reconcile", "Access was restored; the key will not be revoked")
		key.Status.ScheduledRevocation = nil
	}

	store, se := r.store(ctx, key)
	if se != nil {
		return result(se, setReady)
	}
	secretName := key.SecretNameOrDefault()
	secret, se := r.getOwnedSecret(ctx, key, store, secretName)
	if se != nil {
		return result(se, setReady)
	}

	if key.Status.PendingKeyName != "" {
		// Orphan cleanup revokes keys, so it must not act on a stale cached
		// status that has not yet recorded a just-created key.
		if fresh, err := r.isFresh(ctx, key); err != nil || !fresh {
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
		if err := r.revokeOrphans(ctx, acct, key, store); err != nil {
			return result(providerError("cleaning up an interrupted key creation", err), setReady)
		}
	}

	specHash := keySpecHash(key, target)
	why := r.replacementReason(ctx, acct, key, secret, specHash, now)
	if why != "" {
		if se := r.createAndSwap(ctx, acct, key, orig, store, secret, target, specHash, why, now); se != nil {
			return result(se, setReady)
		}
	} else if err := r.syncSecret(ctx, acct, key, store, secret, target); err != nil {
		return result(remoteErr(store, err), setReady)
	}
	key.Status.DeliveredTo = store.where()

	revokeAt, err := r.revokeRetired(ctx, acct, key, now)
	if err != nil {
		logger.Error(err, "revoking retired keys; will retry")
	}

	where := "Secret " + secretName
	if store.remote != "" {
		where = fmt.Sprintf("Secret %s/%s in RemoteCluster %s", store.namespace, secretName, store.remote)
	}
	setCondition(&key.Status.Conditions, key.Generation, metav1.ConditionTrue, b2v1.ReasonReconciled,
		fmt.Sprintf("Key %s is current and stored in %s", key.Status.KeyID, where))
	return ctrl.Result{RequeueAfter: r.nextWake(key, now, revokeAt)}, nil
}

// targetFromSpec determines the bucket name a key is scoped to without
// requiring the bucket to be ready. For a bucketRef it returns the Bucket.
func (r *ApplicationKeyReconciler) targetFromSpec(ctx context.Context, key *b2v1.ApplicationKey) (keyTarget, *b2v1.Bucket, *stageError) {
	switch {
	case key.Spec.BucketRef != nil:
		var bkt b2v1.Bucket
		if err := r.Client.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Spec.BucketRef.Name}, &bkt); err != nil {
			if apierrors.IsNotFound(err) {
				return keyTarget{}, nil, waitFor(b2v1.ReasonBucketNotFound, time.Minute, "Bucket %q not found in namespace %s", key.Spec.BucketRef.Name, key.Namespace)
			}
			return keyTarget{}, nil, &stageError{reason: b2v1.ReasonBucketNotReady, message: err.Error(), err: err}
		}
		if bkt.Spec.ProviderConfigRef.ProviderConfigName() != key.Spec.ProviderConfigRef.ProviderConfigName() {
			return keyTarget{}, nil, waitFor(b2v1.ReasonInvalidSpec, 5*time.Minute, "Bucket %q uses provider config %q, but the key uses %q",
				bkt.Name, bkt.Spec.ProviderConfigRef.ProviderConfigName(), key.Spec.ProviderConfigRef.ProviderConfigName())
		}
		return keyTarget{bucketName: bkt.Spec.BucketName}, &bkt, nil
	case key.Spec.BucketName != "":
		return keyTarget{bucketName: key.Spec.BucketName, external: true}, nil, nil
	}
	return keyTarget{}, nil, nil
}

// resolveBucketID fills in the B2 bucket ID, waiting for a referenced Bucket
// to be ready or looking up an external bucket. An external bucket that is
// managed by (or was released from) another namespace is refused with
// ReasonPolicyDenied, whatever the name patterns say: patterns such as
// "acme-{namespace}-*" also match names of namespaces that share a prefix.
func (r *ApplicationKeyReconciler) resolveBucketID(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, bkt *b2v1.Bucket, t *keyTarget) *stageError {
	switch {
	case bkt != nil:
		if !bkt.DeletionTimestamp.IsZero() || bkt.Status.BucketID == "" || !meta.IsStatusConditionTrue(bkt.Status.Conditions, b2v1.ConditionReady) {
			return waitFor(b2v1.ReasonBucketNotReady, time.Minute, "waiting for Bucket %q to be ready", bkt.Name)
		}
		t.bucketID = bkt.Status.BucketID
	case t.bucketName != "":
		b, err := acct.Client.GetBucketByName(ctx, t.bucketName)
		if err != nil {
			return providerError("looking up bucket", err)
		}
		if b == nil {
			return waitFor(b2v1.ReasonBucketNotFound, 5*time.Minute, "bucket %q does not exist in the B2 account", t.bucketName)
		}
		if rel := b.BucketInfo[ReleasedInfoKey]; rel != "" && rel != key.Namespace {
			return waitFor(b2v1.ReasonPolicyDenied, 10*time.Minute, "bucket %q was released by namespace %s", t.bucketName, rel)
		}
		if owner := b.BucketInfo[OwnerInfoKey]; owner != "" {
			var local b2v1.BucketList
			if err := r.Client.List(ctx, &local, client.InNamespace(key.Namespace)); err != nil {
				return &stageError{reason: b2v1.ReasonProviderError, message: err.Error(), err: err}
			}
			if !slices.ContainsFunc(local.Items, func(b b2v1.Bucket) bool { return string(b.UID) == owner }) {
				return waitFor(b2v1.ReasonPolicyDenied, 10*time.Minute, "bucket %q is managed by a Bucket outside namespace %s", t.bucketName, key.Namespace)
			}
		}
		t.bucketID = b.BucketID
	}
	return nil
}

// denied handles a key that is no longer allowed. Its current key is revoked
// (and Secret deleted) only after the key's grace period, and only if confirm
// (when set) still reports a denial against uncached data, so a momentary
// gap such as a policy being replaced does not break workloads.
func (r *ApplicationKeyReconciler) denied(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, reason string,
	setReady func(string, string), confirm func() (bool, error)) (ctrl.Result, error) {
	if !r.Options.RevokeOnPolicyViolation || key.Status.KeyID == "" {
		return result(&stageError{reason: b2v1.ReasonPolicyDenied, message: reason}, setReady)
	}
	now := r.now()
	if key.Status.ScheduledRevocation == nil {
		at := metav1.NewTime(now.Add(r.gracePeriod(key)))
		key.Status.ScheduledRevocation = &at
		r.Recorder.Eventf(key, nil, corev1.EventTypeWarning, "RevocationScheduled", "Reconcile",
			"Key %s is no longer allowed and will be revoked at %s unless access is restored: %s", key.Status.KeyID, at.UTC().Format(time.RFC3339), reason)
	}
	at := key.Status.ScheduledRevocation.Time
	if now.Before(at) {
		return result(waitFor(b2v1.ReasonPolicyDenied, max(at.Sub(now), time.Second),
			"%s; key %s will be revoked at %s unless access is restored", reason, key.Status.KeyID, at.UTC().Format(time.RFC3339)), setReady)
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
		return result(providerError("revoking a key that is no longer allowed", err), setReady)
	}
	if err := r.deleteOwnedSecret(ctx, key); err != nil {
		return ctrl.Result{}, err
	}
	key.Status.ScheduledRevocation = nil
	r.Recorder.Eventf(key, nil, corev1.EventTypeWarning, "Revoked", "Revoke", "Revoked key %s: %s", revoked, reason)
	return result(&stageError{reason: b2v1.ReasonPolicyDenied, message: reason}, setReady)
}

// isFresh reports whether key matches the latest version in the API server.
func (r *ApplicationKeyReconciler) isFresh(ctx context.Context, key *b2v1.ApplicationKey) (bool, error) {
	var live b2v1.ApplicationKey
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(key), &live); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return live.ResourceVersion == key.ResourceVersion, nil
}

func keyPolicyRequest(key *b2v1.ApplicationKey, t keyTarget) policy.KeyRequest {
	req := policy.KeyRequest{
		Namespace:      key.Namespace,
		ProviderConfig: key.Spec.ProviderConfigRef.ProviderConfigName(),
		BucketName:     t.bucketName,
		External:       t.external,
		Capabilities:   key.Spec.Capabilities,
		DeliverTo:      key.Spec.DeliverTo,
	}
	if key.Spec.ValidFor != nil {
		req.ValidFor = key.Spec.ValidFor.Duration
	}
	return req
}

// getOwnedSecret returns the key's Secret, nil if it does not exist, or a
// SecretConflict error if a Secret of that name exists but is not ours.
func (r *ApplicationKeyReconciler) getOwnedSecret(ctx context.Context, key *b2v1.ApplicationKey, store *secretStore, name string) (*corev1.Secret, *stageError) {
	sec, err := store.get(ctx, name)
	if err != nil {
		return nil, remoteErr(store, fmt.Errorf("reading Secret %s: %w", name, err))
	}
	if sec != nil && !store.owns(sec, key) {
		return nil, waitFor(b2v1.ReasonSecretConflict, 5*time.Minute, "Secret %q already exists in %s and is not owned by this ApplicationKey; delete it or set spec.secretName", name, store.where())
	}
	return sec, nil
}

// replacementReason says why a new key is needed, or "" if the current key is fine.
func (r *ApplicationKeyReconciler) replacementReason(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, secret *corev1.Secret, specHash string, now time.Time) string {
	st := key.Status
	switch {
	case st.KeyID == "":
		return "initial key"
	case st.SpecHash != specHash:
		return "spec changed"
	case secret == nil || string(secret.Data[b2v1.SecretKeyB2KeyID]) != st.KeyID || len(secret.Data[b2v1.SecretKeyB2Key]) == 0:
		return "Secret was deleted or modified"
	case key.Spec.Rotation != nil && key.Spec.Rotation.Every != nil && st.CreatedAt != nil &&
		!now.Before(st.CreatedAt.Add(key.Spec.Rotation.Every.Duration)):
		return "scheduled rotation"
	case st.ExpiresAt != nil && !now.Before(st.ExpiresAt.Add(-renewBefore(key))):
		return "key is about to expire"
	}
	if st.LastVerifiedTime == nil || !now.Before(st.LastVerifiedTime.Add(r.Options.KeyVerifyInterval)) {
		kc := b2.New(b2.Options{
			BaseURL:          acct.APIURL,
			ApplicationKeyID: string(secret.Data[b2v1.SecretKeyB2KeyID]),
			ApplicationKey:   string(secret.Data[b2v1.SecretKeyB2Key]),
			MaxRetries:       1,
		})
		_, err := kc.Authorize(ctx)
		var credErr *b2.CredentialsError
		switch {
		case errors.As(err, &credErr):
			return "key was revoked or is no longer valid in B2"
		case err != nil:
			log.FromContext(ctx).Info("could not verify application key; will retry", "error", err.Error())
		default:
			t := metav1.NewTime(now)
			key.Status.LastVerifiedTime = &t
		}
	}
	return ""
}

func (r *ApplicationKeyReconciler) createAndSwap(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, orig **b2v1.ApplicationKey,
	store *secretStore, secret *corev1.Secret, t keyTarget, specHash, why string, now time.Time) *stageError {
	logger := log.FromContext(ctx)

	// Record the name before creating so a crash cannot orphan the key.
	key.Status.Serial++
	name := r.keyName(key, key.Status.Serial)
	key.Status.PendingKeyName = name
	if err := patchStatus(ctx, r.Client, key, *orig); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: fmt.Sprintf("recording pending key: %v", err), err: err}
	}
	*orig = key.DeepCopy()

	req := b2.CreateKeyRequest{KeyName: name, NamePrefix: key.Spec.NamePrefix}
	for _, c := range key.Spec.Capabilities {
		req.Capabilities = append(req.Capabilities, string(c))
	}
	slices.Sort(req.Capabilities)
	if t.bucketID != "" {
		req.BucketIDs = []string{t.bucketID}
	}
	if key.Spec.ValidFor != nil {
		req.ValidDurationInSeconds = int64(key.Spec.ValidFor.Seconds())
	}
	created, err := acct.Client.CreateKey(ctx, req)
	if err != nil {
		if apiErr, ok := b2.AsAPIError(err); ok && (apiErr.Status == 400 || apiErr.Status == 401 || apiErr.Status == 403) {
			// A definite rejection: nothing was created. Timeouts (408),
			// throttling (429) and server errors are ambiguous, so the
			// pending name is kept for orphan cleanup.
			key.Status.PendingKeyName = ""
		}
		switch {
		case b2.HasCode(err, b2.CodeBadBucketID):
			return waitFor(b2v1.ReasonBucketNotFound, time.Minute, "bucket %s no longer exists in B2", t.bucketName)
		case b2.HasCode(err, b2.CodeUnauthorized):
			return waitFor(b2v1.ReasonProviderError, 10*time.Minute, "B2 refused to create the key; check that the operator's key holds every capability being granted: %v", err)
		}
		return providerError("creating application key", err)
	}

	if err := r.writeSecret(ctx, acct, key, store, secret, t, created, specHash); err != nil {
		if rerr := acct.Client.DeleteKey(ctx, created.ApplicationKeyID); rerr == nil {
			key.Status.PendingKeyName = ""
		}
		return remoteErr(store, fmt.Errorf("writing Secret: %w", err))
	}

	if old := key.Status.KeyID; old != "" {
		grace := r.gracePeriod(key)
		if strings.HasPrefix(why, "key was revoked") {
			grace = 0
		}
		key.Status.RetiringKeys = append(key.Status.RetiringKeys, b2v1.RetiringKey{KeyID: old, RevokeAfter: metav1.NewTime(now.Add(grace))})
	}
	created2 := metav1.NewTime(now)
	key.Status.KeyID = created.ApplicationKeyID
	key.Status.KeyName = created.KeyName
	key.Status.BucketID = t.bucketID
	key.Status.BucketName = t.bucketName
	key.Status.SecretName = key.SecretNameOrDefault()
	key.Status.SpecHash = specHash
	key.Status.CreatedAt = &created2
	key.Status.LastVerifiedTime = &created2
	key.Status.ExpiresAt = nil
	if created.ExpirationTimestamp != nil {
		exp := metav1.NewTime(time.UnixMilli(*created.ExpirationTimestamp))
		key.Status.ExpiresAt = &exp
	}
	key.Status.PendingKeyName = ""

	logger.Info("created application key", "keyID", created.ApplicationKeyID, "reason", why)
	evReason := "KeyCreated"
	if len(key.Status.RetiringKeys) > 0 {
		evReason = "KeyRotated"
	}
	r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, evReason, "CreateKey", "Created key %s (%s) and stored it in Secret %s", created.ApplicationKeyID, why, key.Status.SecretName)
	return nil
}

func (r *ApplicationKeyReconciler) writeSecret(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, store *secretStore, existing *corev1.Secret, t keyTarget, k *b2.ApplicationKey, specHash string) error {
	data := secretData(acct, key, t, []byte(k.ApplicationKeyID), []byte(k.ApplicationKey))
	annotations := map[string]string{
		annotationKeyID:     k.ApplicationKeyID,
		annotationSpecHash:  specHash,
		annotationCreatedAt: r.now().UTC().Format(time.RFC3339),
	}
	if k.ExpirationTimestamp != nil {
		annotations[annotationExpiresAt] = time.UnixMilli(*k.ExpirationTimestamp).UTC().Format(time.RFC3339)
	}
	if existing == nil {
		s := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: key.SecretNameOrDefault(), Namespace: store.namespace},
			Type:       corev1.SecretTypeOpaque,
		}
		applySecretMeta(s, key, annotations)
		s.Data = data
		if err := store.claim(s, key); err != nil {
			return err
		}
		return store.writer.Create(ctx, s, client.FieldOwner(FieldOwner))
	}
	s := existing.DeepCopy()
	applySecretMeta(s, key, annotations)
	if err := store.claim(s, key); err != nil {
		return err
	}
	s.Data = data
	return store.writer.Update(ctx, s, client.FieldOwner(FieldOwner))
}

// syncSecret keeps the non-credential parts of an existing Secret current.
func (r *ApplicationKeyReconciler) syncSecret(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, store *secretStore, s *corev1.Secret, t keyTarget) error {
	want := s.DeepCopy()
	annotations := map[string]string{annotationKeyID: key.Status.KeyID, annotationSpecHash: key.Status.SpecHash}
	if key.Status.CreatedAt != nil {
		annotations[annotationCreatedAt] = key.Status.CreatedAt.UTC().Format(time.RFC3339)
	}
	if key.Status.ExpiresAt != nil {
		annotations[annotationExpiresAt] = key.Status.ExpiresAt.UTC().Format(time.RFC3339)
	}
	applySecretMeta(want, key, annotations)
	if err := store.claim(want, key); err != nil {
		return err
	}
	want.Data = secretData(acct, key, t, s.Data[b2v1.SecretKeyB2KeyID], s.Data[b2v1.SecretKeyB2Key])
	if maps.EqualFunc(want.Data, s.Data, func(a, b []byte) bool { return string(a) == string(b) }) &&
		maps.Equal(want.Labels, s.Labels) && maps.Equal(want.Annotations, s.Annotations) {
		return nil
	}
	return store.writer.Update(ctx, want, client.FieldOwner(FieldOwner))
}

func secretData(acct *provider.Account, key *b2v1.ApplicationKey, t keyTarget, keyID, secret []byte) map[string][]byte {
	data := map[string][]byte{
		b2v1.SecretKeyAWSAccessKeyID:     keyID,
		b2v1.SecretKeyAWSSecretAccessKey: secret,
		b2v1.SecretKeyB2KeyID:            keyID,
		b2v1.SecretKeyB2Key:              secret,
		b2v1.SecretKeyAWSEndpointURL:     []byte(acct.S3Endpoint),
		b2v1.SecretKeyAWSRegion:          []byte(acct.S3Region),
	}
	if t.bucketName != "" {
		data[b2v1.SecretKeyB2BucketName] = []byte(t.bucketName)
	}
	if key.Spec.NamePrefix != "" {
		data[b2v1.SecretKeyB2NamePrefix] = []byte(key.Spec.NamePrefix)
	}
	return data
}

func applySecretMeta(s *corev1.Secret, key *b2v1.ApplicationKey, annotations map[string]string) {
	labels := map[string]string{}
	anns := map[string]string{}
	if tpl := key.Spec.SecretTemplate; tpl != nil {
		maps.Copy(labels, tpl.Labels)
		maps.Copy(anns, tpl.Annotations)
	}
	labels[LabelManagedBy] = ManagedByValue
	labels[labelApplicationKey] = key.Name
	maps.Copy(anns, annotations)
	s.Labels = labels
	s.Annotations = anns
}

// revokeOrphans resolves keys created under status.pendingKeyName that were
// never recorded as current. A key that already reached the Secret (the
// operator stopped after writing the Secret but before recording status) is
// adopted as the current key; any other is revoked.
func (r *ApplicationKeyReconciler) revokeOrphans(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, store *secretStore) error {
	pending := key.Status.PendingKeyName
	orphans, err := acct.Client.FindKeys(ctx, func(k b2.ApplicationKey) bool {
		return k.KeyName == pending && k.ApplicationKeyID != key.Status.KeyID
	})
	if err != nil {
		return err
	}
	delivered, err := store.getUncached(ctx, key.SecretNameOrDefault())
	if err != nil {
		return err
	}
	deliveredID := ""
	if delivered != nil && store.owns(delivered, key) {
		deliveredID = string(delivered.Data[b2v1.SecretKeyB2KeyID])
	}
	for _, k := range orphans {
		if k.ApplicationKeyID == deliveredID {
			r.adoptDeliveredKey(key, &k, delivered)
			continue
		}
		if err := r.deleteKey(ctx, acct, k.ApplicationKeyID); err != nil {
			return err
		}
		log.FromContext(ctx).Info("revoked orphaned key from an interrupted creation", "keyID", k.ApplicationKeyID)
	}
	key.Status.PendingKeyName = ""
	return nil
}

func (r *ApplicationKeyReconciler) adoptDeliveredKey(key *b2v1.ApplicationKey, k *b2.ApplicationKey, s *corev1.Secret) {
	now := r.now()
	if old := key.Status.KeyID; old != "" {
		key.Status.RetiringKeys = append(key.Status.RetiringKeys, b2v1.RetiringKey{KeyID: old, RevokeAfter: metav1.NewTime(now.Add(r.gracePeriod(key)))})
	}
	created := metav1.NewTime(now)
	if t, err := time.Parse(time.RFC3339, s.Annotations[annotationCreatedAt]); err == nil {
		created = metav1.NewTime(t)
	}
	key.Status.KeyID = k.ApplicationKeyID
	key.Status.KeyName = k.KeyName
	key.Status.SpecHash = s.Annotations[annotationSpecHash]
	key.Status.SecretName = s.Name
	key.Status.CreatedAt = &created
	key.Status.LastVerifiedTime = nil
	key.Status.BucketID = ""
	if len(k.BucketIDs) > 0 {
		key.Status.BucketID = k.BucketIDs[0]
	}
	key.Status.BucketName = string(s.Data[b2v1.SecretKeyB2BucketName])
	key.Status.ExpiresAt = nil
	if k.ExpirationTimestamp != nil {
		exp := metav1.NewTime(time.UnixMilli(*k.ExpirationTimestamp))
		key.Status.ExpiresAt = &exp
	}
	r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, "KeyRecovered", "Reconcile", "Recovered key %s that was delivered to Secret %s before an interruption", k.ApplicationKeyID, s.Name)
}

// revokeRetired deletes retiring keys whose grace period has passed and
// returns when the next one is due.
func (r *ApplicationKeyReconciler) revokeRetired(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, now time.Time) (time.Time, error) {
	var keep []b2v1.RetiringKey
	var next time.Time
	var errs []error
	for _, rk := range key.Status.RetiringKeys {
		if now.Before(rk.RevokeAfter.Time) {
			keep = append(keep, rk)
			if next.IsZero() || rk.RevokeAfter.Before(&metav1.Time{Time: next}) {
				next = rk.RevokeAfter.Time
			}
			continue
		}
		if err := r.deleteKey(ctx, acct, rk.KeyID); err != nil {
			errs = append(errs, err)
			keep = append(keep, rk)
			continue
		}
		r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, "KeyRevoked", "DeleteKey", "Revoked replaced key %s", rk.KeyID)
	}
	key.Status.RetiringKeys = keep
	return next, errors.Join(errs...)
}

// revokeAll deletes the current, retiring and any pending keys.
func (r *ApplicationKeyReconciler) revokeAll(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey) error {
	if key.Status.PendingKeyName != "" {
		store, se := r.store(ctx, key)
		if se != nil {
			return se
		}
		if err := r.revokeOrphans(ctx, acct, key, store); err != nil {
			return err
		}
	}
	for len(key.Status.RetiringKeys) > 0 {
		if err := r.deleteKey(ctx, acct, key.Status.RetiringKeys[0].KeyID); err != nil {
			return err
		}
		key.Status.RetiringKeys = key.Status.RetiringKeys[1:]
	}
	if key.Status.KeyID != "" {
		if err := r.deleteKey(ctx, acct, key.Status.KeyID); err != nil {
			return err
		}
		r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, "KeyRevoked", "DeleteKey", "Revoked key %s", key.Status.KeyID)
	}
	key.Status.KeyID = ""
	key.Status.KeyName = ""
	key.Status.SpecHash = ""
	key.Status.CreatedAt = nil
	key.Status.ExpiresAt = nil
	key.Status.LastVerifiedTime = nil
	return nil
}

// deleteKey deletes a key, treating a key that no longer exists as deleted.
func (r *ApplicationKeyReconciler) deleteKey(ctx context.Context, acct *provider.Account, id string) error {
	err := acct.Client.DeleteKey(ctx, id)
	if err == nil {
		return nil
	}
	if apiErr, ok := b2.AsAPIError(err); ok && apiErr.Status == 400 {
		exists, xerr := acct.Client.KeyExists(ctx, id)
		if xerr == nil && !exists {
			return nil
		}
	}
	return err
}

func (r *ApplicationKeyReconciler) deleteOwnedSecret(ctx context.Context, key *b2v1.ApplicationKey) error {
	store, se := r.store(ctx, key)
	if se != nil {
		return se
	}
	sec, err := store.getUncached(ctx, key.SecretNameOrDefault())
	if err != nil || sec == nil || !store.owns(sec, key) {
		return err
	}
	return client.IgnoreNotFound(store.writer.Delete(ctx, sec, client.Preconditions{UID: &sec.UID}))
}

func (r *ApplicationKeyReconciler) finalize(ctx context.Context, key *b2v1.ApplicationKey) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(key, Finalizer) {
		return ctrl.Result{}, nil
	}
	orig := key.DeepCopy()
	fail := func(se *stageError) (ctrl.Result, error) {
		res, err := result(se, func(reason, msg string) {
			setCondition(&key.Status.Conditions, key.Generation, metav1.ConditionFalse, reason, msg)
		})
		if perr := patchStatus(ctx, r.Client, key, orig); perr != nil && !apierrors.IsNotFound(perr) {
			log.FromContext(ctx).Error(perr, "updating status during deletion")
		}
		return res, err
	}

	if key.Status.KeyID != "" || key.Status.PendingKeyName != "" || len(key.Status.RetiringKeys) > 0 {
		acct, se := r.resolveAccount(ctx, key.Spec.ProviderConfigRef.ProviderConfigName())
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
	base := key.DeepCopy()
	controllerutil.RemoveFinalizer(key, Finalizer)
	if err := r.Client.Patch(ctx, key, client.MergeFrom(base), client.FieldOwner(FieldOwner)); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
}

func (r *ApplicationKeyReconciler) gracePeriod(key *b2v1.ApplicationKey) time.Duration {
	if rot := key.Spec.Rotation; rot != nil && rot.GracePeriod != nil {
		return rot.GracePeriod.Duration
	}
	if r.Options.DefaultGracePeriod > 0 {
		return r.Options.DefaultGracePeriod
	}
	return defaultGracePeriod
}

// nextWake is when the key next needs attention.
func (r *ApplicationKeyReconciler) nextWake(key *b2v1.ApplicationKey, now, revokeAt time.Time) time.Duration {
	until := func(t time.Time) time.Duration {
		if t.IsZero() {
			return 0
		}
		return max(t.Sub(now), time.Second)
	}
	var rotateAt, renewAt, verifyAt time.Time
	st := key.Status
	if rot := key.Spec.Rotation; rot != nil && rot.Every != nil && st.CreatedAt != nil {
		rotateAt = st.CreatedAt.Add(rot.Every.Duration)
	}
	if st.ExpiresAt != nil {
		renewAt = st.ExpiresAt.Add(-renewBefore(key))
	}
	if st.LastVerifiedTime != nil && r.Options.KeyVerifyInterval > 0 {
		verifyAt = st.LastVerifiedTime.Add(r.Options.KeyVerifyInterval)
	}
	return minPositive(r.Options.ResyncPeriod, until(revokeAt), until(rotateAt), until(renewAt), until(verifyAt))
}

// renewBefore is how long before expiry a key is replaced: a third of its
// lifetime, at most a week.
func renewBefore(key *b2v1.ApplicationKey) time.Duration {
	if key.Spec.ValidFor == nil {
		return 0
	}
	return min(key.Spec.ValidFor.Duration/3, maxRenewBefore)
}

// keyName builds a B2 key name (at most 100 of [A-Za-z0-9-]):
// b2op-<cluster>-<uid8>-<serial>-<namespace>-<name>. The fixed-format head
// lets the orphan sweep attribute keys to this cluster and to a resource;
// the tail is for humans reading the B2 console.
func (r *ApplicationKeyReconciler) keyName(key *b2v1.ApplicationKey, serial int64) string {
	head := fmt.Sprintf("%s-%s-%s-%d", KeyNamePrefix, r.Options.ClusterID, uid8(key.UID), serial)
	tail := strings.Trim(invalidKeyNameChars.ReplaceAllString(key.Namespace+"-"+key.Name, "-"), "-")
	if room := 100 - len(head) - 1; len(tail) > room {
		tail = strings.TrimRight(tail[:room], "-")
	}
	return head + "-" + tail
}

// uid8 is the first 8 hex characters of a UID.
func uid8(uid types.UID) string {
	u := strings.ReplaceAll(string(uid), "-", "")
	if len(u) > 8 {
		u = u[:8]
	}
	return u
}

// keySpecHash identifies the B2-side properties of a key; a change means the
// key must be replaced, since B2 keys are immutable.
func keySpecHash(key *b2v1.ApplicationKey, t keyTarget) string {
	caps := make([]string, 0, len(key.Spec.Capabilities))
	for _, c := range key.Spec.Capabilities {
		caps = append(caps, string(c))
	}
	slices.Sort(caps)
	var validFor string
	if key.Spec.ValidFor != nil {
		validFor = key.Spec.ValidFor.Duration.String()
	}
	b, _ := json.Marshal([]any{key.Spec.ProviderConfigRef.ProviderConfigName(), t.bucketID, key.Spec.NamePrefix, caps, validFor})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// SetupWithManager registers the controller and its watches.
func (r *ApplicationKeyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ctx := context.Background()
	if err := mgr.GetFieldIndexer().IndexField(ctx, &b2v1.ApplicationKey{}, indexBucketRef, func(o client.Object) []string {
		if ref := o.(*b2v1.ApplicationKey).Spec.BucketRef; ref != nil {
			return []string{ref.Name}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &b2v1.ApplicationKey{}, indexProviderConfig, func(o client.Object) []string {
		return []string{o.(*b2v1.ApplicationKey).Spec.ProviderConfigRef.ProviderConfigName()}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &b2v1.ApplicationKey{}, indexRemoteCluster, func(o client.Object) []string {
		if t := o.(*b2v1.ApplicationKey).Spec.DeliverTo; t != nil {
			return []string{t.RemoteCluster}
		}
		return nil
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.ApplicationKey{}, builder.WithPredicates(specOrDeletionChanged())).
		Owns(&corev1.Secret{}).
		Watches(&b2v1.Bucket{}, handler.EnqueueRequestsFromMapFunc(r.keysForBucket), builder.WithPredicates(bucketReadinessChanged())).
		Watches(&b2v1.ClusterProviderConfig{}, handler.EnqueueRequestsFromMapFunc(r.keysForProviderConfig), builder.WithPredicates(readinessChanged())).
		Watches(&b2v1.B2AccessPolicy{}, handler.EnqueueRequestsFromMapFunc(r.allKeys)).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.keysInNamespace), builder.WithPredicates(predicate.LabelChangedPredicate{})).
		Watches(&b2v1.RemoteCluster{}, handler.EnqueueRequestsFromMapFunc(r.keysForRemoteCluster), builder.WithPredicates(remoteReadinessChanged())).
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Named("applicationkey").
		Complete(r)
}

func (r *ApplicationKeyReconciler) keysForBucket(ctx context.Context, o client.Object) []reconcile.Request {
	return r.listRequests(ctx, client.InNamespace(o.GetNamespace()), client.MatchingFields{indexBucketRef: o.GetName()})
}

func (r *ApplicationKeyReconciler) keysForProviderConfig(ctx context.Context, o client.Object) []reconcile.Request {
	return r.listRequests(ctx, client.MatchingFields{indexProviderConfig: o.GetName()})
}

func (r *ApplicationKeyReconciler) keysForRemoteCluster(ctx context.Context, o client.Object) []reconcile.Request {
	return r.listRequests(ctx, client.MatchingFields{indexRemoteCluster: o.GetName()})
}

// remoteReadinessChanged passes RemoteCluster events that change readiness.
func remoteReadinessChanged() predicate.Predicate {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		o, ok1 := e.ObjectOld.(*b2v1.RemoteCluster)
		n, ok2 := e.ObjectNew.(*b2v1.RemoteCluster)
		return !ok1 || !ok2 || meta.IsStatusConditionTrue(o.Status.Conditions, b2v1.ConditionReady) != meta.IsStatusConditionTrue(n.Status.Conditions, b2v1.ConditionReady)
	}}
}

func (r *ApplicationKeyReconciler) allKeys(ctx context.Context, _ client.Object) []reconcile.Request {
	return r.listRequests(ctx)
}

func (r *ApplicationKeyReconciler) keysInNamespace(ctx context.Context, o client.Object) []reconcile.Request {
	return r.listRequests(ctx, client.InNamespace(o.GetName()))
}

func (r *ApplicationKeyReconciler) listRequests(ctx context.Context, opts ...client.ListOption) []reconcile.Request {
	var list b2v1.ApplicationKeyList
	if err := r.Client.List(ctx, &list, opts...); err != nil {
		log.FromContext(ctx).Error(err, "listing ApplicationKeys for watch")
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for _, k := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&k)})
	}
	return out
}

// bucketReadinessChanged passes Bucket events that can unblock or affect keys.
func bucketReadinessChanged() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			o, ok1 := e.ObjectOld.(*b2v1.Bucket)
			n, ok2 := e.ObjectNew.(*b2v1.Bucket)
			if !ok1 || !ok2 {
				return true
			}
			return o.Status.BucketID != n.Status.BucketID ||
				meta.IsStatusConditionTrue(o.Status.Conditions, b2v1.ConditionReady) != meta.IsStatusConditionTrue(n.Status.Conditions, b2v1.ConditionReady) ||
				o.DeletionTimestamp.IsZero() != n.DeletionTimestamp.IsZero()
		},
	}
}
