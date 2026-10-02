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

// Key rotation. A key's secret half is only returned by B2 once, at
// creation, so the Secret is the only copy. A replacement is always created
// and delivered before the old key is scheduled for revocation, and creation
// is bracketed by status.pendingKeyName so that a key orphaned by a crash
// can be found afterwards.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

// maxRenewBefore caps how early before expiry a key is replaced.
const maxRenewBefore = 7 * 24 * time.Hour

var invalidKeyNameChars = regexp.MustCompile(`[^A-Za-z0-9-]+`)

// replacement says why a key needs replacing; the zero value means it does
// not.
type replacement struct {
	reason string
	// revoked means the current key no longer works, so it gets no grace
	// period.
	revoked bool
}

func (rp replacement) needed() bool { return rp.reason != "" }

// needsReplacement reports whether the current key must be replaced. It
// periodically checks the key against B2, recording LastVerifiedTime.
func (r *ApplicationKeyReconciler) needsReplacement(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey,
	sec *corev1.Secret, specHash string, now time.Time) replacement {
	st := key.Status
	switch {
	case st.KeyID == "":
		return replacement{reason: "initial key"}
	case st.SpecHash != specHash:
		return replacement{reason: "spec changed"}
	case sec == nil || string(sec.Data[b2v1.SecretKeyB2KeyID]) != st.KeyID || len(sec.Data[b2v1.SecretKeyB2Key]) == 0:
		return replacement{reason: "Secret was deleted or modified"}
	case key.Spec.Rotation != nil && key.Spec.Rotation.Every != nil && st.CreatedAt != nil && !now.Before(st.CreatedAt.Add(key.Spec.Rotation.Every.Duration)):
		return replacement{reason: "scheduled rotation"}
	case st.ExpiresAt != nil && !now.Before(st.ExpiresAt.Add(-renewBefore(key))):
		return replacement{reason: "key is about to expire"}
	}

	if st.LastVerifiedTime != nil && now.Before(st.LastVerifiedTime.Add(r.Options.KeyVerifyInterval)) {
		return replacement{}
	}
	kc := r.Accounts.NewClient(acct.APIURL, string(sec.Data[b2v1.SecretKeyB2KeyID]), string(sec.Data[b2v1.SecretKeyB2Key]))
	_, err := kc.Authorize(ctx)
	var credErr *b2.CredentialsError
	switch {
	case errors.As(err, &credErr):
		return replacement{reason: "key was revoked or is no longer valid in B2", revoked: true}
	case err != nil:
		log.FromContext(ctx).Info("could not verify application key; will retry", "error", err.Error())
	default:
		verified := metav1.NewTime(now)
		key.Status.LastVerifiedTime = &verified
	}
	return replacement{}
}

// createAndSwap creates a new key, delivers it to the Secret, and retires
// the old one. The pending key name is persisted first, with an optimistic
// lock against orig, so a crash cannot leave an untracked key; orig is
// updated to the persisted state.
func (r *ApplicationKeyReconciler) createAndSwap(ctx context.Context, acct *provider.Account, key, orig *b2v1.ApplicationKey,
	store *secretStore, sec *corev1.Secret, scope keyScope, specHash string, why replacement, now time.Time) *stageError {
	key.Status.Serial++
	key.Status.PendingKeyName = r.keyName(key, key.Status.Serial)
	if err := r.patchStatus(ctx, key, orig); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: fmt.Sprintf("recording pending key: %v", err), err: err}
	}
	key.DeepCopyInto(orig)

	created, err := acct.Client.CreateKey(ctx, newKeyRequest(key, scope))
	if err != nil {
		if apiErr, ok := b2.AsAPIError(err); ok && (apiErr.Status == 400 || apiErr.Status == 401 || apiErr.Status == 403) {
			// A definite rejection: nothing was created. Timeouts, throttling
			// and server errors are ambiguous, so the pending name is kept
			// for orphan cleanup.
			key.Status.PendingKeyName = ""
		}
		switch {
		case b2.HasCode(err, b2.CodeBadBucketID):
			return waitFor(b2v1.ReasonBucketNotFound, time.Minute, "bucket %s no longer exists in B2", scope.bucketName)
		case b2.HasCode(err, b2.CodeUnauthorized):
			return waitFor(b2v1.ReasonProviderError, 10*time.Minute,
				"B2 refused to create the key; check that the operator's key holds every capability being granted: %v", err)
		}
		return providerError("creating application key", err)
	}

	if err := r.writeSecret(ctx, acct, key, store, sec, scope, created, specHash); err != nil {
		if acct.Client.DeleteKeyIfExists(ctx, created.ApplicationKeyID) == nil {
			key.Status.PendingKeyName = ""
		}
		return store.failed(fmt.Errorf("writing Secret: %w", err))
	}

	grace := r.gracePeriod(key)
	if why.revoked {
		grace = 0
	}
	r.retireCurrentKey(key, now.Add(grace))
	recordCurrentKey(key, created, now)
	key.Status.SpecHash = specHash
	key.Status.BucketID = scope.bucketID
	key.Status.BucketName = scope.bucketName
	key.Status.SecretName = key.SecretNameOrDefault()
	key.Status.LastVerifiedTime = key.Status.CreatedAt

	log.FromContext(ctx).Info("created application key", "keyID", created.ApplicationKeyID, "reason", why.reason)
	event := EventKeyCreated
	if len(key.Status.RetiringKeys) > 0 {
		event = EventKeyRotated
	}
	r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, event, "CreateKey", "Created key %s (%s) and stored it in Secret %s",
		created.ApplicationKeyID, why.reason, key.Status.SecretName)
	return nil
}

func newKeyRequest(key *b2v1.ApplicationKey, scope keyScope) b2.CreateKeyRequest {
	req := b2.CreateKeyRequest{KeyName: key.Status.PendingKeyName, NamePrefix: key.Spec.NamePrefix}
	for _, c := range key.Spec.Capabilities {
		req.Capabilities = append(req.Capabilities, string(c))
	}
	slices.Sort(req.Capabilities)
	if scope.bucketID != "" {
		req.BucketIDs = []string{scope.bucketID}
	}
	if key.Spec.ValidFor != nil {
		req.ValidDurationInSeconds = int64(key.Spec.ValidFor.Seconds())
	}
	return req
}

// retireCurrentKey schedules the current key, if any, for revocation.
func (r *ApplicationKeyReconciler) retireCurrentKey(key *b2v1.ApplicationKey, revokeAfter time.Time) {
	if key.Status.KeyID == "" {
		return
	}
	key.Status.RetiringKeys = append(key.Status.RetiringKeys, b2v1.RetiringKey{KeyID: key.Status.KeyID, RevokeAfter: metav1.NewTime(revokeAfter)})
}

// recordCurrentKey makes k the current key in status.
func recordCurrentKey(key *b2v1.ApplicationKey, k *b2.ApplicationKey, createdAt time.Time) {
	created := metav1.NewTime(createdAt)
	key.Status.KeyID = k.ApplicationKeyID
	key.Status.KeyName = k.KeyName
	key.Status.CreatedAt = &created
	key.Status.ExpiresAt = millisTime(k.ExpirationTimestamp)
	key.Status.PendingKeyName = ""
}

// resolvePendingKey deals with keys created under status.pendingKeyName but
// never recorded. A key that reached the Secret (the operator stopped after
// writing the Secret but before recording status) becomes the current key;
// any other is revoked.
func (r *ApplicationKeyReconciler) resolvePendingKey(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, store *secretStore) error {
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
			r.recoverDeliveredKey(key, &k, delivered)
			continue
		}
		if err := acct.Client.DeleteKeyIfExists(ctx, k.ApplicationKeyID); err != nil {
			return err
		}
		log.FromContext(ctx).Info("revoked orphaned key from an interrupted creation", "keyID", k.ApplicationKeyID)
	}
	key.Status.PendingKeyName = ""
	return nil
}

// recoverDeliveredKey records a key found in the Secret as current, taking
// its spec hash and creation time from the Secret's annotations.
func (r *ApplicationKeyReconciler) recoverDeliveredKey(key *b2v1.ApplicationKey, k *b2.ApplicationKey, sec *corev1.Secret) {
	now := r.now()
	r.retireCurrentKey(key, now.Add(r.gracePeriod(key)))
	createdAt := now
	if t, err := time.Parse(time.RFC3339, sec.Annotations[annotationCreatedAt]); err == nil {
		createdAt = t
	}
	recordCurrentKey(key, k, createdAt)
	key.Status.SpecHash = sec.Annotations[annotationSpecHash]
	key.Status.SecretName = sec.Name
	key.Status.LastVerifiedTime = nil
	key.Status.BucketID = ""
	if len(k.BucketIDs) > 0 {
		key.Status.BucketID = k.BucketIDs[0]
	}
	key.Status.BucketName = string(sec.Data[b2v1.SecretKeyB2BucketName])
	r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, EventKeyRecovered, "Reconcile",
		"Recovered key %s that was delivered to Secret %s before an interruption", k.ApplicationKeyID, sec.Name)
}

// revokeRetired revokes retiring keys whose grace period has passed and
// returns when the next one is due.
func (r *ApplicationKeyReconciler) revokeRetired(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, now time.Time) (time.Time, error) {
	var keep []b2v1.RetiringKey
	var next time.Time
	var errs []error
	for _, rk := range key.Status.RetiringKeys {
		if now.Before(rk.RevokeAfter.Time) {
			keep = append(keep, rk)
			if next.IsZero() || rk.RevokeAfter.Time.Before(next) {
				next = rk.RevokeAfter.Time
			}
			continue
		}
		if err := acct.Client.DeleteKeyIfExists(ctx, rk.KeyID); err != nil {
			errs = append(errs, err)
			keep = append(keep, rk)
			continue
		}
		r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, EventKeyRevoked, "DeleteKey", "Revoked replaced key %s", rk.KeyID)
	}
	key.Status.RetiringKeys = keep
	return next, errors.Join(errs...)
}

// revokeAll revokes the current, retiring and any pending keys.
func (r *ApplicationKeyReconciler) revokeAll(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey) error {
	if key.Status.PendingKeyName != "" {
		store, se := r.store(ctx, key)
		if se != nil {
			return se
		}
		if err := r.resolvePendingKey(ctx, acct, key, store); err != nil {
			return err
		}
	}
	for len(key.Status.RetiringKeys) > 0 {
		if err := acct.Client.DeleteKeyIfExists(ctx, key.Status.RetiringKeys[0].KeyID); err != nil {
			return err
		}
		key.Status.RetiringKeys = key.Status.RetiringKeys[1:]
	}
	if key.Status.KeyID != "" {
		if err := acct.Client.DeleteKeyIfExists(ctx, key.Status.KeyID); err != nil {
			return err
		}
		r.Recorder.Eventf(key, nil, corev1.EventTypeNormal, EventKeyRevoked, "DeleteKey", "Revoked key %s", key.Status.KeyID)
	}
	key.Status.KeyID = ""
	key.Status.KeyName = ""
	key.Status.SpecHash = ""
	key.Status.CreatedAt = nil
	key.Status.ExpiresAt = nil
	key.Status.LastVerifiedTime = nil
	return nil
}

func (r *ApplicationKeyReconciler) gracePeriod(key *b2v1.ApplicationKey) time.Duration {
	if rot := key.Spec.Rotation; rot != nil && rot.GracePeriod != nil {
		return rot.GracePeriod.Duration
	}
	return r.Options.DefaultGracePeriod
}

// nextWake is how long until the key next needs attention.
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
// lifetime, at most maxRenewBefore.
func renewBefore(key *b2v1.ApplicationKey) time.Duration {
	if key.Spec.ValidFor == nil {
		return 0
	}
	return min(key.Spec.ValidFor.Duration/3, maxRenewBefore)
}

// keyName builds a B2 key name (at most 100 of [A-Za-z0-9-]):
// b2op-<cluster>-<uid8>-<serial>-<namespace>-<name>. The fixed-format head
// lets the orphan sweep attribute keys to this cluster and to a resource;
// the tail is for people reading the B2 console.
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
	return u[:min(8, len(u))]
}

// keySpecHash identifies the B2-side properties of a key. B2 keys are
// immutable, so a change means the key must be replaced.
func keySpecHash(key *b2v1.ApplicationKey, scope keyScope) string {
	caps := make([]string, 0, len(key.Spec.Capabilities))
	for _, c := range key.Spec.Capabilities {
		caps = append(caps, string(c))
	}
	slices.Sort(caps)
	var validFor string
	if key.Spec.ValidFor != nil {
		validFor = key.Spec.ValidFor.Duration.String()
	}
	b, _ := json.Marshal([]any{key.Spec.ProviderConfigRef.NameOrDefault(), scope.bucketID, key.Spec.NamePrefix, caps, validFor})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}
