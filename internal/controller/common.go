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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-operator/internal/policy"
	"github.com/backblaze-b2-samples/b2-operator/internal/provider"
)

const (
	// FieldOwner is the field manager name used for writes.
	FieldOwner = "b2-operator"
	// Finalizer guards B2 resources on Bucket and ApplicationKey.
	Finalizer = "b2.backblaze.com/finalizer"
	// LabelManagedBy marks Secrets written by the operator. The manager only
	// caches Secrets with this label.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// ManagedByValue is the value of LabelManagedBy.
	ManagedByValue = "b2-operator"
	// OwnerInfoKey is the bucketInfo key recording the owning resource's UID.
	OwnerInfoKey = "b2operator-owner-uid"
	// ReleasedInfoKey records the namespace that released a retained bucket,
	// so that only that namespace can adopt it again.
	ReleasedInfoKey = "b2operator-released-from"
	// KeyNamePrefix starts the name of every B2 key the operator creates.
	KeyNamePrefix = "b2op"

	indexBucketRef       = ".spec.bucketRef.name"
	indexReplicationDest = ".spec.replication.destinationBucketRef.name"
	indexProviderConfig  = ".spec.providerConfigRef.name"
)

// Options holds settings shared by the reconcilers.
type Options struct {
	// ResyncPeriod is how often resources are compared with B2 when nothing
	// changes in the cluster, to detect and correct drift.
	ResyncPeriod time.Duration
	// KeyVerifyInterval is how often each current application key is
	// checked against B2.
	KeyVerifyInterval time.Duration
	// DefaultGracePeriod is how long a replaced key stays valid.
	DefaultGracePeriod time.Duration
	// RevokeOnPolicyViolation revokes existing keys that a policy change no
	// longer allows, after the key's grace period.
	RevokeOnPolicyViolation bool
	// ClusterID distinguishes this cluster's keys from other clusters'
	// sharing a B2 account (8 characters of [a-z0-9]).
	ClusterID string
}

// Deps are the collaborators shared by the reconcilers.
type Deps struct {
	Client   client.Client
	Registry *provider.Registry
	Policy   *policy.Evaluator
	Recorder events.EventRecorder
	Options  Options
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

func (d *Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func setCondition(conds *[]metav1.Condition, generation int64, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               b2v1.ConditionReady,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            truncate(message, 32768),
	})
}

// eventType is Normal for conditions that resolve on their own (waiting on
// another resource) and Warning for anything that needs attention.
func eventType(reason string) string {
	switch reason {
	case b2v1.ReasonBucketNotReady, b2v1.ReasonBucketNotFound, b2v1.ReasonProviderNotReady, b2v1.ReasonReconciling:
		return corev1.EventTypeNormal
	}
	return corev1.EventTypeWarning
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// stageError is a reconcile failure with the condition it should produce.
type stageError struct {
	reason  string
	message string
	// requeueAfter, when set, schedules a retry instead of returning an
	// error (used for conditions that need a human or another resource).
	requeueAfter time.Duration
	// err is returned to controller-runtime for exponential backoff.
	err error
}

func (e *stageError) Error() string { return e.reason + ": " + e.message }

func waitFor(reason string, after time.Duration, format string, args ...any) *stageError {
	return &stageError{reason: reason, message: fmt.Sprintf(format, args...), requeueAfter: after}
}

// providerError converts a B2 API error into a stageError: throttling is
// retried after the server's Retry-After, other transient errors with
// controller backoff, and permanent errors after a long delay.
func providerError(action string, err error) *stageError {
	se := &stageError{reason: b2v1.ReasonProviderError, message: fmt.Sprintf("%s: %v", action, err)}
	var credErr *b2.CredentialsError
	switch {
	case errors.As(err, &credErr):
		se.reason = b2v1.ReasonInvalidCredentials
		se.requeueAfter = 5 * time.Minute
	case b2.RetryAfter(err) > 0:
		se.requeueAfter = b2.RetryAfter(err)
	case b2.IsRetryable(err):
		se.err = err
	default:
		se.requeueAfter = 10 * time.Minute
	}
	return se
}

// resolveAccount returns the B2 account for a provider config name.
func (d *Deps) resolveAccount(ctx context.Context, name string) (*provider.Account, *stageError) {
	var pc b2v1.ClusterProviderConfig
	if err := d.Client.Get(ctx, client.ObjectKey{Name: name}, &pc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, waitFor(b2v1.ReasonProviderNotReady, time.Minute, "ClusterProviderConfig %q not found", name)
		}
		return nil, &stageError{reason: b2v1.ReasonProviderNotReady, message: err.Error(), err: err}
	}
	if !meta.IsStatusConditionTrue(pc.Status.Conditions, b2v1.ConditionReady) {
		msg := "not ready"
		if c := meta.FindStatusCondition(pc.Status.Conditions, b2v1.ConditionReady); c != nil {
			msg = c.Message
		}
		return nil, waitFor(b2v1.ReasonProviderNotReady, time.Minute, "ClusterProviderConfig %q: %s", name, msg)
	}
	acct, err := d.Registry.Get(ctx, &pc)
	if err != nil {
		if errors.Is(err, provider.ErrSecretNotFound) {
			return nil, waitFor(b2v1.ReasonProviderNotReady, time.Minute, "ClusterProviderConfig %q: %v", name, err)
		}
		return nil, providerError("authorizing with B2", err)
	}
	return acct, nil
}

// result converts a stageError into a reconcile result, recording it on the
// object's Ready condition via setReady.
func result(se *stageError, setReady func(reason, message string)) (ctrl.Result, error) {
	setReady(se.reason, se.message)
	if se.err != nil {
		return ctrl.Result{}, se.err
	}
	return ctrl.Result{RequeueAfter: se.requeueAfter}, nil
}

// patchStatus writes obj's status with an optimistic lock against orig.
func patchStatus(ctx context.Context, c client.Client, obj, orig client.Object) error {
	return c.Status().Patch(ctx, obj, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner))
}

// minPositive returns the smallest positive duration, or zero.
func minPositive(ds ...time.Duration) time.Duration {
	var out time.Duration
	for _, d := range ds {
		if d > 0 && (out == 0 || d < out) {
			out = d
		}
	}
	return out
}
