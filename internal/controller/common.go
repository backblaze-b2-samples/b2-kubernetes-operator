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

// Package controller holds the operator's reconcilers. Each reconciler
// follows the same shape: load the resource, handle deletion, ensure the
// finalizer, reconcile against B2 (policy first, then B2, then status), and
// commit status once. Shared pieces live in this file and in status.go,
// errors.go, watches.go and events.go.
package controller

import (
	"context"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/policy"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

const (
	// FieldOwner is the field manager name used for writes.
	FieldOwner = "b2-operator"
	// Finalizer guards B2 resources on Buckets, ApplicationKeys and B2Accounts.
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
	indexRemoteCluster   = ".spec.deliverTo.remoteCluster"
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
	Client client.Client
	// APIReader reads from the API server directly: Secrets outside the
	// operator's label-filtered cache, and freshness checks before
	// destructive actions.
	APIReader client.Reader
	Accounts  *provider.Registry
	Policy    *policy.Evaluator
	Recorder  events.EventRecorder
	Options   Options
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

func (d *Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// resolveAccount returns the B2 account for bucket and key management.
// Partner configs are refused: their master key only provisions B2Accounts.
func (d *Deps) resolveAccount(ctx context.Context, name string) (*provider.Account, *stageError) {
	return d.resolve(ctx, name, false)
}

// resolvePartnerAccount returns the Group admin account of a partner config.
func (d *Deps) resolvePartnerAccount(ctx context.Context, name string) (*provider.Account, *stageError) {
	return d.resolve(ctx, name, true)
}

func (d *Deps) resolve(ctx context.Context, name string, partner bool) (*provider.Account, *stageError) {
	var pc b2v1.ClusterProviderConfig
	if err := d.Client.Get(ctx, client.ObjectKey{Name: name}, &pc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, waitFor(b2v1.ReasonProviderConfigNotReady, time.Minute, "ClusterProviderConfig %q not found", name)
		}
		return nil, &stageError{reason: b2v1.ReasonProviderConfigNotReady, message: err.Error(), err: err}
	}
	if pc.Spec.Partner != nil && !partner {
		return nil, waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute,
			"ClusterProviderConfig %q is a Partner API config holding the Group admin's master key; it only provisions B2Accounts. Use the provider config of a B2Account (or another account) instead", name)
	}
	if !meta.IsStatusConditionTrue(pc.Status.Conditions, b2v1.ConditionReady) {
		return nil, waitFor(b2v1.ReasonProviderConfigNotReady, time.Minute, "ClusterProviderConfig %q: %s", name, readyMessage(&pc))
	}
	acct, err := d.Accounts.Get(ctx, &pc)
	if err != nil {
		if errors.Is(err, provider.ErrSecretNotFound) {
			return nil, waitFor(b2v1.ReasonProviderConfigNotReady, time.Minute, "ClusterProviderConfig %q: %v", name, err)
		}
		return nil, providerError("authorizing with B2", err)
	}
	return acct, nil
}

// millisTime converts a B2 timestamp (milliseconds since the epoch) to a
// Kubernetes time, or nil.
func millisTime(ms *int64) *metav1.Time {
	if ms == nil {
		return nil
	}
	t := metav1.NewTime(time.UnixMilli(*ms))
	return &t
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
