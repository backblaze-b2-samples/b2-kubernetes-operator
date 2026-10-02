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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

// operatorCapabilities are what the operator's key needs for full function.
// Their absence is reported but does not make the config unusable.
var operatorCapabilities = []string{
	"listBuckets", "writeBuckets", "deleteBuckets", "listKeys", "writeKeys", "deleteKeys",
	"readBucketEncryption", "writeBucketEncryption", "readBucketRetentions", "writeBucketRetentions",
	"readBucketReplications", "writeBucketReplications",
}

// ClusterProviderConfigReconciler validates B2 credentials and publishes the
// account's details in status. It re-validates every ResyncPeriod, which
// also picks up rotated credentials.
type ClusterProviderConfigReconciler struct {
	Deps
}

// +kubebuilder:rbac:groups=b2.backblaze.com,resources=clusterproviderconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=clusterproviderconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *ClusterProviderConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pc b2v1.ClusterProviderConfig
	if err := r.Client.Get(ctx, req.NamespacedName, &pc); err != nil {
		if client.IgnoreNotFound(err) == nil {
			r.Accounts.Forget(req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := pc.DeepCopy()
	res, err := r.reconcile(ctx, &pc)
	pc.Status.ObservedGeneration = pc.Generation
	return r.commitStatus(ctx, &pc, orig, res, err)
}

func (r *ClusterProviderConfigReconciler) reconcile(ctx context.Context, pc *b2v1.ClusterProviderConfig) (ctrl.Result, error) {
	notReady := r.notReady(pc)
	acct, err := r.Accounts.Refresh(ctx, pc)
	if err != nil {
		var credErr *b2.CredentialsError
		switch {
		case errors.Is(err, provider.ErrSecretNotFound):
			return result(waitFor(b2v1.ReasonCredentialsSecretNotFound, time.Minute, "%v", err), notReady)
		case errors.As(err, &credErr):
			return result(waitFor(b2v1.ReasonInvalidCredentials, 5*time.Minute, "B2 rejected the credentials: %v", credErr.Err), notReady)
		case errors.Is(err, provider.ErrInvalidAPIURL):
			return result(waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "%v", err), notReady)
		default:
			return result(providerError("authorizing with B2", err), notReady)
		}
	}

	now := metav1.NewTime(r.now())
	pc.Status.AccountID = acct.AccountID
	pc.Status.S3Endpoint = acct.S3Endpoint
	pc.Status.S3Region = acct.S3Region
	pc.Status.Capabilities = slices.Sorted(slices.Values(acct.Capabilities))
	pc.Status.LastAuthorizedTime = &now
	pc.Status.KeyExpiresAt = millisTime(acct.KeyExpirationMillis)
	pc.Status.KeyType = b2v1.KeyTypeApplication
	if acct.MasterKey {
		pc.Status.KeyType = b2v1.KeyTypeMaster
	}

	if pc.Spec.Partner != nil {
		if se := checkPartnerAccess(acct); se != nil {
			return result(se, notReady)
		}
	}
	msg := fmt.Sprintf("Authorized to B2 account %s", acct.AccountID)
	if missing := missingCapabilities(acct.Capabilities); len(missing) > 0 {
		msg += fmt.Sprintf("; the key lacks %s, so some operations will fail", strings.Join(missing, ", "))
	}
	if pc.Spec.Partner == nil && acct.MasterKey {
		msg += "; this is the account's master key, use a restricted application key instead"
		if prev := meta.FindStatusCondition(pc.Status.Conditions, b2v1.ConditionReady); prev == nil || prev.Message != msg {
			r.Recorder.Eventf(pc, nil, corev1.EventTypeWarning, EventMasterKeyInUse, "Authorize",
				"Bucket and key management is using the account's master key; create an application key with the capabilities the operator needs and use it instead")
		}
	}
	markReady(pc, msg)
	return ctrl.Result{RequeueAfter: r.Options.ResyncPeriod}, nil
}

// checkPartnerAccess checks that a partner config's credentials can use the
// Partner API: the Group admin's master key (whose key ID equals the account
// ID), on an account that sales has enabled.
func checkPartnerAccess(acct *provider.Account) *stageError {
	if !acct.MasterKey {
		return waitFor(b2v1.ReasonPartnerRequiresMasterKey, 10*time.Minute,
			"the Partner API requires the Group admin account's master application key (its key ID equals the account ID %s); these credentials are an application key",
			acct.AccountID)
	}
	if !acct.PartnerAPI {
		return waitFor(b2v1.ReasonPartnerAPINotEnabled, 10*time.Minute,
			"account %s is not enabled for the Backblaze Partner API; it is enabled by Backblaze sales for committed-contract customers, and the credentials must be the Group admin's master application key",
			acct.AccountID)
	}
	return nil
}

func missingCapabilities(have []string) []string {
	var missing []string
	for _, c := range operatorCapabilities {
		if !slices.Contains(have, c) {
			missing = append(missing, c)
		}
	}
	return missing
}

// SetupWithManager registers the controller.
func (r *ClusterProviderConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.ClusterProviderConfig{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Named("clusterproviderconfig").
		Complete(r)
}
