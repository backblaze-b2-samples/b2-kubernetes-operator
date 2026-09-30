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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	b2v1 "github.com/backblaze-b2-samples/b2-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-operator/internal/provider"
)

// operatorCapabilities are what the operator key needs for full function.
var operatorCapabilities = []string{
	"listBuckets", "writeBuckets", "deleteBuckets", "listKeys", "writeKeys", "deleteKeys",
}

// ClusterProviderConfigReconciler validates B2 credentials and publishes the
// account's details in status. It re-validates every ResyncPeriod, which also
// picks up rotated credentials.
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
		if apierrors.IsNotFound(err) {
			r.Registry.Forget(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	orig := pc.DeepCopy()
	res, err := r.reconcile(ctx, &pc)
	pc.Status.ObservedGeneration = pc.Generation
	if perr := patchStatus(ctx, r.Client, &pc, orig); perr != nil {
		return ctrl.Result{}, client.IgnoreNotFound(perr)
	}
	return res, err
}

func (r *ClusterProviderConfigReconciler) reconcile(ctx context.Context, pc *b2v1.ClusterProviderConfig) (ctrl.Result, error) {
	setReady := func(reason, msg string) {
		setCondition(&pc.Status.Conditions, pc.Generation, metav1.ConditionFalse, reason, msg)
	}
	acct, err := r.Registry.Refresh(ctx, pc)
	if err != nil {
		var credErr *b2.CredentialsError
		switch {
		case errors.Is(err, provider.ErrSecretNotFound):
			return result(waitFor(b2v1.ReasonCredentialsNotFound, time.Minute, "%v", err), setReady)
		case errors.As(err, &credErr):
			r.Recorder.Eventf(pc, nil, corev1.EventTypeWarning, b2v1.ReasonInvalidCredentials, "Authorize", "B2 rejected the credentials: %v", credErr.Err)
			return result(waitFor(b2v1.ReasonInvalidCredentials, 5*time.Minute, "B2 rejected the credentials: %v", credErr.Err), setReady)
		case errors.Is(err, provider.ErrInvalidAPIURL):
			return result(waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "%v", err), setReady)
		default:
			return result(providerError("authorizing with B2", err), setReady)
		}
	}

	now := metav1.NewTime(r.now())
	pc.Status.AccountID = acct.AccountID
	pc.Status.S3Endpoint = acct.S3Endpoint
	pc.Status.S3Region = acct.S3Region
	pc.Status.Capabilities = slices.Sorted(slices.Values(acct.Capabilities))
	pc.Status.LastAuthorizedTime = &now
	pc.Status.KeyExpiresAt = nil
	if acct.KeyExpirationMillis != nil {
		t := metav1.NewTime(time.UnixMilli(*acct.KeyExpirationMillis))
		pc.Status.KeyExpiresAt = &t
	}

	if pc.Spec.Partner != nil && !acct.PartnerAPI {
		return result(waitFor(b2v1.ReasonPartnerAPINotEnabled, 10*time.Minute,
			"account %s is not enabled for the Backblaze Partner API; it is enabled by Backblaze sales for committed-contract customers, and the credentials must be the Group admin's master application key",
			acct.AccountID), setReady)
	}

	msg := fmt.Sprintf("Authorized to B2 account %s", acct.AccountID)
	var missing []string
	for _, c := range operatorCapabilities {
		if !slices.Contains(acct.Capabilities, c) {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		msg += fmt.Sprintf("; the key lacks %s, so some operations will fail", strings.Join(missing, ", "))
	}
	setCondition(&pc.Status.Conditions, pc.Generation, metav1.ConditionTrue, b2v1.ReasonReconciled, msg)
	return ctrl.Result{RequeueAfter: r.Options.ResyncPeriod}, nil
}

// SetupWithManager registers the controller.
func (r *ClusterProviderConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.ClusterProviderConfig{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Named("clusterproviderconfig").
		Complete(r)
}
