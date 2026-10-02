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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/remote"
)

// RemoteClusterReconciler checks that remote clusters are reachable with
// their kubeconfig. It re-checks every ResyncPeriod, which also picks up a
// rotated kubeconfig.
type RemoteClusterReconciler struct {
	Deps
	Remote *remote.Registry
}

// +kubebuilder:rbac:groups=b2.backblaze.com,resources=remoteclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=remoteclusters/status,verbs=get;update;patch

func (r *RemoteClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var rc b2v1.RemoteCluster
	if err := r.Client.Get(ctx, req.NamespacedName, &rc); err != nil {
		if client.IgnoreNotFound(err) == nil {
			r.Remote.Forget(req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := rc.DeepCopy()
	res, err := r.reconcile(ctx, &rc)
	rc.Status.ObservedGeneration = rc.Generation
	return r.commitStatus(ctx, &rc, orig, res, err)
}

func (r *RemoteClusterReconciler) reconcile(ctx context.Context, rc *b2v1.RemoteCluster) (ctrl.Result, error) {
	notReady := r.notReady(rc)
	_, version, err := r.Remote.Refresh(ctx, rc)
	switch {
	case errors.Is(err, remote.ErrSecretNotFound):
		return result(waitFor(b2v1.ReasonCredentialsSecretNotFound, time.Minute, "%v", err), notReady)
	case errors.Is(err, remote.ErrInvalidKubeconfig):
		return result(waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "%v", err), notReady)
	case err != nil:
		return result(waitFor(b2v1.ReasonRemoteClusterNotReady, time.Minute, "%v", err), notReady)
	}
	now := metav1.NewTime(r.now())
	rc.Status.ServerVersion = version
	rc.Status.LastCheckedTime = &now
	markReady(rc, "Connected to Kubernetes "+version)
	return ctrl.Result{RequeueAfter: r.Options.ResyncPeriod}, nil
}

// SetupWithManager registers the controller.
func (r *RemoteClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.RemoteCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("remotecluster").
		Complete(r)
}
