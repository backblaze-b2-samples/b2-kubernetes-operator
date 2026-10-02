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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

// conditioned is a resource with a Ready condition.
type conditioned interface {
	client.Object
	GetConditions() *[]metav1.Condition
}

const maxConditionMessage = 32768

func setReady(obj conditioned, status metav1.ConditionStatus, reason, message string) {
	if len(message) > maxConditionMessage {
		message = message[:maxConditionMessage-3] + "..."
	}
	meta.SetStatusCondition(obj.GetConditions(), metav1.Condition{
		Type:               b2v1.ConditionReady,
		Status:             status,
		ObservedGeneration: obj.GetGeneration(),
		Reason:             reason,
		Message:            message,
	})
}

// markReady sets Ready=True.
func markReady(obj conditioned, message string) {
	setReady(obj, metav1.ConditionTrue, b2v1.ReasonReconciled, message)
}

// notReady returns a function that sets Ready=False on obj and emits an event
// when the reason or message changes, so a resource stuck in the same state
// does not flood the event stream.
func (d *Deps) notReady(obj conditioned) func(reason, message string) {
	prev := meta.FindStatusCondition(*obj.GetConditions(), b2v1.ConditionReady)
	return func(reason, message string) {
		if prev == nil || prev.Reason != reason || prev.Message != message {
			d.Recorder.Eventf(obj, nil, eventType(reason), reason, "Reconcile", "%s", message)
		}
		setReady(obj, metav1.ConditionFalse, reason, message)
	}
}

// eventType is Normal for conditions that resolve on their own (waiting on
// another resource) and Warning for anything that needs attention.
func eventType(reason string) string {
	switch reason {
	case b2v1.ReasonBucketNotReady, b2v1.ReasonBucketNotFound, b2v1.ReasonProviderConfigNotReady, b2v1.ReasonReconciling:
		return corev1.EventTypeNormal
	}
	return corev1.EventTypeWarning
}

// readyMessage is the message of obj's Ready condition, for explaining why a
// dependency is not usable.
func readyMessage(obj conditioned) string {
	if c := meta.FindStatusCondition(*obj.GetConditions(), b2v1.ConditionReady); c != nil {
		return c.Message
	}
	return "not ready"
}

// commitStatus writes obj's status (with an optimistic lock against orig)
// and returns the reconcile outcome. A conflict means another write won;
// reconciling again shortly picks it up.
func (d *Deps) commitStatus(ctx context.Context, obj, orig client.Object, res ctrl.Result, err error) (ctrl.Result, error) {
	if perr := d.patchStatus(ctx, obj, orig); perr != nil {
		if apierrors.IsConflict(perr) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, client.IgnoreNotFound(perr)
	}
	return res, err
}

func (d *Deps) patchStatus(ctx context.Context, obj, orig client.Object) error {
	return d.Client.Status().Patch(ctx, obj, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner))
}

// failDeletion records why deletion is blocked and returns the retry. The
// status write is best effort: the object may already be gone.
func (d *Deps) failDeletion(ctx context.Context, obj conditioned, orig client.Object, se *stageError) (ctrl.Result, error) {
	res, err := result(se, func(reason, message string) { setReady(obj, metav1.ConditionFalse, reason, message) })
	if perr := d.patchStatus(ctx, obj, orig); perr != nil && !apierrors.IsNotFound(perr) {
		log.FromContext(ctx).Error(perr, "updating status during deletion")
	}
	return res, err
}

// ensureFinalizer adds the operator's finalizer so B2 resources are cleaned
// up before obj is deleted.
func (d *Deps) ensureFinalizer(ctx context.Context, obj client.Object) error {
	if controllerutil.ContainsFinalizer(obj, Finalizer) {
		return nil
	}
	base := obj.DeepCopyObject().(client.Object)
	controllerutil.AddFinalizer(obj, Finalizer)
	return d.Client.Patch(ctx, obj, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner))
}

// releaseFinalizer removes the finalizer, letting Kubernetes delete obj.
func (d *Deps) releaseFinalizer(ctx context.Context, obj client.Object) (ctrl.Result, error) {
	base := obj.DeepCopyObject().(client.Object)
	controllerutil.RemoveFinalizer(obj, Finalizer)
	err := d.Client.Patch(ctx, obj, client.MergeFrom(base), client.FieldOwner(FieldOwner))
	return ctrl.Result{}, client.IgnoreNotFound(err)
}
