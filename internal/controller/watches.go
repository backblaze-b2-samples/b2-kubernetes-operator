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
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

// listRequests lists objects into list and returns a reconcile request for
// each, for mapping one watched object to the resources that depend on it.
func listRequests(ctx context.Context, c client.Reader, list client.ObjectList, opts ...client.ListOption) []reconcile.Request {
	if err := c.List(ctx, list, opts...); err != nil {
		log.FromContext(ctx).Error(err, "listing objects for a watch")
		return nil
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(items))
	for _, item := range items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(item.(client.Object))})
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

// onlyDeletes passes delete events only.
func onlyDeletes() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// readinessChanged passes updates that flip the Ready condition or change
// the spec of a resource others depend on.
func readinessChanged() predicate.Predicate {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		o, ok1 := e.ObjectOld.(conditioned)
		n, ok2 := e.ObjectNew.(conditioned)
		if !ok1 || !ok2 {
			return true
		}
		return isReady(o) != isReady(n) || o.GetGeneration() != n.GetGeneration()
	}}
}

// bucketChanged passes Bucket updates that matter to keys and replication
// peers: readiness, the B2 bucket ID, deletion, and the replication a source
// has in place (destinations wait for its teardown).
func bucketChanged() predicate.Predicate {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		o, ok1 := e.ObjectOld.(*b2v1.Bucket)
		n, ok2 := e.ObjectNew.(*b2v1.Bucket)
		if !ok1 || !ok2 {
			return true
		}
		return isReady(o) != isReady(n) ||
			o.Status.BucketID != n.Status.BucketID ||
			o.DeletionTimestamp.IsZero() != n.DeletionTimestamp.IsZero() ||
			o.Generation != n.Generation ||
			!reflect.DeepEqual(o.Status.Replication, n.Status.Replication)
	}}
}

func isReady(obj conditioned) bool {
	return meta.IsStatusConditionTrue(*obj.GetConditions(), b2v1.ConditionReady)
}
