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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

const (
	// annotationOwnerUID marks a Secret in a remote cluster as written for
	// the ApplicationKey with this UID (owner references cannot cross
	// clusters).
	annotationOwnerUID = "b2.backblaze.com/owner-uid"
	// annotationSource records which cluster and resource wrote a remote
	// Secret, for people reading the remote cluster.
	annotationSource = "b2.backblaze.com/source"
	deliveredLocal   = "local"
)

// secretStore is where an ApplicationKey's Secret lives: the key's own
// namespace, or a namespace in a RemoteCluster.
type secretStore struct {
	writer    client.Client
	cached    client.Reader // label-filtered cache; nil for remote clusters
	reader    client.Reader // uncached
	namespace string
	remote    string // RemoteCluster name; empty for the local cluster
	source    string
}

func (s *secretStore) where() string {
	if s.remote == "" {
		return deliveredLocal
	}
	return s.remote + "/" + s.namespace
}

// get returns the Secret, or nil if it does not exist.
func (s *secretStore) get(ctx context.Context, name string) (*corev1.Secret, error) {
	if s.cached != nil {
		var sec corev1.Secret
		err := s.cached.Get(ctx, client.ObjectKey{Namespace: s.namespace, Name: name}, &sec)
		if err == nil {
			return &sec, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
	}
	// Not in the label-filtered cache, or no cache: read directly, so a
	// same-named Secret the operator does not own is never missed.
	return s.getUncached(ctx, name)
}

// getUncached reads the Secret from the API server, or returns nil.
func (s *secretStore) getUncached(ctx context.Context, name string) (*corev1.Secret, error) {
	var sec corev1.Secret
	err := s.reader.Get(ctx, client.ObjectKey{Namespace: s.namespace, Name: name}, &sec)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sec, nil
}

// owns reports whether sec was written for key.
func (s *secretStore) owns(sec *corev1.Secret, key *b2v1.ApplicationKey) bool {
	if s.remote == "" {
		return metav1.IsControlledBy(sec, key)
	}
	return sec.Annotations[annotationOwnerUID] == string(key.UID)
}

// claim marks a new Secret as owned by key.
func (s *secretStore) claim(sec *corev1.Secret, key *b2v1.ApplicationKey) error {
	if s.remote == "" {
		return controllerutil.SetControllerReference(key, sec, s.writer.Scheme())
	}
	if sec.Annotations == nil {
		sec.Annotations = map[string]string{}
	}
	sec.Annotations[annotationOwnerUID] = string(key.UID)
	sec.Annotations[annotationSource] = s.source
	return nil
}

// store returns where key's Secret lives, checking that a remote target is
// reachable.
func (r *ApplicationKeyReconciler) store(ctx context.Context, key *b2v1.ApplicationKey) (*secretStore, *stageError) {
	t := key.Spec.DeliverTo
	if t == nil {
		return &secretStore{writer: r.Client, cached: r.Client, reader: r.APIReader, namespace: key.Namespace}, nil
	}
	var rc b2v1.RemoteCluster
	if err := r.Client.Get(ctx, client.ObjectKey{Name: t.RemoteCluster}, &rc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, waitFor(b2v1.ReasonRemoteClusterNotReady, time.Minute, "RemoteCluster %q not found", t.RemoteCluster)
		}
		return nil, &stageError{reason: b2v1.ReasonRemoteClusterNotReady, message: err.Error(), err: err}
	}
	if !meta.IsStatusConditionTrue(rc.Status.Conditions, b2v1.ConditionReady) {
		msg := "not ready"
		if c := meta.FindStatusCondition(rc.Status.Conditions, b2v1.ConditionReady); c != nil {
			msg = c.Message
		}
		return nil, waitFor(b2v1.ReasonRemoteClusterNotReady, time.Minute, "RemoteCluster %q: %s", t.RemoteCluster, msg)
	}
	if r.Remote == nil {
		return nil, waitFor(b2v1.ReasonRemoteClusterNotReady, 10*time.Minute, "remote delivery is not enabled in this operator")
	}
	c, err := r.Remote.Get(ctx, &rc)
	if err != nil {
		return nil, waitFor(b2v1.ReasonRemoteClusterNotReady, time.Minute, "RemoteCluster %q: %v", t.RemoteCluster, err)
	}
	return &secretStore{
		writer: c, reader: c, namespace: t.Namespace, remote: t.RemoteCluster,
		source: fmt.Sprintf("%s/%s/%s", r.Options.ClusterID, key.Namespace, key.Name),
	}, nil
}

// remoteErr reports whether err came from talking to a remote cluster, so
// callers can surface it as RemoteClusterNotReady.
func remoteErr(s *secretStore, err error) *stageError {
	if s.remote == "" {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	var se *stageError
	if errors.As(err, &se) {
		return se
	}
	return waitFor(b2v1.ReasonRemoteClusterNotReady, 30*time.Second, "writing to RemoteCluster %q: %v", s.remote, err)
}
