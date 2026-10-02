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
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

const (
	annotationKeyID     = "b2.backblaze.com/key-id"
	annotationExpiresAt = "b2.backblaze.com/expires-at"
	annotationSpecHash  = "b2.backblaze.com/spec-hash"
	annotationCreatedAt = "b2.backblaze.com/created-at"
	labelApplicationKey = "b2.backblaze.com/application-key"

	// annotationOwnerUID marks a Secret in a remote cluster as written for
	// the ApplicationKey with this UID (owner references cannot cross
	// clusters).
	annotationOwnerUID = "b2.backblaze.com/owner-uid"
	// annotationSource records which cluster and resource wrote a remote
	// Secret, for people reading the remote cluster.
	annotationSource = "b2.backblaze.com/source"
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
		return "local"
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
	// Not in the label-filtered cache (or no cache): read directly, so a
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

// claim marks a Secret as owned by key.
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

// failed wraps a Secret read or write error. For a remote cluster it becomes
// RemoteClusterNotReady, so the condition points at the cluster.
func (s *secretStore) failed(err error) *stageError {
	var se *stageError
	if errors.As(err, &se) {
		return se
	}
	if s.remote == "" {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	return waitFor(b2v1.ReasonRemoteClusterNotReady, 30*time.Second, "RemoteCluster %q: %v", s.remote, err)
}

// store returns where key's Secret lives, checking that a remote target is
// registered and reachable.
func (r *ApplicationKeyReconciler) store(ctx context.Context, key *b2v1.ApplicationKey) (*secretStore, *stageError) {
	dst := key.Spec.DeliverTo
	if dst == nil {
		return &secretStore{writer: r.Client, cached: r.Client, reader: r.APIReader, namespace: key.Namespace}, nil
	}
	var rc b2v1.RemoteCluster
	if err := r.Client.Get(ctx, client.ObjectKey{Name: dst.RemoteCluster}, &rc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, waitFor(b2v1.ReasonRemoteClusterNotReady, time.Minute, "RemoteCluster %q not found", dst.RemoteCluster)
		}
		return nil, &stageError{reason: b2v1.ReasonRemoteClusterNotReady, message: err.Error(), err: err}
	}
	if !isReady(&rc) {
		return nil, waitFor(b2v1.ReasonRemoteClusterNotReady, time.Minute, "RemoteCluster %q: %s", dst.RemoteCluster, readyMessage(&rc))
	}
	c, err := r.Remote.Get(ctx, &rc)
	if err != nil {
		return nil, waitFor(b2v1.ReasonRemoteClusterNotReady, time.Minute, "RemoteCluster %q: %v", dst.RemoteCluster, err)
	}
	return &secretStore{
		writer: c, reader: c, namespace: dst.Namespace, remote: dst.RemoteCluster,
		source: fmt.Sprintf("%s/%s/%s", r.Options.ClusterID, key.Namespace, key.Name),
	}, nil
}

// ownedSecret returns the key's Secret, nil if it does not exist, or a
// SecretConflict error if a Secret of that name exists but is not ours.
func (r *ApplicationKeyReconciler) ownedSecret(ctx context.Context, key *b2v1.ApplicationKey, store *secretStore) (*corev1.Secret, *stageError) {
	name := key.SecretNameOrDefault()
	sec, err := store.get(ctx, name)
	if err != nil {
		return nil, store.failed(fmt.Errorf("reading Secret %s: %w", name, err))
	}
	if sec != nil && !store.owns(sec, key) {
		return nil, waitFor(b2v1.ReasonSecretConflict, 5*time.Minute,
			"Secret %q already exists in %s and is not owned by this ApplicationKey; delete it or set spec.secretName", name, store.where())
	}
	return sec, nil
}

// writeSecret stores a newly created key, creating or replacing the Secret.
func (r *ApplicationKeyReconciler) writeSecret(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, store *secretStore,
	existing *corev1.Secret, scope keyScope, created *b2.ApplicationKey, specHash string) error {
	annotations := map[string]string{
		annotationKeyID:     created.ApplicationKeyID,
		annotationSpecHash:  specHash,
		annotationCreatedAt: r.now().UTC().Format(time.RFC3339),
	}
	if exp := millisTime(created.ExpirationTimestamp); exp != nil {
		annotations[annotationExpiresAt] = exp.UTC().Format(time.RFC3339)
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.SecretNameOrDefault(), Namespace: store.namespace},
		Type:       corev1.SecretTypeOpaque,
	}
	if existing != nil {
		sec = existing.DeepCopy()
	}
	applySecretMeta(sec, key, annotations)
	if err := store.claim(sec, key); err != nil {
		return err
	}
	sec.Data = secretData(acct, key, scope, []byte(created.ApplicationKeyID), []byte(created.ApplicationKey))
	if existing == nil {
		return store.writer.Create(ctx, sec, client.FieldOwner(FieldOwner))
	}
	return store.writer.Update(ctx, sec, client.FieldOwner(FieldOwner))
}

// syncSecret keeps the non-credential parts of an existing Secret current:
// endpoint, region, bucket and the template's labels and annotations.
func (r *ApplicationKeyReconciler) syncSecret(ctx context.Context, acct *provider.Account, key *b2v1.ApplicationKey, store *secretStore,
	sec *corev1.Secret, scope keyScope) error {
	annotations := map[string]string{annotationKeyID: key.Status.KeyID, annotationSpecHash: key.Status.SpecHash}
	if key.Status.CreatedAt != nil {
		annotations[annotationCreatedAt] = key.Status.CreatedAt.UTC().Format(time.RFC3339)
	}
	if key.Status.ExpiresAt != nil {
		annotations[annotationExpiresAt] = key.Status.ExpiresAt.UTC().Format(time.RFC3339)
	}
	want := sec.DeepCopy()
	applySecretMeta(want, key, annotations)
	if err := store.claim(want, key); err != nil {
		return err
	}
	want.Data = secretData(acct, key, scope, sec.Data[b2v1.SecretKeyB2KeyID], sec.Data[b2v1.SecretKeyB2Key])
	unchanged := maps.EqualFunc(want.Data, sec.Data, func(a, b []byte) bool { return string(a) == string(b) }) &&
		maps.Equal(want.Labels, sec.Labels) && maps.Equal(want.Annotations, sec.Annotations)
	if unchanged {
		return nil
	}
	return store.writer.Update(ctx, want, client.FieldOwner(FieldOwner))
}

// deleteOwnedSecret deletes the key's Secret if this key wrote it.
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

func secretData(acct *provider.Account, key *b2v1.ApplicationKey, scope keyScope, keyID, secret []byte) map[string][]byte {
	data := map[string][]byte{
		b2v1.SecretKeyAWSAccessKeyID:     keyID,
		b2v1.SecretKeyAWSSecretAccessKey: secret,
		b2v1.SecretKeyB2KeyID:            keyID,
		b2v1.SecretKeyB2Key:              secret,
		b2v1.SecretKeyAWSEndpointURL:     []byte(acct.S3Endpoint),
		b2v1.SecretKeyAWSRegion:          []byte(acct.S3Region),
	}
	if scope.bucketName != "" {
		data[b2v1.SecretKeyB2BucketName] = []byte(scope.bucketName)
	}
	if key.Spec.NamePrefix != "" {
		data[b2v1.SecretKeyB2NamePrefix] = []byte(key.Spec.NamePrefix)
	}
	return data
}

// applySecretMeta sets the Secret's labels and annotations: the template's,
// then the operator's own, which win.
func applySecretMeta(sec *corev1.Secret, key *b2v1.ApplicationKey, annotations map[string]string) {
	labels, anns := map[string]string{}, map[string]string{}
	if tpl := key.Spec.SecretTemplate; tpl != nil {
		maps.Copy(labels, tpl.Labels)
		maps.Copy(anns, tpl.Annotations)
	}
	labels[LabelManagedBy] = ManagedByValue
	labels[labelApplicationKey] = key.Name
	maps.Copy(anns, annotations)
	sec.Labels, sec.Annotations = labels, anns
}
