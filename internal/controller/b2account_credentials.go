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

// Account credentials. b2_create_group_member returns the new account's key
// exactly once, so it is written to the credentials Secret before anything
// else; if that write fails, the key is held in memory and the write retried
// on every reconcile. It is never logged or put in status, and the operator
// never deletes a credentials Secret.
//
// The operator then uses that key once, to create its own application key in
// the account (the "operations key"). The account's ClusterProviderConfig
// uses the operations key, so the key B2 returned serves no day-to-day
// traffic.

import (
	"context"
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

const (
	credentialWriteTries   = 5
	credentialWriteBackoff = 500 * time.Millisecond
)

// credentialsKey is where the account's credentials Secret lives.
func credentialsKey(acct *b2v1.B2Account) client.ObjectKey {
	return client.ObjectKey{Namespace: acct.Spec.CredentialsSecretRef.Namespace, Name: acct.SecretNameOrDefault()}
}

// readCredentials reads the credentials Secret directly (it may predate the
// operator, and so be missing from its label-filtered cache), or returns nil.
func (r *B2AccountReconciler) readCredentials(ctx context.Context, acct *b2v1.B2Account) (*corev1.Secret, error) {
	var s corev1.Secret
	err := r.APIReader.Get(ctx, credentialsKey(acct), &s)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// flushUnsaved stores credentials held in memory for acct, retrying briefly.
func (r *B2AccountReconciler) flushUnsaved(ctx context.Context, acct *b2v1.B2Account) error {
	v, ok := r.unsaved.Load(acct.UID)
	if !ok {
		return nil
	}
	created := v.(*b2.CreateGroupMemberResponse)
	var err error
	for attempt := range credentialWriteTries {
		// Detached from the reconcile's context: losing this write loses the key.
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err = r.writeCredentials(wctx, acct, created.ApplicationKeyID, created.ApplicationKey, &created.GroupMember)
		cancel()
		if err == nil {
			r.unsaved.Delete(acct.UID)
			recordMember(acct, &created.GroupMember)
			return nil
		}
		time.Sleep(credentialWriteBackoff << attempt)
	}
	return err
}

// writeCredentials stores an account key and the account's details in the
// credentials Secret, keeping any operations key already there.
func (r *B2AccountReconciler) writeCredentials(ctx context.Context, acct *b2v1.B2Account, keyID, key string, m *b2.GroupMember) error {
	s, err := r.readCredentials(ctx, acct)
	if err != nil {
		return err
	}
	exists := s != nil
	if !exists {
		ref := credentialsKey(acct)
		s = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name}, Type: corev1.SecretTypeOpaque}
	}
	s.Labels = withEntries(s.Labels, map[string]string{LabelManagedBy: ManagedByValue, labelAccount: acct.Name})
	s.Annotations = withEntries(s.Annotations, map[string]string{annotationAccountUID: string(acct.UID)})
	if s.Data == nil {
		s.Data = map[string][]byte{}
	}
	for k, v := range map[string]string{
		b2v1.AccountSecretKeyID:      keyID,
		b2v1.AccountSecretKey:        key,
		b2v1.AccountSecretAccountID:  m.AccountID,
		b2v1.AccountSecretEmail:      m.Email,
		b2v1.AccountSecretRegion:     m.Region,
		b2v1.AccountSecretS3Endpoint: m.S3Endpoint,
		b2v1.AccountSecretGroupID:    m.GroupID,
	} {
		s.Data[k] = []byte(v)
	}
	if exists {
		return r.Client.Update(ctx, s, client.FieldOwner(FieldOwner))
	}
	return r.Client.Create(ctx, s, client.FieldOwner(FieldOwner))
}

// ensureOperationsKey makes sure the account has the operator's own
// application key, creating it with the stored account key if needed.
func (r *B2AccountReconciler) ensureOperationsKey(ctx context.Context, acct *b2v1.B2Account, apiURL string) *stageError {
	s, err := r.readCredentials(ctx, acct)
	if err != nil || s == nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: fmt.Sprintf("reading Secret %s: %v", credentialsKey(acct), err), err: err}
	}
	accountKeyID, accountKey := string(s.Data[b2v1.AccountSecretKeyID]), string(s.Data[b2v1.AccountSecretKey])
	if accountKeyID == "" || accountKey == "" {
		return waitFor(b2v1.ReasonCredentialsMissing, 10*time.Minute, "Secret %s holds no key for account %s", credentialsKey(acct), acct.Status.AccountID)
	}
	accountClient := r.Accounts.NewClient(apiURL, accountKeyID, accountKey)

	if opsID := string(s.Data[b2v1.AccountSecretOperationsKeyID]); opsID != "" && len(s.Data[b2v1.AccountSecretOperationsKey]) > 0 {
		exists, err := accountClient.KeyExists(ctx, opsID)
		if err != nil {
			return providerError("checking the operations key", err)
		}
		if exists {
			acct.Status.OperationsKeyID = opsID
			return nil
		}
		r.Recorder.Eventf(acct, nil, corev1.EventTypeWarning, EventOperationsKeyMissing, "Reconcile",
			"Operations key %s no longer exists in B2; creating a new one", opsID)
	}

	// A key under our name whose secret half was never stored is useless.
	name := fmt.Sprintf("%s-%s-%s-ops", KeyNamePrefix, r.Options.ClusterID, uid8(acct.UID))
	stale, err := accountClient.FindKeys(ctx, func(k b2.ApplicationKey) bool { return k.KeyName == name })
	if err != nil {
		return providerError("listing keys", err)
	}
	for _, k := range stale {
		if err := accountClient.DeleteKeyIfExists(ctx, k.ApplicationKeyID); err != nil {
			return providerError("revoking a stale operations key", err)
		}
	}
	created, err := accountClient.CreateKey(ctx, b2.CreateKeyRequest{KeyName: name, Capabilities: b2.AllCapabilities})
	if err != nil {
		return providerError("creating the operations key", err)
	}
	base := s.DeepCopy()
	s.Data[b2v1.AccountSecretOperationsKeyID] = []byte(created.ApplicationKeyID)
	s.Data[b2v1.AccountSecretOperationsKey] = []byte(created.ApplicationKey)
	if err := r.Client.Patch(ctx, s, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner)); err != nil {
		_ = accountClient.DeleteKeyIfExists(ctx, created.ApplicationKeyID)
		return &stageError{reason: b2v1.ReasonReconciling, message: "storing the operations key: " + err.Error(), err: err}
	}
	acct.Status.OperationsKeyID = created.ApplicationKeyID
	r.Recorder.Eventf(acct, nil, corev1.EventTypeNormal, EventOperationsKeyCreated, "CreateKey",
		"Created application key %s for managing the account; the key B2 returned is stored but not used for it", created.ApplicationKeyID)
	return nil
}

// handOff prepares the credentials Secret for an ejected account: the
// operator's own key is revoked and removed, the account key is kept, and
// the Secret is annotated with the time.
func (r *B2AccountReconciler) handOff(ctx context.Context, acct *b2v1.B2Account, apiURL string) {
	logger := log.FromContext(ctx)
	s, err := r.readCredentials(ctx, acct)
	if err != nil || s == nil {
		return
	}
	base := s.DeepCopy()
	if opsID := string(s.Data[b2v1.AccountSecretOperationsKeyID]); opsID != "" {
		accountClient := r.Accounts.NewClient(apiURL, string(s.Data[b2v1.AccountSecretKeyID]), string(s.Data[b2v1.AccountSecretKey]))
		if err := accountClient.DeleteKeyIfExists(ctx, opsID); err != nil {
			logger.Error(err, "revoking the operations key before eject", "keyID", opsID)
		} else {
			delete(s.Data, b2v1.AccountSecretOperationsKeyID)
			delete(s.Data, b2v1.AccountSecretOperationsKey)
		}
	}
	s.Annotations = withEntries(s.Annotations, map[string]string{annotationEjectedAt: r.now().UTC().Format(time.RFC3339)})
	if err := r.Client.Patch(ctx, s, client.MergeFrom(base)); err != nil {
		logger.Error(err, "updating the credentials Secret after eject")
	}
}

func recordMember(acct *b2v1.B2Account, m *b2.GroupMember) {
	acct.Status.AccountID = m.AccountID
	acct.Status.Email = m.Email
	acct.Status.GroupID = m.GroupID
	acct.Status.GroupName = m.GroupName
	acct.Status.S3Endpoint = m.S3Endpoint
	acct.Status.CredentialsSecret = credentialsKey(acct).String()
}

// withEntries returns m (allocated if nil) with entries added.
func withEntries(m, entries map[string]string) map[string]string {
	if m == nil {
		m = map[string]string{}
	}
	maps.Copy(m, entries)
	return m
}
