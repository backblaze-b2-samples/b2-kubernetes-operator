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
	"reflect"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

const (
	labelAccount          = "b2.backblaze.com/account"
	annotationAccountUID  = "b2.backblaze.com/account-uid"
	annotationEjectedAt   = "b2.backblaze.com/ejected-at"
	accessPolicyPrefix    = "b2account-"
	credentialWriteTries  = 5
	credentialWriteBackof = 500 * time.Millisecond
)

// B2AccountReconciler provisions B2 accounts through the Partner API.
//
// b2_create_group_member returns the new account's application key exactly
// once, so it is written to the credentials Secret before anything else. If
// that write fails, the key is kept in memory and the write retried on every
// reconcile until it succeeds; it is never logged or put in status. The
// operator never deletes a credentials Secret.
type B2AccountReconciler struct {
	Deps
	// APIReader reads Secrets that are not in the operator's cache.
	APIReader client.Reader

	unsaved sync.Map // types.UID -> *b2.CreateGroupMemberResponse
}

// +kubebuilder:rbac:groups=b2.backblaze.com,resources=b2accounts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=b2accounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=b2accounts/finalizers,verbs=update
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=clusterproviderconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=b2.backblaze.com,resources=b2accesspolicies,verbs=get;list;watch;create;update;patch;delete

func (r *B2AccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var acct b2v1.B2Account
	if err := r.Client.Get(ctx, req.NamespacedName, &acct); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !acct.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &acct)
	}
	if !controllerutil.ContainsFinalizer(&acct, Finalizer) {
		base := acct.DeepCopy()
		controllerutil.AddFinalizer(&acct, Finalizer)
		if err := r.Client.Patch(ctx, &acct, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner)); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}

	orig := acct.DeepCopy()
	res, err := r.reconcile(ctx, &acct)
	acct.Status.ObservedGeneration = acct.Generation
	if perr := patchStatus(ctx, r.Client, &acct, orig); perr != nil {
		if apierrors.IsConflict(perr) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, client.IgnoreNotFound(perr)
	}
	return res, err
}

func (r *B2AccountReconciler) reconcile(ctx context.Context, acct *b2v1.B2Account) (ctrl.Result, error) {
	prev := meta.FindStatusCondition(acct.Status.Conditions, b2v1.ConditionReady)
	setReady := func(reason, msg string) {
		if prev == nil || prev.Reason != reason || prev.Message != msg {
			r.Recorder.Eventf(acct, nil, eventType(reason), reason, "Reconcile", "%s", msg)
		}
		setCondition(&acct.Status.Conditions, acct.Generation, metav1.ConditionFalse, reason, msg)
	}

	// Credentials from an earlier pass that could not be saved come first.
	if err := r.flushUnsaved(ctx, acct); err != nil {
		return result(&stageError{reason: b2v1.ReasonReconciling, message: "storing account credentials: " + err.Error(), err: err}, setReady)
	}

	partner, admin, se := r.resolvePartner(ctx, acct)
	if se != nil {
		return result(se, setReady)
	}
	email := acct.Status.Email
	if email == "" {
		email = memberEmail(partner.Spec.Partner.MemberEmailTemplate, acct.Spec.Customer, string(acct.Spec.Region))
	}
	if se := r.checkConflicts(ctx, acct, email); se != nil {
		return result(se, setReady)
	}

	secretKey := client.ObjectKey{Namespace: acct.Spec.CredentialsSecretRef.Namespace, Name: acct.SecretNameOrDefault()}
	var secret corev1.Secret
	secretExists := true
	if err := r.APIReader.Get(ctx, secretKey, &secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		secretExists = false
	}

	if acct.Status.AccountID == "" {
		se := r.provision(ctx, acct, partner, admin, email, secretKey, &secret, secretExists)
		if se != nil {
			return result(se, setReady)
		}
	}

	if se := r.ensureProviderConfig(ctx, acct, partner); se != nil {
		return result(se, setReady)
	}
	if err := r.ensureAccessPolicy(ctx, acct); err != nil {
		return ctrl.Result{}, err
	}

	var pc b2v1.ClusterProviderConfig
	if err := r.Client.Get(ctx, client.ObjectKey{Name: acct.ProviderConfigNameOrDefault()}, &pc); err == nil &&
		!meta.IsStatusConditionTrue(pc.Status.Conditions, b2v1.ConditionReady) {
		return result(waitFor(b2v1.ReasonProviderNotReady, 30*time.Second, "waiting for ClusterProviderConfig %q to become ready", pc.Name), setReady)
	}
	setCondition(&acct.Status.Conditions, acct.Generation, metav1.ConditionTrue, b2v1.ReasonReconciled,
		fmt.Sprintf("Account %s (%s) is provisioned; use ClusterProviderConfig %q", acct.Status.AccountID, acct.Status.Email, acct.Status.ProviderConfigName))
	return ctrl.Result{RequeueAfter: r.Options.ResyncPeriod}, nil
}

func (r *B2AccountReconciler) resolvePartner(ctx context.Context, acct *b2v1.B2Account) (*b2v1.ClusterProviderConfig, *provider.Account, *stageError) {
	name := acct.Spec.PartnerConfigRef.ProviderConfigName()
	var pc b2v1.ClusterProviderConfig
	if err := r.Client.Get(ctx, client.ObjectKey{Name: name}, &pc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, waitFor(b2v1.ReasonProviderNotReady, time.Minute, "partner ClusterProviderConfig %q not found", name)
		}
		return nil, nil, &stageError{reason: b2v1.ReasonProviderNotReady, message: err.Error(), err: err}
	}
	if pc.Spec.Partner == nil {
		return nil, nil, waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "ClusterProviderConfig %q has no spec.partner settings", name)
	}
	admin, se := r.resolveAccount(ctx, name)
	if se != nil {
		return nil, nil, se
	}
	return &pc, admin, nil
}

// memberEmail renders the partner's email template.
func memberEmail(template, customer, region string) string {
	return strings.NewReplacer("{customer}", customer, "{region}", region).Replace(template)
}

// checkConflicts refuses a resource that would produce the same account as
// another B2Account.
func (r *B2AccountReconciler) checkConflicts(ctx context.Context, acct *b2v1.B2Account, email string) *stageError {
	var all b2v1.B2AccountList
	if err := r.Client.List(ctx, &all); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	for _, other := range all.Items {
		if other.UID == acct.UID {
			continue
		}
		otherEmail := other.Status.Email
		if otherEmail == "" && other.Spec.PartnerConfigRef.ProviderConfigName() == acct.Spec.PartnerConfigRef.ProviderConfigName() &&
			other.Spec.Customer == acct.Spec.Customer && other.Spec.Region == acct.Spec.Region {
			otherEmail = email
		}
		// The older resource keeps the address.
		if strings.EqualFold(otherEmail, email) && (other.CreationTimestamp.Before(&acct.CreationTimestamp) ||
			(other.CreationTimestamp.Equal(&acct.CreationTimestamp) && other.Name < acct.Name)) {
			return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "B2Account %q already uses email %s (customer %s, region %s)",
				other.Name, email, acct.Spec.Customer, acct.Spec.Region)
		}
		if other.ProviderConfigNameOrDefault() == acct.ProviderConfigNameOrDefault() {
			return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "B2Account %q already uses providerConfigName %q", other.Name, acct.ProviderConfigNameOrDefault())
		}
	}
	return nil
}

// provision creates the account, or adopts an existing Group member.
func (r *B2AccountReconciler) provision(ctx context.Context, acct *b2v1.B2Account, partner *b2v1.ClusterProviderConfig, admin *provider.Account,
	email string, secretKey client.ObjectKey, secret *corev1.Secret, secretExists bool) *stageError {
	groupID := partner.Spec.Partner.GroupID
	member, err := admin.Client.FindGroupMember(ctx, groupID, email)
	if err != nil {
		return partnerError("looking up Group member", err)
	}

	ours := secretExists && secret.Annotations[annotationAccountUID] == string(acct.UID)
	storedID, storedKey := string(secret.Data[b2v1.AccountSecretKeyID]), string(secret.Data[b2v1.AccountSecretKey])

	if member != nil {
		if storedID == "" || storedKey == "" {
			return waitFor(b2v1.ReasonCredentialsMissing, 10*time.Minute,
				"a Group member with email %s already exists (account %s) but Secret %s holds no application key for it; "+
					"store one there under %q and %q and set spec.adoptExisting", email, member.AccountID, secretKey, b2v1.AccountSecretKeyID, b2v1.AccountSecretKey)
		}
		if !ours && !acct.Spec.AdoptExisting {
			return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute,
				"a Group member with email %s already exists (account %s); set spec.adoptExisting to manage it", email, member.AccountID)
		}
		auth, err := b2.New(b2.Options{BaseURL: partner.Spec.APIURL, ApplicationKeyID: storedID, ApplicationKey: storedKey}).Authorize(ctx)
		if err != nil {
			return partnerError("verifying stored credentials", err)
		}
		if auth.AccountID != member.AccountID {
			return waitFor(b2v1.ReasonCredentialsMissing, 10*time.Minute,
				"Secret %s holds a key for account %s, not Group member %s", secretKey, auth.AccountID, member.AccountID)
		}
		if err := r.writeCredentials(ctx, acct, secretKey, storedID, storedKey, member, groupID); err != nil {
			return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
		}
		r.recordMember(acct, member, email, secretKey)
		r.Recorder.Eventf(acct, nil, corev1.EventTypeNormal, "Adopted", "Adopt", "Adopted existing account %s (%s)", member.AccountID, email)
		return nil
	}

	// Never overwrite credentials this resource did not write.
	if secretExists && !ours && (storedID != "" || storedKey != "") {
		return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "Secret %s already holds an application key; refusing to overwrite it", secretKey)
	}

	resp, err := admin.Client.CreateGroupMember(ctx, groupID, email, string(acct.Spec.Region))
	if err != nil {
		switch {
		case b2.HasCode(err, "invalid_email"):
			return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "B2 refused email %s; it is invalid or already a Backblaze account outside the Group", email)
		case b2.HasCode(err, "invalid_group_id", "invalid_region"):
			return waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "B2 refused to create the account: %v", err)
		case b2.HasCode(err, "too_many_members"):
			return waitFor(b2v1.ReasonProviderError, 10*time.Minute, "the Group has reached its member limit")
		}
		return partnerError("creating account", err)
	}
	r.unsaved.Store(acct.UID, resp)
	log.FromContext(ctx).Info("created B2 account", "accountID", resp.GroupMember.AccountID, "email", email, "region", resp.GroupMember.Region)
	r.Recorder.Eventf(acct, nil, corev1.EventTypeNormal, "Created", "CreateAccount", "Created account %s (%s) in %s", resp.GroupMember.AccountID, email, resp.GroupMember.Region)

	if err := r.flushUnsaved(ctx, acct); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: "the account was created but its credentials are not stored yet; retrying: " + err.Error(), err: err}
	}
	return nil
}

// flushUnsaved writes credentials held in memory for acct, retrying briefly.
func (r *B2AccountReconciler) flushUnsaved(ctx context.Context, acct *b2v1.B2Account) error {
	v, ok := r.unsaved.Load(acct.UID)
	if !ok {
		return nil
	}
	resp := v.(*b2.CreateGroupMemberResponse)
	secretKey := client.ObjectKey{Namespace: acct.Spec.CredentialsSecretRef.Namespace, Name: acct.SecretNameOrDefault()}
	var err error
	for i := range credentialWriteTries {
		// Detached from the reconcile context: losing this write loses the key.
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err = r.writeCredentials(wctx, acct, secretKey, resp.ApplicationKeyID, resp.ApplicationKey, &resp.GroupMember, resp.GroupMember.GroupID)
		cancel()
		if err == nil {
			r.unsaved.Delete(acct.UID)
			r.recordMember(acct, &resp.GroupMember, resp.GroupMember.Email, secretKey)
			return nil
		}
		time.Sleep(credentialWriteBackof << i)
	}
	return err
}

func (r *B2AccountReconciler) writeCredentials(ctx context.Context, acct *b2v1.B2Account, key client.ObjectKey, keyID, appKey string, m *b2.GroupMember, groupID string) error {
	// The Secret may predate the operator (and so be missing from its
	// label-filtered cache): read it directly.
	s := &corev1.Secret{}
	err := r.APIReader.Get(ctx, key, s)
	exists := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if !exists {
		s = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}, Type: corev1.SecretTypeOpaque}
	}
	if s.Labels == nil {
		s.Labels = map[string]string{}
	}
	if s.Annotations == nil {
		s.Annotations = map[string]string{}
	}
	s.Labels[LabelManagedBy] = ManagedByValue
	s.Labels[labelAccount] = acct.Name
	s.Annotations[annotationAccountUID] = string(acct.UID)
	s.Data = map[string][]byte{
		b2v1.AccountSecretKeyID:      []byte(keyID),
		b2v1.AccountSecretKey:        []byte(appKey),
		b2v1.AccountSecretAccountID:  []byte(m.AccountID),
		b2v1.AccountSecretEmail:      []byte(m.Email),
		b2v1.AccountSecretRegion:     []byte(m.Region),
		b2v1.AccountSecretS3Endpoint: []byte(m.S3Endpoint),
		b2v1.AccountSecretGroupID:    []byte(groupID),
	}
	if exists {
		return r.Client.Update(ctx, s, client.FieldOwner(FieldOwner))
	}
	return r.Client.Create(ctx, s, client.FieldOwner(FieldOwner))
}

func (r *B2AccountReconciler) recordMember(acct *b2v1.B2Account, m *b2.GroupMember, email string, secretKey client.ObjectKey) {
	acct.Status.AccountID = m.AccountID
	acct.Status.Email = email
	acct.Status.GroupID = m.GroupID
	acct.Status.GroupName = m.GroupName
	acct.Status.S3Endpoint = m.S3Endpoint
	acct.Status.CredentialsSecret = secretKey.String()
}

func (r *B2AccountReconciler) ensureProviderConfig(ctx context.Context, acct *b2v1.B2Account, partner *b2v1.ClusterProviderConfig) *stageError {
	name := acct.ProviderConfigNameOrDefault()
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(pc), pc)
	if err == nil && pc.Labels[labelAccount] != acct.Name {
		return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "ClusterProviderConfig %q already exists and is not managed by this B2Account", name)
	}
	if client.IgnoreNotFound(err) != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	want := b2v1.ClusterProviderConfigSpec{
		APIURL: partner.Spec.APIURL,
		CredentialsSecretRef: b2v1.CredentialsSecretReference{
			Namespace:           acct.Spec.CredentialsSecretRef.Namespace,
			Name:                acct.SecretNameOrDefault(),
			ApplicationKeyIDKey: b2v1.AccountSecretKeyID,
			ApplicationKeyKey:   b2v1.AccountSecretKey,
		},
	}
	switch {
	case apierrors.IsNotFound(err):
		pc.Labels = map[string]string{labelAccount: acct.Name, LabelManagedBy: ManagedByValue}
		pc.Spec = want
		if err := r.Client.Create(ctx, pc, client.FieldOwner(FieldOwner)); err != nil {
			return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
		}
	case !reflect.DeepEqual(pc.Spec, want):
		pc.Spec = want
		if err := r.Client.Update(ctx, pc, client.FieldOwner(FieldOwner)); err != nil {
			return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
		}
	}
	acct.Status.ProviderConfigName = name
	return nil
}

func (r *B2AccountReconciler) ensureAccessPolicy(ctx context.Context, acct *b2v1.B2Account) error {
	p := &b2v1.B2AccessPolicy{ObjectMeta: metav1.ObjectMeta{Name: accessPolicyPrefix + acct.Name}}
	if acct.Spec.Access == nil {
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(p), p); err != nil {
			return client.IgnoreNotFound(err)
		}
		if metav1.IsControlledBy(p, acct) {
			return client.IgnoreNotFound(r.Client.Delete(ctx, p))
		}
		return nil
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, p, func() error {
		if p.CreationTimestamp.IsZero() {
			if err := controllerutil.SetControllerReference(acct, p, r.Client.Scheme()); err != nil {
				return err
			}
		} else if !metav1.IsControlledBy(p, acct) {
			return fmt.Errorf("B2AccessPolicy %q exists and is not managed by this B2Account", p.Name)
		}
		p.Labels = map[string]string{labelAccount: acct.Name, LabelManagedBy: ManagedByValue}
		p.Spec = b2v1.B2AccessPolicySpec{
			NamespaceSelector: acct.Spec.Access.NamespaceSelector,
			ProviderConfigs:   []string{acct.ProviderConfigNameOrDefault()},
			Buckets:           acct.Spec.Access.Buckets,
			Keys:              acct.Spec.Access.Keys,
		}
		return nil
	})
	return err
}

func (r *B2AccountReconciler) finalize(ctx context.Context, acct *b2v1.B2Account) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(acct, Finalizer) {
		return ctrl.Result{}, nil
	}
	orig := acct.DeepCopy()
	fail := func(se *stageError) (ctrl.Result, error) {
		res, err := result(se, func(reason, msg string) {
			setCondition(&acct.Status.Conditions, acct.Generation, metav1.ConditionFalse, reason, msg)
		})
		if perr := patchStatus(ctx, r.Client, acct, orig); perr != nil && !apierrors.IsNotFound(perr) {
			log.FromContext(ctx).Error(perr, "updating status during deletion")
		}
		return res, err
	}
	if err := r.flushUnsaved(ctx, acct); err != nil {
		return fail(&stageError{reason: b2v1.ReasonReconciling, message: "storing account credentials: " + err.Error(), err: err})
	}

	pcName := acct.ProviderConfigNameOrDefault()
	var buckets b2v1.BucketList
	var keys b2v1.ApplicationKeyList
	if err := r.Client.List(ctx, &buckets, client.MatchingFields{indexProviderConfig: pcName}); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Client.List(ctx, &keys, client.MatchingFields{indexProviderConfig: pcName}); err != nil {
		return ctrl.Result{}, err
	}
	if n := len(buckets.Items) + len(keys.Items); n > 0 {
		return fail(waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
			"%d Bucket and ApplicationKey resource(s) still use ClusterProviderConfig %q; delete them first", n, pcName))
	}

	if acct.Spec.DeletionPolicy == b2v1.AccountDeletionPolicyEject && acct.Status.AccountID != "" {
		_, admin, se := r.resolvePartner(ctx, acct)
		if se != nil {
			se.message = "cannot eject: " + se.message + " (remove the finalizer to leave the account in the Group)"
			return fail(se)
		}
		err := admin.Client.EjectGroupMember(ctx, acct.Status.GroupID, acct.Status.AccountID)
		if err != nil && !b2.HasCode(err, "invalid_member_account_id") {
			return fail(partnerError("ejecting account", err))
		}
		r.Recorder.Eventf(acct, nil, corev1.EventTypeNormal, "Ejected", "Eject", "Ejected account %s from the Group; it keeps existing on its own", acct.Status.AccountID)
		r.annotateSecret(ctx, acct, annotationEjectedAt, r.now().UTC().Format(time.RFC3339))
	}

	pc := &b2v1.ClusterProviderConfig{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: pcName}, pc); err == nil && pc.Labels[labelAccount] == acct.Name {
		if err := r.Client.Delete(ctx, pc); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
	}

	base := acct.DeepCopy()
	controllerutil.RemoveFinalizer(acct, Finalizer)
	if err := r.Client.Patch(ctx, acct, client.MergeFrom(base), client.FieldOwner(FieldOwner)); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
}

func (r *B2AccountReconciler) annotateSecret(ctx context.Context, acct *b2v1.B2Account, key, value string) {
	var s corev1.Secret
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: acct.Spec.CredentialsSecretRef.Namespace, Name: acct.SecretNameOrDefault()}, &s); err != nil {
		return
	}
	base := s.DeepCopy()
	if s.Annotations == nil {
		s.Annotations = map[string]string{}
	}
	s.Annotations[key] = value
	if err := r.Client.Patch(ctx, &s, client.MergeFrom(base)); err != nil {
		log.FromContext(ctx).Error(err, "annotating credentials Secret")
	}
}

// partnerError is providerError for Partner API calls; B2 reports most
// Partner API refusals as 401, which must not look like bad credentials.
func partnerError(action string, err error) *stageError {
	var credErr *b2.CredentialsError
	if errors.As(err, &credErr) {
		return providerError(action, err)
	}
	if apiErr, ok := b2.AsAPIError(err); ok && !apiErr.Retryable() {
		return waitFor(b2v1.ReasonProviderError, 10*time.Minute, "%s: %v", action, err)
	}
	return providerError(action, err)
}

// SetupWithManager registers the controller.
func (r *B2AccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&b2v1.B2Account{}, builder.WithPredicates(specOrDeletionChanged())).
		Owns(&b2v1.B2AccessPolicy{}).
		Watches(&b2v1.ClusterProviderConfig{}, handler.EnqueueRequestsFromMapFunc(r.accountsForProviderConfig), builder.WithPredicates(readinessChanged())).
		Watches(&b2v1.Bucket{}, handler.EnqueueRequestsFromMapFunc(r.accountsForUser), builder.WithPredicates(onlyDeletes())).
		Watches(&b2v1.ApplicationKey{}, handler.EnqueueRequestsFromMapFunc(r.accountsForUser), builder.WithPredicates(onlyDeletes())).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Named("b2account").
		Complete(r)
}

// accountsForProviderConfig maps a provider config to the accounts that
// generated it or use it as their partner config.
func (r *B2AccountReconciler) accountsForProviderConfig(ctx context.Context, o client.Object) []reconcile.Request {
	var list b2v1.B2AccountList
	if err := r.Client.List(ctx, &list); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, a := range list.Items {
		if a.ProviderConfigNameOrDefault() == o.GetName() || a.Spec.PartnerConfigRef.ProviderConfigName() == o.GetName() {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: a.Name}})
		}
	}
	return out
}

// accountsForUser maps a deleted Bucket or ApplicationKey to the account
// whose provider config it used, which may be waiting to be deleted.
func (r *B2AccountReconciler) accountsForUser(ctx context.Context, o client.Object) []reconcile.Request {
	var pcName string
	switch v := o.(type) {
	case *b2v1.Bucket:
		pcName = v.Spec.ProviderConfigRef.ProviderConfigName()
	case *b2v1.ApplicationKey:
		pcName = v.Spec.ProviderConfigRef.ProviderConfigName()
	}
	var list b2v1.B2AccountList
	if err := r.Client.List(ctx, &list); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, a := range list.Items {
		if a.ProviderConfigNameOrDefault() == pcName && !a.DeletionTimestamp.IsZero() {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: a.Name}})
		}
	}
	return out
}

func onlyDeletes() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
