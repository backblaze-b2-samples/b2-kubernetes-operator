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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/provider"
)

const (
	labelAccount         = "b2.backblaze.com/account"
	annotationAccountUID = "b2.backblaze.com/account-uid"
	annotationEjectedAt  = "b2.backblaze.com/ejected-at"
	accessPolicyPrefix   = "b2account-"
)

// B2AccountReconciler provisions B2 accounts through the Partner API, one
// per customer and region, and publishes a ClusterProviderConfig (and
// optionally a B2AccessPolicy) for each. Credential handling is in
// b2account_credentials.go.
type B2AccountReconciler struct {
	Deps

	// unsaved holds keys returned by b2_create_group_member that could not
	// be stored yet, by B2Account UID.
	unsaved sync.Map
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
	if err := r.ensureFinalizer(ctx, &acct); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := acct.DeepCopy()
	res, err := r.reconcile(ctx, &acct)
	acct.Status.ObservedGeneration = acct.Generation
	return r.commitStatus(ctx, &acct, orig, res, err)
}

func (r *B2AccountReconciler) reconcile(ctx context.Context, acct *b2v1.B2Account) (ctrl.Result, error) {
	notReady := r.notReady(acct)
	// Credentials from an earlier pass that could not be stored come first.
	if err := r.flushUnsaved(ctx, acct); err != nil {
		return result(&stageError{reason: b2v1.ReasonReconciling, message: "storing account credentials: " + err.Error(), err: err}, notReady)
	}
	partner, admin, se := r.resolvePartner(ctx, acct)
	if se != nil {
		return result(se, notReady)
	}
	email := acct.Status.Email
	if email == "" {
		email = memberEmail(partner.Spec.Partner.MemberEmailTemplate, acct.Spec.Customer, string(acct.Spec.Region))
	}
	if se := r.checkConflicts(ctx, acct, email); se != nil {
		return result(se, notReady)
	}
	if acct.Status.AccountID == "" {
		if se := r.provision(ctx, acct, partner, admin, email); se != nil {
			return result(se, notReady)
		}
	}
	if se := r.ensureOperationsKey(ctx, acct, partner.Spec.APIURL); se != nil {
		return result(se, notReady)
	}
	if se := r.ensureProviderConfig(ctx, acct, partner.Spec.APIURL); se != nil {
		return result(se, notReady)
	}
	if err := r.ensureAccessPolicy(ctx, acct); err != nil {
		return ctrl.Result{}, err
	}

	var pc b2v1.ClusterProviderConfig
	if err := r.Client.Get(ctx, client.ObjectKey{Name: acct.ProviderConfigNameOrDefault()}, &pc); err == nil && !isReady(&pc) {
		return result(waitFor(b2v1.ReasonProviderConfigNotReady, 30*time.Second, "waiting for ClusterProviderConfig %q to become ready", pc.Name), notReady)
	}
	markReady(acct, fmt.Sprintf("Account %s (%s) is provisioned; use ClusterProviderConfig %q", acct.Status.AccountID, acct.Status.Email, acct.Status.ProviderConfigName))
	return ctrl.Result{RequeueAfter: r.Options.ResyncPeriod}, nil
}

// resolvePartner returns the partner config and its Group admin account.
func (r *B2AccountReconciler) resolvePartner(ctx context.Context, acct *b2v1.B2Account) (*b2v1.ClusterProviderConfig, *provider.Account, *stageError) {
	name := acct.Spec.PartnerConfigRef.NameOrDefault()
	var pc b2v1.ClusterProviderConfig
	if err := r.Client.Get(ctx, client.ObjectKey{Name: name}, &pc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, waitFor(b2v1.ReasonProviderConfigNotReady, time.Minute, "partner ClusterProviderConfig %q not found", name)
		}
		return nil, nil, &stageError{reason: b2v1.ReasonProviderConfigNotReady, message: err.Error(), err: err}
	}
	if pc.Spec.Partner == nil {
		return nil, nil, waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "ClusterProviderConfig %q has no spec.partner settings", name)
	}
	admin, se := r.resolvePartnerAccount(ctx, name)
	if se != nil {
		return nil, nil, se
	}
	return &pc, admin, nil
}

// memberEmail renders the partner's email template.
func memberEmail(template, customer, region string) string {
	return strings.NewReplacer("{customer}", customer, "{region}", region).Replace(template)
}

// checkConflicts refuses a resource that would produce the same account, or
// the same provider config name, as an older B2Account.
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
		if otherEmail == "" && other.Spec.PartnerConfigRef.NameOrDefault() == acct.Spec.PartnerConfigRef.NameOrDefault() &&
			other.Spec.Customer == acct.Spec.Customer && other.Spec.Region == acct.Spec.Region {
			otherEmail = email
		}
		older := other.CreationTimestamp.Before(&acct.CreationTimestamp) ||
			(other.CreationTimestamp.Equal(&acct.CreationTimestamp) && other.Name < acct.Name)
		if strings.EqualFold(otherEmail, email) && older {
			return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "B2Account %q already uses email %s (customer %s, region %s)",
				other.Name, email, acct.Spec.Customer, acct.Spec.Region)
		}
		if other.ProviderConfigNameOrDefault() == acct.ProviderConfigNameOrDefault() {
			return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "B2Account %q already uses providerConfigName %q", other.Name, acct.ProviderConfigNameOrDefault())
		}
	}
	return nil
}

// provision creates the account, or adopts an existing Group member whose
// key is already stored.
func (r *B2AccountReconciler) provision(ctx context.Context, acct *b2v1.B2Account, partner *b2v1.ClusterProviderConfig, admin *provider.Account, email string) *stageError {
	groupID := partner.Spec.Partner.GroupID
	member, err := admin.Client.FindGroupMember(ctx, groupID, email)
	if err != nil {
		return partnerError("looking up Group member", err)
	}
	stored, err := r.readCredentials(ctx, acct)
	if err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	var storedID, storedKey string
	ours := false
	if stored != nil {
		storedID, storedKey = string(stored.Data[b2v1.AccountSecretKeyID]), string(stored.Data[b2v1.AccountSecretKey])
		ours = stored.Annotations[annotationAccountUID] == string(acct.UID)
	}

	if member != nil {
		if member.GroupID == "" {
			member.GroupID = groupID
		}
		return r.adopt(ctx, acct, partner.Spec.APIURL, member, storedID, storedKey, ours)
	}
	// Never overwrite credentials this resource did not write.
	if !ours && (storedID != "" || storedKey != "") {
		return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "Secret %s already holds an application key; refusing to overwrite it", credentialsKey(acct))
	}

	created, err := admin.Client.CreateGroupMember(ctx, groupID, email, string(acct.Spec.Region))
	switch {
	case b2.HasCode(err, b2.CodeInvalidEmail):
		return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "B2 refused email %s; it is invalid or already a Backblaze account outside the Group", email)
	case b2.HasCode(err, b2.CodeInvalidGroupID, b2.CodeInvalidRegion):
		return waitFor(b2v1.ReasonInvalidSpec, 10*time.Minute, "B2 refused to create the account: %v", err)
	case b2.HasCode(err, b2.CodeTooManyMembers):
		return waitFor(b2v1.ReasonProviderError, 10*time.Minute, "the Group has reached its member limit")
	case err != nil:
		return partnerError("creating account", err)
	}
	if created.GroupMember.GroupID == "" {
		created.GroupMember.GroupID = groupID
	}
	if created.GroupMember.Email == "" {
		created.GroupMember.Email = email
	}
	r.unsaved.Store(acct.UID, created)
	log.FromContext(ctx).Info("created B2 account", "accountID", created.GroupMember.AccountID, "email", email, "region", created.GroupMember.Region)
	r.Recorder.Eventf(acct, nil, corev1.EventTypeNormal, EventCreated, "CreateAccount", "Created account %s (%s) in %s",
		created.GroupMember.AccountID, email, created.GroupMember.Region)
	if err := r.flushUnsaved(ctx, acct); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: "the account was created but its credentials are not stored yet; retrying: " + err.Error(), err: err}
	}
	return nil
}

// adopt takes over an existing Group member, using the key already stored
// for it after checking that the key belongs to that account.
func (r *B2AccountReconciler) adopt(ctx context.Context, acct *b2v1.B2Account, apiURL string, member *b2.GroupMember, storedID, storedKey string, ours bool) *stageError {
	if storedID == "" || storedKey == "" {
		return waitFor(b2v1.ReasonCredentialsMissing, 10*time.Minute,
			"a Group member with email %s already exists (account %s) but Secret %s holds no application key for it; store one there under %q and %q and set spec.adoptExisting",
			member.Email, member.AccountID, credentialsKey(acct), b2v1.AccountSecretKeyID, b2v1.AccountSecretKey)
	}
	if !ours && !acct.Spec.AdoptExisting {
		return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute,
			"a Group member with email %s already exists (account %s); set spec.adoptExisting to manage it", member.Email, member.AccountID)
	}
	auth, err := r.Accounts.NewClient(apiURL, storedID, storedKey).Authorize(ctx)
	if err != nil {
		return partnerError("verifying stored credentials", err)
	}
	if auth.AccountID != member.AccountID {
		return waitFor(b2v1.ReasonCredentialsMissing, 10*time.Minute,
			"Secret %s holds a key for account %s, not Group member %s", credentialsKey(acct), auth.AccountID, member.AccountID)
	}
	if err := r.writeCredentials(ctx, acct, storedID, storedKey, member); err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	recordMember(acct, member)
	r.Recorder.Eventf(acct, nil, corev1.EventTypeNormal, EventAdopted, "Adopt", "Adopted existing account %s (%s)", member.AccountID, member.Email)
	return nil
}

// ensureProviderConfig publishes the account's ClusterProviderConfig,
// pointing at the operations key.
func (r *B2AccountReconciler) ensureProviderConfig(ctx context.Context, acct *b2v1.B2Account, apiURL string) *stageError {
	name := acct.ProviderConfigNameOrDefault()
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(pc), pc)
	if err == nil && pc.Labels[labelAccount] != acct.Name {
		return waitFor(b2v1.ReasonAccountConflict, 10*time.Minute, "ClusterProviderConfig %q already exists and is not managed by this B2Account", name)
	}
	if client.IgnoreNotFound(err) != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	ref := credentialsKey(acct)
	want := b2v1.ClusterProviderConfigSpec{
		APIURL: apiURL,
		CredentialsSecretRef: b2v1.CredentialsSecretReference{
			Namespace:           ref.Namespace,
			Name:                ref.Name,
			ApplicationKeyIDKey: b2v1.AccountSecretOperationsKeyID,
			ApplicationKeyKey:   b2v1.AccountSecretOperationsKey,
		},
	}
	switch {
	case apierrors.IsNotFound(err):
		pc.Labels = map[string]string{labelAccount: acct.Name, LabelManagedBy: ManagedByValue}
		pc.Spec = want
		err = r.Client.Create(ctx, pc, client.FieldOwner(FieldOwner))
	case !reflect.DeepEqual(pc.Spec, want):
		pc.Spec = want
		err = r.Client.Update(ctx, pc, client.FieldOwner(FieldOwner))
	}
	if err != nil {
		return &stageError{reason: b2v1.ReasonReconciling, message: err.Error(), err: err}
	}
	acct.Status.ProviderConfigName = name
	return nil
}

// ensureAccessPolicy maintains the B2AccessPolicy for spec.access, or
// removes the one it created when access is unset.
func (r *B2AccountReconciler) ensureAccessPolicy(ctx context.Context, acct *b2v1.B2Account) error {
	p := &b2v1.B2AccessPolicy{ObjectMeta: metav1.ObjectMeta{Name: accessPolicyPrefix + acct.Name}}
	if acct.Spec.Access == nil {
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(p), p); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !metav1.IsControlledBy(p, acct) {
			return nil
		}
		return client.IgnoreNotFound(r.Client.Delete(ctx, p))
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, p, func() error {
		if !p.CreationTimestamp.IsZero() && !metav1.IsControlledBy(p, acct) {
			return fmt.Errorf("B2AccessPolicy %q exists and is not managed by this B2Account", p.Name)
		}
		p.Labels = map[string]string{labelAccount: acct.Name, LabelManagedBy: ManagedByValue}
		p.Spec = b2v1.B2AccessPolicySpec{
			NamespaceSelector: acct.Spec.Access.NamespaceSelector,
			ProviderConfigs:   []string{acct.ProviderConfigNameOrDefault()},
			Buckets:           acct.Spec.Access.Buckets,
			Keys:              acct.Spec.Access.Keys,
		}
		return controllerutil.SetControllerReference(acct, p, r.Client.Scheme())
	})
	return err
}

// finalize waits until nothing uses the account, ejects it if requested, and
// removes its provider config. The credentials Secret is always kept.
func (r *B2AccountReconciler) finalize(ctx context.Context, acct *b2v1.B2Account) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(acct, Finalizer) {
		return ctrl.Result{}, nil
	}
	orig := acct.DeepCopy()
	fail := func(se *stageError) (ctrl.Result, error) { return r.failDeletion(ctx, acct, orig, se) }

	if err := r.flushUnsaved(ctx, acct); err != nil {
		return fail(&stageError{reason: b2v1.ReasonReconciling, message: "storing account credentials: " + err.Error(), err: err})
	}
	pcName := acct.ProviderConfigNameOrDefault()
	users, err := r.countUsers(ctx, pcName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if users > 0 {
		return fail(waitFor(b2v1.ReasonDeletionBlocked, time.Minute,
			"%d Bucket and ApplicationKey resource(s) still use ClusterProviderConfig %q; delete them first", users, pcName))
	}

	if acct.Spec.DeletionPolicy == b2v1.AccountDeletionPolicyEject && acct.Status.AccountID != "" {
		partner, admin, se := r.resolvePartner(ctx, acct)
		if se != nil {
			se.message = "cannot eject: " + se.message + " (remove the finalizer to leave the account in the Group)"
			return fail(se)
		}
		r.handOff(ctx, acct, partner.Spec.APIURL)
		err := admin.Client.EjectGroupMember(ctx, acct.Status.GroupID, acct.Status.AccountID)
		if err != nil && !b2.HasCode(err, b2.CodeInvalidMemberAccountID) {
			return fail(partnerError("ejecting account", err))
		}
		r.Recorder.Eventf(acct, nil, corev1.EventTypeNormal, EventEjected, "Eject",
			"Ejected account %s from the Group; it keeps existing on its own", acct.Status.AccountID)
	}

	var pc b2v1.ClusterProviderConfig
	if err := r.Client.Get(ctx, client.ObjectKey{Name: pcName}, &pc); err == nil && pc.Labels[labelAccount] == acct.Name {
		if err := r.Client.Delete(ctx, &pc); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
	}
	return r.releaseFinalizer(ctx, acct)
}

// countUsers counts Buckets and ApplicationKeys using a provider config.
func (r *B2AccountReconciler) countUsers(ctx context.Context, pcName string) (int, error) {
	var buckets b2v1.BucketList
	if err := r.Client.List(ctx, &buckets, client.MatchingFields{indexProviderConfig: pcName}); err != nil {
		return 0, err
	}
	var keys b2v1.ApplicationKeyList
	if err := r.Client.List(ctx, &keys, client.MatchingFields{indexProviderConfig: pcName}); err != nil {
		return 0, err
	}
	return len(buckets.Items) + len(keys.Items), nil
}

// partnerError is providerError for Partner API calls. B2 reports most
// Partner API refusals as 401, which must not look like bad credentials.
func partnerError(action string, err error) *stageError {
	var credErr *b2.CredentialsError
	if apiErr, ok := b2.AsAPIError(err); ok && !apiErr.Retryable() && !errors.As(err, &credErr) {
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
		Watches(&b2v1.Bucket{}, handler.EnqueueRequestsFromMapFunc(r.deletingAccountsForUser), builder.WithPredicates(onlyDeletes())).
		Watches(&b2v1.ApplicationKey{}, handler.EnqueueRequestsFromMapFunc(r.deletingAccountsForUser), builder.WithPredicates(onlyDeletes())).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Named("b2account").
		Complete(r)
}

// accountsForProviderConfig maps a provider config to the accounts that
// published it or use it as their partner config.
func (r *B2AccountReconciler) accountsForProviderConfig(ctx context.Context, o client.Object) []reconcile.Request {
	return r.accountsWhere(ctx, func(a *b2v1.B2Account) bool {
		return a.ProviderConfigNameOrDefault() == o.GetName() || a.Spec.PartnerConfigRef.NameOrDefault() == o.GetName()
	})
}

// deletingAccountsForUser maps a deleted Bucket or ApplicationKey to the
// account it used, if that account is waiting to be deleted.
func (r *B2AccountReconciler) deletingAccountsForUser(ctx context.Context, o client.Object) []reconcile.Request {
	var pcName string
	switch v := o.(type) {
	case *b2v1.Bucket:
		pcName = v.Spec.ProviderConfigRef.NameOrDefault()
	case *b2v1.ApplicationKey:
		pcName = v.Spec.ProviderConfigRef.NameOrDefault()
	}
	return r.accountsWhere(ctx, func(a *b2v1.B2Account) bool {
		return a.ProviderConfigNameOrDefault() == pcName && !a.DeletionTimestamp.IsZero()
	})
}

func (r *B2AccountReconciler) accountsWhere(ctx context.Context, match func(*b2v1.B2Account) bool) []reconcile.Request {
	var list b2v1.B2AccountList
	if err := r.Client.List(ctx, &list); err != nil {
		log.FromContext(ctx).Error(err, "listing B2Accounts for a watch")
		return nil
	}
	var out []reconcile.Request
	for i := range list.Items {
		if match(&list.Items[i]) {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
	}
	return out
}
