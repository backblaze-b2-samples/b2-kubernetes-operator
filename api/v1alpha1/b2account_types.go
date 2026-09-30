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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Region is a B2 region a Partner API account can be created in.
// +kubebuilder:validation:Enum=us-east;us-west;ca-east;eu-central
type Region string

// AccountDeletionPolicy controls what happens to the B2 account when the
// B2Account resource is deleted. B2 accounts cannot be deleted through the
// Partner API.
// +kubebuilder:validation:Enum=Retain;Eject
type AccountDeletionPolicy string

const (
	// AccountDeletionPolicyRetain leaves the account in the Group.
	AccountDeletionPolicyRetain AccountDeletionPolicy = "Retain"
	// AccountDeletionPolicyEject removes the account from the Group. The
	// account and its data continue to exist, billed on their own.
	AccountDeletionPolicyEject AccountDeletionPolicy = "Eject"
)

// Keys written to an account's credentials Secret, besides the application
// key ID and key.
const (
	AccountSecretKeyID      = "applicationKeyId"
	AccountSecretKey        = "applicationKey"
	AccountSecretAccountID  = "accountId"
	AccountSecretEmail      = "email"
	AccountSecretRegion     = "region"
	AccountSecretS3Endpoint = "s3Endpoint"
	AccountSecretGroupID    = "groupId"
)

// AccountSecretReference is where the account's application key is stored.
type AccountSecretReference struct {
	// Namespace of the Secret. It should be readable only by cluster
	// administrators, e.g. the operator's namespace.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// Name of the Secret. Defaults to b2-account-<resource name>.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Name string `json:"name,omitempty"`
}

// AccountAccess grants namespaces use of the account. The operator
// maintains a B2AccessPolicy named b2account-<resource name> from it.
type AccountAccess struct {
	// NamespaceSelector selects the customer's namespaces.
	NamespaceSelector metav1.LabelSelector `json:"namespaceSelector"`
	// +optional
	Buckets BucketPolicy `json:"buckets,omitempty"`
	// +optional
	Keys KeyPolicy `json:"keys,omitempty"`
}

// B2AccountSpec is a B2 account provisioned through the Partner API for one
// customer in one region.
// +kubebuilder:validation:XValidation:rule="self.customer == oldSelf.customer && self.region == oldSelf.region && self.partnerConfigRef.name == oldSelf.partnerConfigRef.name",message="customer, region and partnerConfigRef are immutable"
type B2AccountSpec struct {
	// PartnerConfigRef names the ClusterProviderConfig of the Group admin
	// account. It must have spec.partner set.
	PartnerConfigRef ProviderConfigReference `json:"partnerConfigRef"`

	// Customer identifies the customer, e.g. a customer number. It is
	// substituted for {customer} in the partner's memberEmailTemplate.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=48
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$`
	Customer string `json:"customer"`

	// Region the account's data lives in.
	Region Region `json:"region"`

	// CredentialsSecretRef is where the account's application key is
	// stored. B2 returns the key only once, so the Secret is never deleted
	// by the operator, whatever the deletion policy.
	CredentialsSecretRef AccountSecretReference `json:"credentialsSecretRef"`

	// ProviderConfigName is the ClusterProviderConfig the operator creates
	// for the account, which Buckets and ApplicationKeys reference.
	// Defaults to the resource name.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ProviderConfigName string `json:"providerConfigName,omitempty"`

	// Access, when set, grants the selected namespaces use of the account.
	// +optional
	Access *AccountAccess `json:"access,omitempty"`

	// DeletionPolicy is Retain (default: keep the account in the Group) or
	// Eject (remove it from the Group; the account keeps existing).
	// +kubebuilder:default=Retain
	// +optional
	DeletionPolicy AccountDeletionPolicy `json:"deletionPolicy,omitempty"`

	// AdoptExisting allows taking over a Group member that already exists
	// with this resource's email address. Its application key must already
	// be in the credentials Secret.
	// +optional
	AdoptExisting bool `json:"adoptExisting,omitempty"`
}

// B2AccountStatus is the observed state of a B2Account.
type B2AccountStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// AccountID of the B2 account.
	// +optional
	AccountID string `json:"accountID,omitempty"`
	// Email the account was created with. Fixed at creation.
	// +optional
	Email string `json:"email,omitempty"`
	// GroupID and GroupName the account belongs to.
	// +optional
	GroupID string `json:"groupID,omitempty"`
	// +optional
	GroupName string `json:"groupName,omitempty"`
	// S3Endpoint for the account's region.
	// +optional
	S3Endpoint string `json:"s3Endpoint,omitempty"`
	// ProviderConfigName is the ClusterProviderConfig created for the account.
	// +optional
	ProviderConfigName string `json:"providerConfigName,omitempty"`
	// CredentialsSecret is namespace/name of the stored application key.
	// +optional
	CredentialsSecret string `json:"credentialsSecret,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=b2acct,categories=b2
// +kubebuilder:printcolumn:name="Customer",type=string,JSONPath=`.spec.customer`
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.region`
// +kubebuilder:printcolumn:name="Account",type=string,JSONPath=`.status.accountID`
// +kubebuilder:printcolumn:name="Email",type=string,JSONPath=`.status.email`,priority=1
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// B2Account is a B2 account created through the Backblaze Partner API for
// one customer in one region. The operator stores its application key, and
// publishes a ClusterProviderConfig for it so Buckets and ApplicationKeys
// can be created in it.
type B2Account struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   B2AccountSpec   `json:"spec"`
	Status B2AccountStatus `json:"status,omitempty"`
}

// ProviderConfigNameOrDefault returns the ClusterProviderConfig name.
func (a *B2Account) ProviderConfigNameOrDefault() string {
	if a.Spec.ProviderConfigName != "" {
		return a.Spec.ProviderConfigName
	}
	return a.Name
}

// SecretNameOrDefault returns the credentials Secret name.
func (a *B2Account) SecretNameOrDefault() string {
	if a.Spec.CredentialsSecretRef.Name != "" {
		return a.Spec.CredentialsSecretRef.Name
	}
	return "b2-account-" + a.Name
}

// +kubebuilder:object:root=true

// B2AccountList contains a list of B2Account.
type B2AccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []B2Account `json:"items"`
}
