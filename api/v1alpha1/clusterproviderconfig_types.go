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

// CredentialsSecretReference locates a B2 application key in a Secret.
type CredentialsSecretReference struct {
	// Namespace of the Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Key in the Secret holding the application key ID.
	// +kubebuilder:default=applicationKeyId
	// +optional
	ApplicationKeyIDKey string `json:"applicationKeyIdKey,omitempty"`

	// Key in the Secret holding the application key.
	// +kubebuilder:default=applicationKey
	// +optional
	ApplicationKeyKey string `json:"applicationKeyKey,omitempty"`
}

// ClusterProviderConfigSpec configures access to one B2 account.
type ClusterProviderConfigSpec struct {
	// CredentialsSecretRef points at the application key the operator uses
	// for this account. It needs listBuckets, readBuckets, writeBuckets and
	// deleteBuckets to manage buckets; listKeys, writeKeys and deleteKeys to
	// manage keys; read/writeBucketEncryption and read/writeBucketRetentions
	// for encryption and Object Lock settings; and every capability it must
	// be able to grant to the keys it creates.
	CredentialsSecretRef CredentialsSecretReference `json:"credentialsSecretRef"`

	// Partner enables provisioning B2 accounts (B2Account) through the
	// Backblaze Partner API. The credentials must be the master application
	// key of the Group admin account.
	// +optional
	Partner *PartnerSettings `json:"partner,omitempty"`

	// APIURL is the B2 authorization endpoint.
	// +kubebuilder:default="https://api.backblazeb2.com"
	// +kubebuilder:validation:Pattern=`^https?://[^\s/]+(:[0-9]+)?/?$`
	// +optional
	APIURL string `json:"apiURL,omitempty"`
}

// PartnerSettings configures Partner API account provisioning.
type PartnerSettings struct {
	// GroupID is the Group that new accounts join. It must be a managed
	// Group with B2 enabled.
	// +kubebuilder:validation:MinLength=1
	GroupID string `json:"groupID"`

	// MemberEmailTemplate generates each account's email address from
	// {customer} and {region}, e.g. "{customer}-{region}@hosting-company.com".
	// B2 requires a unique, well-formed address per account; it does not
	// have to receive mail. Changing the template does not rename existing
	// accounts.
	// +kubebuilder:validation:XValidation:rule="self.contains('{customer}') && self.contains('{region}')",message="memberEmailTemplate must contain {customer} and {region}"
	// +kubebuilder:validation:XValidation:rule="self.matches('^[^@ ]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$')",message="memberEmailTemplate must be an email address with a domain"
	MemberEmailTemplate string `json:"memberEmailTemplate"`
}

// ClusterProviderConfigStatus is the observed state of the account.
type ClusterProviderConfigStatus struct {
	// ObservedGeneration is the generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// AccountID is the B2 account the credentials belong to.
	// +optional
	AccountID string `json:"accountID,omitempty"`

	// S3Endpoint is the account's S3-compatible endpoint URL.
	// +optional
	S3Endpoint string `json:"s3Endpoint,omitempty"`

	// S3Region is the region component of the S3 endpoint.
	// +optional
	S3Region string `json:"s3Region,omitempty"`

	// Capabilities held by the operator's application key.
	// +listType=set
	// +optional
	Capabilities []string `json:"capabilities,omitempty"`

	// KeyExpiresAt is when the operator's application key expires, if ever.
	// +optional
	KeyExpiresAt *metav1.Time `json:"keyExpiresAt,omitempty"`

	// LastAuthorizedTime is when the credentials were last validated.
	// +optional
	LastAuthorizedTime *metav1.Time `json:"lastAuthorizedTime,omitempty"`

	// Conditions describe the current state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=b2pc,categories=b2
// +kubebuilder:printcolumn:name="Account",type=string,JSONPath=`.status.accountID`
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.status.s3Region`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterProviderConfig holds the credentials the operator uses for one B2
// account. Only cluster administrators should be able to create or read it.
type ClusterProviderConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterProviderConfigSpec   `json:"spec"`
	Status ClusterProviderConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterProviderConfigList contains a list of ClusterProviderConfig.
type ClusterProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterProviderConfig `json:"items"`
}
