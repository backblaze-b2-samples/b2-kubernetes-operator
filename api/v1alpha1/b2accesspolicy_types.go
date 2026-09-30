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

// BucketPolicy limits which buckets namespaces may manage or reference.
type BucketPolicy struct {
	// NamePatterns are glob patterns (`*` matches any run of characters) that
	// bucket names must match. The literal `{namespace}` is replaced with the
	// requesting namespace, e.g. `acme-{namespace}-*`. A name must match at
	// least one pattern to be created, adopted, or referenced by a key
	// through spec.bucketName. An empty list allows no buckets.
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	NamePatterns []string `json:"namePatterns,omitempty"`

	// AllowPublic permits bucketType allPublic.
	// +optional
	AllowPublic bool `json:"allowPublic,omitempty"`

	// AllowAdoption permits spec.adoptExisting, taking over management of a
	// bucket that already exists in the account and matches NamePatterns.
	// +optional
	AllowAdoption bool `json:"allowAdoption,omitempty"`

	// AllowDeletion permits deletionPolicy Delete, which deletes the B2
	// bucket (if empty) when the Bucket resource is deleted.
	// +optional
	AllowDeletion bool `json:"allowDeletion,omitempty"`

	// AllowUnencrypted permits defaultEncryption mode None.
	// +optional
	AllowUnencrypted bool `json:"allowUnencrypted,omitempty"`

	// AllowReplication permits Cloud Replication rules. Replication to
	// another region or account incurs storage in the destination.
	// +optional
	AllowReplication bool `json:"allowReplication,omitempty"`

	// AllowComplianceRetention permits Object Lock default retention in
	// compliance mode. Compliance-mode files cannot be deleted by anyone
	// until their retention expires, so this can lock in storage costs.
	// +optional
	AllowComplianceRetention bool `json:"allowComplianceRetention,omitempty"`
}

// KeyPolicy limits the application keys namespaces may create.
type KeyPolicy struct {
	// AllowedCapabilities is the set of capabilities keys may be granted.
	// The key-management capabilities (listKeys, writeKeys, deleteKeys) are
	// additionally blocked by the operator unless it runs with
	// --allow-key-management-capabilities.
	// +listType=set
	// +optional
	AllowedCapabilities []Capability `json:"allowedCapabilities,omitempty"`

	// AllowAccountWide permits keys that are not restricted to a bucket.
	// +optional
	AllowAccountWide bool `json:"allowAccountWide,omitempty"`

	// AllowExternalBuckets permits keys for buckets that are not managed by
	// a Bucket resource in the same namespace (spec.bucketName). The bucket
	// name must still match buckets.namePatterns.
	// +optional
	AllowExternalBuckets bool `json:"allowExternalBuckets,omitempty"`

	// MaxValidity, if set, requires every key to set spec.validFor no longer
	// than this duration.
	// +optional
	MaxValidity *metav1.Duration `json:"maxValidity,omitempty"`
}

// B2AccessPolicySpec grants the namespaces selected by NamespaceSelector
// permission to use B2 through the listed provider configs.
type B2AccessPolicySpec struct {
	// NamespaceSelector selects the namespaces this policy applies to. An
	// empty selector selects every namespace.
	NamespaceSelector metav1.LabelSelector `json:"namespaceSelector"`

	// ProviderConfigs are the ClusterProviderConfig names the selected
	// namespaces may use. "*" allows all.
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	ProviderConfigs []string `json:"providerConfigs"`

	// Buckets limits bucket management.
	// +optional
	Buckets BucketPolicy `json:"buckets,omitempty"`

	// Keys limits application key creation.
	// +optional
	Keys KeyPolicy `json:"keys,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=b2policy,categories=b2
// +kubebuilder:printcolumn:name="Providers",type=string,JSONPath=`.spec.providerConfigs`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// B2AccessPolicy is a cluster-scoped allow-list that controls what B2
// resources each namespace may request. The operator denies any Bucket or
// ApplicationKey request that no policy fully allows; a single policy must
// allow the whole request (permissions are not combined across policies).
type B2AccessPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec B2AccessPolicySpec `json:"spec"`
}

// +kubebuilder:object:root=true

// B2AccessPolicyList contains a list of B2AccessPolicy.
type B2AccessPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []B2AccessPolicy `json:"items"`
}
