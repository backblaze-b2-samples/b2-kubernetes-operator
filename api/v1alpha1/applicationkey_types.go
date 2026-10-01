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

// Keys written to the connection Secret.
const (
	SecretKeyAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	SecretKeyAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	SecretKeyAWSEndpointURL     = "AWS_ENDPOINT_URL"
	SecretKeyAWSRegion          = "AWS_REGION"
	SecretKeyB2KeyID            = "B2_APPLICATION_KEY_ID"
	SecretKeyB2Key              = "B2_APPLICATION_KEY"
	SecretKeyB2BucketName       = "B2_BUCKET_NAME"
	SecretKeyB2NamePrefix       = "B2_NAME_PREFIX"
)

// LocalBucketReference names a Bucket in the same namespace.
type LocalBucketReference struct {
	// Name of the Bucket resource.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// KeyRotation configures automatic replacement of the key.
type KeyRotation struct {
	// Every rotates the key on this interval. Omit to rotate only when the
	// spec changes, the Secret is lost, or the key nears expiry.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1h')",message="rotation interval must be at least 1h"
	// +optional
	Every *metav1.Duration `json:"every,omitempty"`

	// GracePeriod is how long a replaced key stays valid after the Secret is
	// updated, so running workloads can pick up the new key. Defaults to 15m.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s') && duration(self) <= duration('168h')",message="gracePeriod must be between 0s and 168h"
	// +optional
	GracePeriod *metav1.Duration `json:"gracePeriod,omitempty"`
}

// DeliveryTarget sends the key's Secret to another cluster.
type DeliveryTarget struct {
	// RemoteCluster names the RemoteCluster to write the Secret to.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	RemoteCluster string `json:"remoteCluster"`

	// Namespace in the remote cluster.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace"`
}

// SecretTemplate adds metadata to the generated Secret.
type SecretTemplate struct {
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ApplicationKeySpec is the desired B2 application key.
// +kubebuilder:validation:XValidation:rule="!(has(self.bucketRef) && has(self.bucketName))",message="set at most one of bucketRef and bucketName"
// +kubebuilder:validation:XValidation:rule="!has(self.namePrefix) || has(self.bucketRef) || has(self.bucketName)",message="namePrefix requires bucketRef or bucketName"
// +kubebuilder:validation:XValidation:rule="has(self.secretName) == has(oldSelf.secretName) && (!has(self.secretName) || self.secretName == oldSelf.secretName)",message="secretName is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.deliverTo) == has(oldSelf.deliverTo) && (!has(self.deliverTo) || self.deliverTo == oldSelf.deliverTo)",message="deliverTo is immutable"
type ApplicationKeySpec struct {
	// ProviderConfigRef selects the B2 account.
	// +kubebuilder:default={name: default}
	// +optional
	ProviderConfigRef ProviderConfigReference `json:"providerConfigRef,omitempty"`

	// BucketRef restricts the key to the bucket of a Bucket resource in this
	// namespace. This is the recommended way to scope a key.
	// +optional
	BucketRef *LocalBucketReference `json:"bucketRef,omitempty"`

	// BucketName restricts the key to an existing bucket not managed in this
	// namespace. Requires keys.allowExternalBuckets in a B2AccessPolicy.
	// +kubebuilder:validation:MinLength=6
	// +kubebuilder:validation:MaxLength=63
	// +optional
	BucketName string `json:"bucketName,omitempty"`

	// NamePrefix restricts the key to files whose names start with it.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	NamePrefix string `json:"namePrefix,omitempty"`

	// Capabilities granted to the key.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +listType=set
	Capabilities []Capability `json:"capabilities"`

	// ValidFor makes the key expire this long after it is created. The
	// operator replaces the key before it expires. At most 1000 days.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1h') && duration(self) <= duration('24000h')",message="validFor must be between 1h and 24000h (1000 days)"
	// +optional
	ValidFor *metav1.Duration `json:"validFor,omitempty"`

	// Rotation configures automatic key replacement.
	// +optional
	Rotation *KeyRotation `json:"rotation,omitempty"`

	// SecretName is the Secret the key is written to, in this namespace.
	// Defaults to the resource name. The operator will not overwrite a
	// Secret it does not own. Immutable.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// SecretTemplate adds labels and annotations to the Secret.
	// +optional
	SecretTemplate *SecretTemplate `json:"secretTemplate,omitempty"`

	// DeliverTo writes the Secret to a namespace in another cluster instead
	// of this namespace. Must be allowed by keys.allowedDeliveryTargets in a
	// B2AccessPolicy. Immutable.
	// +optional
	DeliverTo *DeliveryTarget `json:"deliverTo,omitempty"`
}

// RetiringKey is a replaced key awaiting revocation.
type RetiringKey struct {
	// KeyID is the B2 application key ID.
	KeyID string `json:"keyID"`
	// RevokeAfter is when the key will be deleted from B2.
	RevokeAfter metav1.Time `json:"revokeAfter"`
}

// ApplicationKeyStatus is the observed state of an ApplicationKey.
type ApplicationKeyStatus struct {
	// ObservedGeneration is the generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// KeyID is the current B2 application key ID.
	// +optional
	KeyID string `json:"keyID,omitempty"`

	// KeyName is the current key's name in B2.
	// +optional
	KeyName string `json:"keyName,omitempty"`

	// BucketID the key is restricted to, if any.
	// +optional
	BucketID string `json:"bucketID,omitempty"`

	// BucketName the key is restricted to, if any.
	// +optional
	BucketName string `json:"bucketName,omitempty"`

	// SecretName holding the key.
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// DeliveredTo is where the Secret is: "local", or
	// "<remote cluster>/<namespace>".
	// +optional
	DeliveredTo string `json:"deliveredTo,omitempty"`

	// CreatedAt is when the current key was created.
	// +optional
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`

	// ExpiresAt is when the current key expires, if ever.
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// LastVerifiedTime is when the current key was last confirmed valid.
	// +optional
	LastVerifiedTime *metav1.Time `json:"lastVerifiedTime,omitempty"`

	// SpecHash identifies the spec the current key was created from.
	// +optional
	SpecHash string `json:"specHash,omitempty"`

	// Serial counts keys created for this resource.
	// +optional
	Serial int64 `json:"serial,omitempty"`

	// PendingKeyName is set while a key is being created, so that a key
	// orphaned by a crash can be found and revoked.
	// +optional
	PendingKeyName string `json:"pendingKeyName,omitempty"`

	// ScheduledRevocation is when the current key will be revoked because it
	// is no longer allowed (policy change, or its Bucket is gone). Cleared if
	// access is restored before then.
	// +optional
	ScheduledRevocation *metav1.Time `json:"scheduledRevocation,omitempty"`

	// RetiringKeys are replaced keys that will be revoked after their grace
	// period.
	// +listType=map
	// +listMapKey=keyID
	// +optional
	RetiringKeys []RetiringKey `json:"retiringKeys,omitempty"`

	// Conditions describe the current state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=b2key,categories=b2
// +kubebuilder:printcolumn:name="Bucket",type=string,JSONPath=`.status.bucketName`
// +kubebuilder:printcolumn:name="Secret",type=string,JSONPath=`.status.secretName`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Expires",type=string,format=date-time,JSONPath=`.status.expiresAt`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ApplicationKey is a scoped B2 application key, delivered to a Secret.
type ApplicationKey struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ApplicationKeySpec   `json:"spec"`
	Status ApplicationKeyStatus `json:"status,omitempty"`
}

// SecretNameOrDefault returns the Secret name the key is written to.
func (k *ApplicationKey) SecretNameOrDefault() string {
	if k.Spec.SecretName != "" {
		return k.Spec.SecretName
	}
	return k.Name
}

// +kubebuilder:object:root=true

// ApplicationKeyList contains a list of ApplicationKey.
type ApplicationKeyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ApplicationKey `json:"items"`
}
