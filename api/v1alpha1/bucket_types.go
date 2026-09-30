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

// BucketType is the B2 bucket visibility.
// +kubebuilder:validation:Enum=allPrivate;allPublic
type BucketType string

const (
	BucketTypeAllPrivate BucketType = "allPrivate"
	BucketTypeAllPublic  BucketType = "allPublic"
)

// DeletionPolicy controls what happens to the B2 bucket when the Bucket
// resource is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type DeletionPolicy string

const (
	// DeletionPolicyRetain leaves the bucket and its data in B2. The bucket
	// can be adopted again from the same namespace; adoption from another
	// namespace needs a cluster administrator to clear the release marker.
	DeletionPolicyRetain DeletionPolicy = "Retain"
	// DeletionPolicyDelete deletes the bucket. B2 only deletes empty buckets;
	// the resource stays in Terminating until the bucket is emptied.
	DeletionPolicyDelete DeletionPolicy = "Delete"
)

// LifecycleRule hides and deletes file versions by age. See
// https://www.backblaze.com/docs/cloud-storage-lifecycle-rules.
// +kubebuilder:validation:XValidation:rule="has(self.daysFromUploadingToHiding) || has(self.daysFromHidingToDeleting) || has(self.daysFromStartingToCancelingUnfinishedLargeFiles)",message="a lifecycle rule must set at least one of the day counts"
type LifecycleRule struct {
	// FileNamePrefix selects the files the rule applies to. Empty applies
	// to every file in the bucket.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	FileNamePrefix string `json:"fileNamePrefix"`

	// DaysFromUploadingToHiding hides files this many days after upload.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DaysFromUploadingToHiding *int32 `json:"daysFromUploadingToHiding,omitempty"`

	// DaysFromHidingToDeleting deletes hidden file versions this many days
	// after they were hidden.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DaysFromHidingToDeleting *int32 `json:"daysFromHidingToDeleting,omitempty"`

	// DaysFromStartingToCancelingUnfinishedLargeFiles cancels large file
	// uploads that are still unfinished this many days after they started.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DaysFromStartingToCancelingUnfinishedLargeFiles *int32 `json:"daysFromStartingToCancelingUnfinishedLargeFiles,omitempty"`
}

// CORSOperation is a B2 or S3 operation a CORS rule can allow.
// +kubebuilder:validation:Enum=b2_download_file_by_name;b2_download_file_by_id;b2_upload_file;b2_upload_part;s3_delete;s3_get;s3_head;s3_post;s3_put
type CORSOperation string

// CORSRule allows cross-origin browser requests to the bucket.
type CORSRule struct {
	// Name identifies the rule.
	// +kubebuilder:validation:MinLength=6
	// +kubebuilder:validation:MaxLength=50
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9-]+$`
	Name string `json:"name"`

	// AllowedOrigins are origins such as "https://app.example.com", or "*".
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	AllowedOrigins []string `json:"allowedOrigins"`

	// AllowedOperations are the operations browsers may perform.
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	AllowedOperations []CORSOperation `json:"allowedOperations"`

	// AllowedHeaders browsers may send in preflighted requests.
	// +listType=set
	// +optional
	AllowedHeaders []string `json:"allowedHeaders,omitempty"`

	// ExposeHeaders browsers may read from responses.
	// +listType=set
	// +optional
	ExposeHeaders []string `json:"exposeHeaders,omitempty"`

	// MaxAgeSeconds browsers may cache a preflight response.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:default=3600
	// +optional
	MaxAgeSeconds int32 `json:"maxAgeSeconds,omitempty"`
}

// EncryptionMode is a bucket default server-side encryption mode.
// +kubebuilder:validation:Enum=None;SSE-B2
type EncryptionMode string

const (
	EncryptionModeNone  EncryptionMode = "None"
	EncryptionModeSSEB2 EncryptionMode = "SSE-B2"
)

// DefaultEncryption is the bucket's default server-side encryption.
type DefaultEncryption struct {
	// Mode is SSE-B2 (Backblaze-managed keys, AES-256) or None.
	Mode EncryptionMode `json:"mode"`
}

// RetentionMode is an Object Lock retention mode.
// +kubebuilder:validation:Enum=governance;compliance
type RetentionMode string

const (
	RetentionModeGovernance RetentionMode = "governance"
	RetentionModeCompliance RetentionMode = "compliance"
)

// RetentionUnit is an Object Lock period unit.
// +kubebuilder:validation:Enum=days;years
type RetentionUnit string

// DefaultRetention is applied to new files uploaded to the bucket.
type DefaultRetention struct {
	// Mode is governance (can be bypassed with bypassGovernance) or
	// compliance (cannot be shortened or removed by anyone).
	Mode RetentionMode `json:"mode"`

	// Duration of the retention period, in Unit.
	// +kubebuilder:validation:Minimum=1
	Duration int32 `json:"duration"`

	// Unit of Duration.
	Unit RetentionUnit `json:"unit"`
}

// ObjectLock configures B2 Object Lock (immutability).
// +kubebuilder:validation:XValidation:rule="!has(self.defaultRetention) || self.enabled",message="defaultRetention requires enabled: true"
type ObjectLock struct {
	// Enabled turns on Object Lock. It cannot be turned off again.
	// +kubebuilder:validation:XValidation:rule="!oldSelf || self",message="Object Lock cannot be disabled once enabled"
	Enabled bool `json:"enabled"`

	// DefaultRetention for new files. Omit to have no default retention.
	// +optional
	DefaultRetention *DefaultRetention `json:"defaultRetention,omitempty"`
}

// BucketSpec is the desired state of a B2 bucket. Lifecycle rules, CORS
// rules and bucket info are authoritative: whatever is in B2 is replaced by
// the spec. Default encryption and Object Lock are only managed when set.
type BucketSpec struct {
	// ProviderConfigRef selects the B2 account.
	// +kubebuilder:default={name: default}
	// +optional
	ProviderConfigRef ProviderConfigReference `json:"providerConfigRef,omitempty"`

	// BucketName is the globally unique B2 bucket name. Immutable.
	// +kubebuilder:validation:MinLength=6
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9.-]*[A-Za-z0-9]$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="bucketName is immutable"
	// +kubebuilder:validation:XValidation:rule="!self.lowerAscii().startsWith('b2-')",message="bucket names may not start with b2-"
	// +kubebuilder:validation:XValidation:rule="!self.contains('..')",message="bucket names may not contain adjacent periods"
	// +kubebuilder:validation:XValidation:rule="!self.matches('^[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+$')",message="bucket names may not look like an IPv4 address"
	BucketName string `json:"bucketName"`

	// BucketType is allPrivate (default) or allPublic.
	// +kubebuilder:default=allPrivate
	// +optional
	BucketType BucketType `json:"bucketType,omitempty"`

	// BucketInfo is user metadata stored with the bucket. B2 lowercases keys,
	// so keys must be lowercase. One entry is used by the operator.
	// +kubebuilder:validation:MaxProperties=9
	// +kubebuilder:validation:XValidation:rule="self.all(k, k == k.lowerAscii())",message="bucketInfo keys must be lowercase"
	// +kubebuilder:validation:XValidation:rule="!('b2operator-owner-uid' in self) && !('b2operator-released-from' in self)",message="bucketInfo keys b2operator-owner-uid and b2operator-released-from are reserved"
	// +optional
	BucketInfo map[string]string `json:"bucketInfo,omitempty"`

	// LifecycleRules replace the bucket's lifecycle rules.
	// +kubebuilder:validation:MaxItems=100
	// +listType=atomic
	// +optional
	LifecycleRules []LifecycleRule `json:"lifecycleRules,omitempty"`

	// CORSRules replace the bucket's CORS rules.
	// +kubebuilder:validation:MaxItems=100
	// +listType=map
	// +listMapKey=name
	// +optional
	CORSRules []CORSRule `json:"corsRules,omitempty"`

	// DefaultEncryption, when set, is enforced. When omitted, the bucket's
	// encryption setting is left as it is.
	// +optional
	DefaultEncryption *DefaultEncryption `json:"defaultEncryption,omitempty"`

	// ObjectLock, when set, is enforced. When omitted, the bucket's Object
	// Lock settings are left as they are.
	// +optional
	ObjectLock *ObjectLock `json:"objectLock,omitempty"`

	// DeletionPolicy decides whether deleting this resource deletes the
	// bucket. Defaults to Retain.
	// +kubebuilder:default=Retain
	// +optional
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// AdoptExisting allows taking over a bucket that already exists in the
	// account and is not owned by another resource. Must also be allowed by
	// a B2AccessPolicy. Adoption replaces the bucket's lifecycle rules, CORS
	// rules and bucket info with this spec.
	// +optional
	AdoptExisting bool `json:"adoptExisting,omitempty"`
}

// BucketStatus is the observed state of a Bucket.
type BucketStatus struct {
	// ObservedGeneration is the generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// BucketID is the B2 bucket ID.
	// +optional
	BucketID string `json:"bucketID,omitempty"`

	// Revision is the B2 bucket revision last observed.
	// +optional
	Revision int64 `json:"revision,omitempty"`

	// S3Endpoint is the S3-compatible endpoint for the bucket's account.
	// +optional
	S3Endpoint string `json:"s3Endpoint,omitempty"`

	// S3Region is the region to use with S3-compatible clients.
	// +optional
	S3Region string `json:"s3Region,omitempty"`

	// ObjectLockEnabled reports whether Object Lock is on in B2.
	// +optional
	ObjectLockEnabled bool `json:"objectLockEnabled,omitempty"`

	// LastSyncTime is when the bucket was last compared with B2.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// Conditions describe the current state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=b2bucket,categories=b2
// +kubebuilder:printcolumn:name="Bucket",type=string,JSONPath=`.spec.bucketName`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.bucketType`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Bucket ID",type=string,JSONPath=`.status.bucketID`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Bucket is a B2 bucket managed by the operator.
type Bucket struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BucketSpec   `json:"spec"`
	Status BucketStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BucketList contains a list of Bucket.
type BucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Bucket `json:"items"`
}
