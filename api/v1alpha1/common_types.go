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

// Condition types used by all resources in this group.
const (
	// ConditionReady is True when the resource is fully reconciled with B2.
	ConditionReady = "Ready"
)

// Condition reasons.
const (
	ReasonReconciled            = "Reconciled"
	ReasonReconciling           = "Reconciling"
	ReasonProviderNotReady      = "ProviderConfigNotReady"
	ReasonInvalidCredentials    = "InvalidCredentials"
	ReasonCredentialsNotFound   = "CredentialsSecretNotFound"
	ReasonPolicyDenied          = "PolicyDenied"
	ReasonBucketNameUnavailable = "BucketNameUnavailable"
	ReasonBucketOwnedElsewhere  = "BucketOwnedElsewhere"
	ReasonBucketExists          = "BucketAlreadyExists"
	ReasonBucketNotReady        = "BucketNotReady"
	ReasonBucketNotFound        = "BucketNotFound"
	ReasonDeletionBlocked       = "DeletionBlocked"
	ReasonSecretConflict        = "SecretConflict"
	ReasonProviderError         = "ProviderError"
	ReasonInvalidSpec           = "InvalidSpec"
	ReasonUnsupported           = "Unsupported"
	ReasonAccountConflict       = "AccountConflict"
	ReasonPartnerAPINotEnabled  = "PartnerAPINotEnabled"
	ReasonPartnerNeedsMasterKey = "PartnerRequiresMasterKey"
	ReasonCredentialsMissing    = "CredentialsMissing"
	ReasonReplicationNotReady   = "ReplicationNotReady"
)

// ProviderConfigReference names the ClusterProviderConfig holding the B2
// account credentials to use.
type ProviderConfigReference struct {
	// Name of the ClusterProviderConfig.
	// +kubebuilder:default=default
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Name string `json:"name,omitempty"`
}

// ProviderConfigName returns the referenced name, defaulting to "default".
func (r ProviderConfigReference) ProviderConfigName() string {
	if r.Name == "" {
		return "default"
	}
	return r.Name
}

// Capability is a B2 application key capability.
// +kubebuilder:validation:Enum=listKeys;writeKeys;deleteKeys;listAllBucketNames;listBuckets;readBuckets;writeBuckets;deleteBuckets;readBucketRetentions;writeBucketRetentions;readBucketEncryption;writeBucketEncryption;readBucketNotifications;writeBucketNotifications;listFiles;readFiles;shareFiles;writeFiles;deleteFiles;readFileLegalHolds;writeFileLegalHolds;readFileRetentions;writeFileRetentions;bypassGovernance;readBucketLogging;writeBucketLogging;readBucketReplications;writeBucketReplications
type Capability string

// KeyManagementCapabilities let a key create, list or delete other keys. A
// key holding them can escalate to the full power of the account, so they are
// denied to tenants by default.
var KeyManagementCapabilities = []Capability{"listKeys", "writeKeys", "deleteKeys"}
