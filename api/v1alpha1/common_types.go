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

// Condition types used by all resources in this group.
const (
	// ConditionReady is True when the resource is fully reconciled with B2.
	ConditionReady = "Ready"
)

// Condition reasons.
const (
	ReasonReconciled                = "Reconciled"
	ReasonReconciling               = "Reconciling"
	ReasonProviderConfigNotReady    = "ProviderConfigNotReady"
	ReasonInvalidCredentials        = "InvalidCredentials"
	ReasonCredentialsSecretNotFound = "CredentialsSecretNotFound"
	ReasonPolicyDenied              = "PolicyDenied"
	ReasonBucketNameUnavailable     = "BucketNameUnavailable"
	ReasonBucketOwnedElsewhere      = "BucketOwnedElsewhere"
	ReasonBucketAlreadyExists       = "BucketAlreadyExists"
	ReasonBucketNotReady            = "BucketNotReady"
	ReasonBucketNotFound            = "BucketNotFound"
	ReasonDeletionBlocked           = "DeletionBlocked"
	ReasonSecretConflict            = "SecretConflict"
	ReasonProviderError             = "ProviderError"
	ReasonInvalidSpec               = "InvalidSpec"
	ReasonAccountConflict           = "AccountConflict"
	ReasonPartnerAPINotEnabled      = "PartnerAPINotEnabled"
	ReasonPartnerRequiresMasterKey  = "PartnerRequiresMasterKey"
	ReasonCredentialsMissing        = "CredentialsMissing"
	ReasonReplicationNotReady       = "ReplicationNotReady"
	ReasonRemoteClusterNotReady     = "RemoteClusterNotReady"
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

// NameOrDefault returns the referenced name, defaulting to "default".
func (r ProviderConfigReference) NameOrDefault() string {
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

// GetConditions gives the controllers uniform access to each resource's
// status conditions.
func (b *Bucket) GetConditions() *[]metav1.Condition                { return &b.Status.Conditions }
func (k *ApplicationKey) GetConditions() *[]metav1.Condition        { return &k.Status.Conditions }
func (a *B2Account) GetConditions() *[]metav1.Condition             { return &a.Status.Conditions }
func (c *ClusterProviderConfig) GetConditions() *[]metav1.Condition { return &c.Status.Conditions }
func (r *RemoteCluster) GetConditions() *[]metav1.Condition         { return &r.Status.Conditions }
