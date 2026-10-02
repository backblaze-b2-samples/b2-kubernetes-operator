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

package b2

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Wire types for the B2 Native API v4. Field names and shapes follow
// https://www.backblaze.com/apidocs. Only the fields the operator uses are
// modelled; unknown fields are ignored on decode.

// BucketType values accepted by b2_create_bucket / b2_update_bucket.
const (
	BucketTypeAllPrivate = "allPrivate"
	BucketTypeAllPublic  = "allPublic"
)

// Server-side encryption modes for a bucket default.
const (
	SSEModeB2       = "SSE-B2"
	SSEAlgorithmAES = "AES256"
)

// Object Lock retention modes and period units.
const (
	RetentionModeGovernance = "governance"
	RetentionModeCompliance = "compliance"
	PeriodUnitDays          = "days"
	PeriodUnitYears         = "years"
)

// Authorization is the decoded response of b2_authorize_account.
type Authorization struct {
	AccountID          string  `json:"accountId"`
	AuthorizationToken string  `json:"authorizationToken"`
	APIInfo            APIInfo `json:"apiInfo"`
	// ApplicationKeyExpirationTimestamp is milliseconds since the epoch, or nil
	// for a key that never expires.
	ApplicationKeyExpirationTimestamp *int64 `json:"applicationKeyExpirationTimestamp"`
}

type APIInfo struct {
	StorageAPI StorageAPI `json:"storageApi"`
	// GroupsAPI is present for accounts enabled for the Partner API.
	GroupsAPI *GroupsAPI `json:"groupsApi,omitempty"`
}

type GroupsAPI struct {
	GroupsAPIURL string   `json:"groupsApiUrl"`
	Capabilities []string `json:"capabilities"`
}

type StorageAPI struct {
	APIURL      string  `json:"apiUrl"`
	DownloadURL string  `json:"downloadUrl"`
	S3APIURL    string  `json:"s3ApiUrl"`
	Allowed     Allowed `json:"allowed"`
}

type Allowed struct {
	Buckets      []AllowedBucket `json:"buckets,omitempty"`
	Capabilities []string        `json:"capabilities"`
	NamePrefix   *string         `json:"namePrefix"`
}

type AllowedBucket struct {
	ID   string  `json:"id"`
	Name *string `json:"name"`
}

// LifecycleRule mirrors a B2 lifecycle rule. Nil day counts mean "unset".
type LifecycleRule struct {
	FileNamePrefix                                  string `json:"fileNamePrefix"`
	DaysFromUploadingToHiding                       *int32 `json:"daysFromUploadingToHiding"`
	DaysFromHidingToDeleting                        *int32 `json:"daysFromHidingToDeleting"`
	DaysFromStartingToCancelingUnfinishedLargeFiles *int32 `json:"daysFromStartingToCancelingUnfinishedLargeFiles,omitempty"`
}

// CORSRule mirrors a B2 CORS rule.
type CORSRule struct {
	CORSRuleName      string   `json:"corsRuleName"`
	AllowedOrigins    []string `json:"allowedOrigins"`
	AllowedOperations []string `json:"allowedOperations"`
	AllowedHeaders    []string `json:"allowedHeaders,omitempty"`
	ExposeHeaders     []string `json:"exposeHeaders,omitempty"`
	MaxAgeSeconds     int32    `json:"maxAgeSeconds"`
}

// ServerSideEncryption is the bucket default encryption setting. A nil Mode
// means "no default encryption" and is serialised as {"mode": null}.
type ServerSideEncryption struct {
	Mode      *string `json:"mode"`
	Algorithm *string `json:"algorithm,omitempty"`
}

// RetentionPeriod is an Object Lock default retention period.
type RetentionPeriod struct {
	Duration int32  `json:"duration"`
	Unit     string `json:"unit"`
}

// DefaultRetention is the Object Lock default retention. A nil Mode clears
// the default and is serialised as {"mode": null}.
type DefaultRetention struct {
	Mode   *string          `json:"mode"`
	Period *RetentionPeriod `json:"period,omitempty"`
}

// ProtectedSSE wraps settings that B2 returns only when the calling key has
// the capability to read them.
type ProtectedSSE struct {
	IsClientAuthorizedToRead bool                  `json:"isClientAuthorizedToRead"`
	Value                    *ServerSideEncryption `json:"value"`
}

type FileLockValue struct {
	IsFileLockEnabled bool              `json:"isFileLockEnabled"`
	DefaultRetention  *DefaultRetention `json:"defaultRetention"`
}

type ProtectedFileLock struct {
	IsClientAuthorizedToRead bool           `json:"isClientAuthorizedToRead"`
	Value                    *FileLockValue `json:"value"`
}

// ReplicationRule is one Cloud Replication rule on a source bucket.
type ReplicationRule struct {
	ReplicationRuleName  string `json:"replicationRuleName"`
	DestinationBucketID  string `json:"destinationBucketId"`
	FileNamePrefix       string `json:"fileNamePrefix"`
	IncludeExistingFiles bool   `json:"includeExistingFiles"`
	IsEnabled            bool   `json:"isEnabled"`
	Priority             int32  `json:"priority"`
}

// ReplicationSource configures a bucket as a replication source. All rules
// on a bucket share one source key.
type ReplicationSource struct {
	ReplicationRules       []ReplicationRule `json:"replicationRules"`
	SourceApplicationKeyID *string           `json:"sourceApplicationKeyId,omitempty"`
}

// ReplicationDestination maps source keys to the destination keys that
// write replicated files into this bucket.
type ReplicationDestination struct {
	SourceToDestinationKeyMapping map[string]string `json:"sourceToDestinationKeyMapping"`
}

// ReplicationConfiguration is a bucket's replication settings. B2 replaces
// the whole configuration on every b2_update_bucket: a side left nil is
// removed, so callers must send both sides they want to keep.
type ReplicationConfiguration struct {
	AsReplicationSource      *ReplicationSource      `json:"asReplicationSource,omitempty"`
	AsReplicationDestination *ReplicationDestination `json:"asReplicationDestination,omitempty"`
}

type ProtectedReplication struct {
	IsClientAuthorizedToRead bool                      `json:"isClientAuthorizedToRead"`
	Value                    *ReplicationConfiguration `json:"value"`
}

// Bucket is a bucket as returned by list/create/update/delete.
type Bucket struct {
	AccountID                   string                `json:"accountId"`
	BucketID                    string                `json:"bucketId"`
	BucketName                  string                `json:"bucketName"`
	BucketType                  string                `json:"bucketType"`
	BucketInfo                  map[string]string     `json:"bucketInfo"`
	CORSRules                   []CORSRule            `json:"corsRules"`
	LifecycleRules              []LifecycleRule       `json:"lifecycleRules"`
	DefaultServerSideEncryption *ProtectedSSE         `json:"defaultServerSideEncryption,omitempty"`
	FileLockConfiguration       *ProtectedFileLock    `json:"fileLockConfiguration,omitempty"`
	ReplicationConfiguration    *ProtectedReplication `json:"replicationConfiguration,omitempty"`
	Revision                    Revision              `json:"revision"`
	Options                     []string              `json:"options,omitempty"`
}

// CreateBucketRequest is the body of b2_create_bucket. AccountID is filled in
// by the client.
type CreateBucketRequest struct {
	AccountID                   string                `json:"accountId"`
	BucketName                  string                `json:"bucketName"`
	BucketType                  string                `json:"bucketType"`
	BucketInfo                  map[string]string     `json:"bucketInfo,omitempty"`
	CORSRules                   []CORSRule            `json:"corsRules,omitempty"`
	LifecycleRules              []LifecycleRule       `json:"lifecycleRules,omitempty"`
	FileLockEnabled             bool                  `json:"fileLockEnabled,omitempty"`
	DefaultServerSideEncryption *ServerSideEncryption `json:"defaultServerSideEncryption,omitempty"`
}

// UpdateBucketRequest is the body of b2_update_bucket. Nil fields are left
// unchanged by B2. Slices are pointers so that "replace with an empty list"
// can be distinguished from "leave unchanged".
type UpdateBucketRequest struct {
	AccountID                   string                    `json:"accountId"`
	BucketID                    string                    `json:"bucketId"`
	BucketType                  string                    `json:"bucketType,omitempty"`
	BucketInfo                  *map[string]string        `json:"bucketInfo,omitempty"`
	CORSRules                   *[]CORSRule               `json:"corsRules,omitempty"`
	LifecycleRules              *[]LifecycleRule          `json:"lifecycleRules,omitempty"`
	FileLockEnabled             *bool                     `json:"fileLockEnabled,omitempty"`
	DefaultRetention            *DefaultRetention         `json:"defaultRetention,omitempty"`
	DefaultServerSideEncryption *ServerSideEncryption     `json:"defaultServerSideEncryption,omitempty"`
	ReplicationConfiguration    *ReplicationConfiguration `json:"replicationConfiguration,omitempty"`
	IfRevisionIs                Revision                  `json:"ifRevisionIs,omitempty"`
}

// ListBucketsRequest is the body of b2_list_buckets. Set at most one of
// BucketID and BucketName to look up a single bucket.
type ListBucketsRequest struct {
	AccountID  string `json:"accountId"`
	BucketID   string `json:"bucketId,omitempty"`
	BucketName string `json:"bucketName,omitempty"`
}

type listBucketsResponse struct {
	Buckets []Bucket `json:"buckets"`
}

type deleteBucketRequest struct {
	AccountID string `json:"accountId"`
	BucketID  string `json:"bucketId"`
}

// CreateKeyRequest is the body of b2_create_key. AccountID is filled in by the
// client.
type CreateKeyRequest struct {
	AccountID              string   `json:"accountId"`
	Capabilities           []string `json:"capabilities"`
	KeyName                string   `json:"keyName"`
	ValidDurationInSeconds int64    `json:"validDurationInSeconds,omitempty"`
	BucketIDs              []string `json:"bucketIds,omitempty"`
	NamePrefix             string   `json:"namePrefix,omitempty"`
}

// ApplicationKey is a key as returned by create/list/delete. ApplicationKey
// (the secret) is only populated in the b2_create_key response.
type ApplicationKey struct {
	AccountID           string   `json:"accountId"`
	ApplicationKeyID    string   `json:"applicationKeyId"`
	ApplicationKey      string   `json:"applicationKey,omitempty"`
	KeyName             string   `json:"keyName"`
	Capabilities        []string `json:"capabilities"`
	BucketIDs           []string `json:"bucketIds,omitempty"`
	NamePrefix          *string  `json:"namePrefix,omitempty"`
	ExpirationTimestamp *int64   `json:"expirationTimestamp,omitempty"`
	Options             []string `json:"options,omitempty"`
}

type deleteKeyRequest struct {
	ApplicationKeyID string `json:"applicationKeyId"`
}

type listKeysRequest struct {
	AccountID             string `json:"accountId"`
	MaxKeyCount           int    `json:"maxKeyCount,omitempty"`
	StartApplicationKeyID string `json:"startApplicationKeyId,omitempty"`
}

type listKeysResponse struct {
	Keys                 []ApplicationKey `json:"keys"`
	NextApplicationKeyID *string          `json:"nextApplicationKeyId"`
}

// AllCapabilities is every application key capability (what a master key holds).
var AllCapabilities = []string{
	"listKeys", "writeKeys", "deleteKeys", "listAllBucketNames", "listBuckets", "readBuckets",
	"writeBuckets", "deleteBuckets", "readBucketRetentions", "writeBucketRetentions",
	"readBucketEncryption", "writeBucketEncryption", "readBucketNotifications",
	"writeBucketNotifications", "listFiles", "readFiles", "shareFiles", "writeFiles", "deleteFiles",
	"readFileLegalHolds", "writeFileLegalHolds", "readFileRetentions", "writeFileRetentions",
	"bypassGovernance", "readBucketLogging", "writeBucketLogging",
	// Not in the v4 b2_create_key reference, but accepted by B2 and required
	// to read and change Cloud Replication settings.
	"readBucketReplications", "writeBucketReplications",
}

// IsMasterKey reports whether keyID is the account's master application key,
// whose ID is the account ID.
func IsMasterKey(keyID, accountID string) bool { return keyID != "" && keyID == accountID }

// Revision is a bucket revision. B2 documents it as an integer; it is decoded
// leniently from either a JSON number or a string.
type Revision int64

func (r *Revision) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*r = 0
		return nil
	}
	var n json.Number
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		n = json.Number(s)
	} else if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	v, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid bucket revision %q: %w", string(n), err)
	}
	*r = Revision(v)
	return nil
}

// Ptr returns a pointer to v. Convenience for optional wire fields.
func Ptr[T any](v T) *T { return &v }

// Partner API regions accepted by b2_create_group_member.
var PartnerRegions = []string{"us-east", "us-west", "ca-east", "eu-central"}

// GroupMember is a B2 account in a Partner API Group.
type GroupMember struct {
	AccountID  string `json:"accountId"`
	Email      string `json:"email"`
	GroupID    string `json:"groupId"`
	GroupName  string `json:"groupName"`
	Region     string `json:"region"`
	S3Endpoint string `json:"s3Endpoint"`
}

// CreateGroupMemberResponse holds the new account and the only copy B2 will
// ever return of its application key.
type CreateGroupMemberResponse struct {
	ApplicationKeyID string      `json:"applicationKeyId"`
	ApplicationKey   string      `json:"applicationKey"`
	GroupMember      GroupMember `json:"groupMember"`
}

type createGroupMemberRequest struct {
	AdminAccountID string `json:"adminAccountId"`
	GroupID        string `json:"groupId"`
	MemberEmail    string `json:"memberEmail"`
	Region         string `json:"region,omitempty"`
}

type listGroupMembersRequest struct {
	AdminAccountID string `json:"adminAccountId"`
	GroupID        string `json:"groupId"`
	StartEmail     string `json:"startEmail,omitempty"`
	MaxMemberCount int    `json:"maxMemberCount,omitempty"`
}

type listGroupMembersResponse struct {
	GroupMembers []GroupMember `json:"groupMembers"`
	NextEmail    *string       `json:"nextEmail"`
}

type ejectGroupMemberRequest struct {
	AdminAccountID  string `json:"adminAccountId"`
	GroupID         string `json:"groupId"`
	MemberAccountID string `json:"memberAccountId"`
}
