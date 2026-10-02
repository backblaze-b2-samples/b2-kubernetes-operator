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

// Event reasons. Failures reported through the Ready condition reuse its
// reason (see status.go); these mark actions the operator took.
const (
	EventCreated        = "Created"
	EventAdopted        = "Adopted"
	EventUpdated        = "Updated"
	EventDriftCorrected = "DriftCorrected"
	EventBucketMissing  = "BucketMissing"
	EventDeleted        = "Deleted"
	EventRetained       = "Retained"

	EventKeyCreated          = "KeyCreated"
	EventKeyRotated          = "KeyRotated"
	EventKeyRevoked          = "KeyRevoked"
	EventKeyRecovered        = "KeyRecovered"
	EventRevocationScheduled = "RevocationScheduled"
	EventRevocationCancelled = "RevocationCancelled"

	EventReplicationUpdated = "ReplicationUpdated"

	EventOperationsKeyCreated = "OperationsKeyCreated"
	EventOperationsKeyMissing = "OperationsKeyMissing"
	EventEjected              = "Ejected"

	EventMasterKeyInUse = "MasterKeyInUse"
)
