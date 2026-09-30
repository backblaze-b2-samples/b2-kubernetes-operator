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
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Error codes returned by the B2 Native API that the operator acts on.
const (
	CodeBadRequest                 = "bad_request"
	CodeBadBucketID                = "bad_bucket_id"
	CodeDuplicateBucketName        = "duplicate_bucket_name"
	CodeTooManyBuckets             = "too_many_buckets"
	CodeCannotDeleteNonEmptyBucket = "cannot_delete_non_empty_bucket"
	CodeUnauthorized               = "unauthorized"
	CodeBadAuthToken               = "bad_auth_token"
	CodeExpiredAuthToken           = "expired_auth_token"
	CodeUnsupported                = "unsupported"
	CodeConflict                   = "conflict"
	CodeFileLockConflict           = "file_lock_conflict"
	CodeTransactionCapExceeded     = "transaction_cap_exceeded"
	CodeTooManyRequests            = "too_many_requests"
	CodeServiceUnavailable         = "service_unavailable"
	CodeEmailNotVerified           = "email_not_verified"
	CodeNoPaymentHistory           = "no_payment_history"
	CodeRestrictedBucketConflict   = "restricted_bucket_conflict"
	CodeSourceReplicationConflict  = "source_replication_conflict"
)

// APIError is an error response from the B2 Native API.
type APIError struct {
	Operation  string        `json:"-"`
	Status     int           `json:"status"`
	Code       string        `json:"code"`
	Message    string        `json:"message"`
	RetryAfter time.Duration `json:"-"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("b2 %s: %d %s: %s", e.Operation, e.Status, e.Code, e.Message)
}

// Retryable reports whether the request may succeed if retried later without
// any change: throttling, timeouts and server-side failures.
func (e *APIError) Retryable() bool {
	switch {
	case e.Status == http.StatusTooManyRequests,
		e.Status == http.StatusRequestTimeout,
		e.Status >= 500:
		return true
	case e.Code == CodeTooManyRequests, e.Code == CodeServiceUnavailable:
		return true
	}
	return false
}

// AsAPIError unwraps err into an *APIError.
func AsAPIError(err error) (*APIError, bool) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr, true
	}
	return nil, false
}

// HasCode reports whether err is an *APIError with one of the given codes.
func HasCode(err error, codes ...string) bool {
	apiErr, ok := AsAPIError(err)
	if !ok {
		return false
	}
	for _, c := range codes {
		if apiErr.Code == c {
			return true
		}
	}
	return false
}

// IsRetryable reports whether err is transient. Errors that are not B2 API
// errors (network failures, timeouts) are treated as retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := AsAPIError(err); ok {
		return apiErr.Retryable()
	}
	var credErr *CredentialsError
	return !errors.As(err, &credErr)
}

// RetryAfter returns the server-requested delay for err, or zero.
func RetryAfter(err error) time.Duration {
	if apiErr, ok := AsAPIError(err); ok {
		return apiErr.RetryAfter
	}
	return 0
}

// CredentialsError means the configured application key was rejected by
// b2_authorize_account. It is never retryable without a configuration change.
type CredentialsError struct {
	Err *APIError
}

func (e *CredentialsError) Error() string {
	return fmt.Sprintf("b2 credentials rejected: %v", e.Err)
}

func (e *CredentialsError) Unwrap() error { return e.Err }

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

func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
