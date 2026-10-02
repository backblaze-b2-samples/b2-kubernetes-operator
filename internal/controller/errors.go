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

import (
	"errors"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

// stageError is a reconcile failure together with the Ready condition it
// should produce and how to retry.
type stageError struct {
	reason  string
	message string
	// requeueAfter schedules a retry instead of returning an error; used
	// for conditions that need a person or another resource to change.
	requeueAfter time.Duration
	// err is returned to controller-runtime for exponential backoff.
	err error
}

func (e *stageError) Error() string { return e.reason + ": " + e.message }

// waitFor is a stageError that retries after a fixed delay.
func waitFor(reason string, after time.Duration, format string, args ...any) *stageError {
	return &stageError{reason: reason, message: fmt.Sprintf(format, args...), requeueAfter: after}
}

// providerError classifies a B2 API error: throttling is retried after the
// server's Retry-After, other transient errors with controller backoff, and
// permanent errors after a long delay.
func providerError(action string, err error) *stageError {
	se := &stageError{reason: b2v1.ReasonProviderError, message: fmt.Sprintf("%s: %v", action, err)}
	var credErr *b2.CredentialsError
	switch {
	case errors.As(err, &credErr):
		se.reason = b2v1.ReasonInvalidCredentials
		se.requeueAfter = 5 * time.Minute
	case b2.RetryAfter(err) > 0:
		se.requeueAfter = b2.RetryAfter(err)
	case b2.IsRetryable(err):
		se.err = err
	default:
		se.requeueAfter = 10 * time.Minute
	}
	return se
}

// result records se on the Ready condition through setNotReady and returns
// the matching reconcile result.
func result(se *stageError, setNotReady func(reason, message string)) (ctrl.Result, error) {
	setNotReady(se.reason, se.message)
	if se.err != nil {
		return ctrl.Result{}, se.err
	}
	return ctrl.Result{RequeueAfter: se.requeueAfter}, nil
}
