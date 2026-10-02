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

// Package b2 is a small, dependency-free client for the subset of the
// Backblaze B2 Native API (v4) that the operator needs: account
// authorization, bucket management and application key management.
//
// The client authorizes lazily, re-authorizes transparently when the account
// token expires, retries idempotent calls on throttling and server errors
// with jittered exponential backoff (honouring Retry-After), and never
// retries non-idempotent creates.
package b2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// DefaultBaseURL is the public B2 API endpoint used for b2_authorize_account.
const DefaultBaseURL = "https://api.backblazeb2.com"

const (
	apiPrefix        = "/b2api/v4/"
	groupsAPIPrefix  = "/b2api/v3/"
	maxResponseBytes = 8 << 20
	defaultTimeout   = 60 * time.Second
)

// Options configures a Client.
type Options struct {
	// BaseURL is the authorization endpoint. Defaults to DefaultBaseURL.
	BaseURL string
	// ApplicationKeyID and ApplicationKey are the credentials to authorize with.
	ApplicationKeyID string
	ApplicationKey   string
	// HTTPClient is used for all requests. Defaults to a client with a 60s timeout.
	HTTPClient *http.Client
	// UserAgent is sent with every request.
	UserAgent string
	// MaxRetries bounds retries of idempotent calls. Zero means the default
	// of 3; a negative value disables retries.
	MaxRetries int
	// BaseBackoff is the first retry delay before jitter. Defaults to 500ms.
	BaseBackoff time.Duration
	// MaxBackoff caps a single retry delay. Defaults to 15s.
	MaxBackoff time.Duration
}

// Client is safe for concurrent use.
type Client struct {
	opts Options

	mu   sync.Mutex
	auth *Authorization
}

// New returns a Client. It does not contact B2 until the first call.
func New(opts Options) *Client {
	if opts.BaseURL == "" {
		opts.BaseURL = DefaultBaseURL
	}
	opts.BaseURL = strings.TrimRight(opts.BaseURL, "/")
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: defaultTimeout}
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "b2-operator"
	}
	switch {
	case opts.MaxRetries == 0:
		opts.MaxRetries = 3
	case opts.MaxRetries < 0:
		opts.MaxRetries = 0
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = 500 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 15 * time.Second
	}
	return &Client{opts: opts}
}

// Authorize performs b2_authorize_account now, replacing any cached token,
// and returns the result. Use it to validate credentials.
func (c *Client) Authorize(ctx context.Context) (*Authorization, error) {
	auth, err := c.authorize(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.auth = auth
	c.mu.Unlock()
	cp := *auth
	return &cp, nil
}

// ListBuckets calls b2_list_buckets.
func (c *Client) ListBuckets(ctx context.Context, req ListBucketsRequest) ([]Bucket, error) {
	var resp listBucketsResponse
	err := c.call(ctx, "b2_list_buckets", true, func(a *Authorization) any {
		req.AccountID = a.AccountID
		return req
	}, &resp)
	return resp.Buckets, err
}

// GetBucketByName returns the bucket with the given name, or nil if the
// account has no such bucket.
func (c *Client) GetBucketByName(ctx context.Context, name string) (*Bucket, error) {
	return c.getBucket(ctx, ListBucketsRequest{BucketName: name})
}

// GetBucketByID returns the bucket with the given ID, or nil if the account
// has no such bucket.
func (c *Client) GetBucketByID(ctx context.Context, id string) (*Bucket, error) {
	return c.getBucket(ctx, ListBucketsRequest{BucketID: id})
}

func (c *Client) getBucket(ctx context.Context, req ListBucketsRequest) (*Bucket, error) {
	buckets, err := c.ListBuckets(ctx, req)
	if err != nil {
		if HasCode(err, CodeBadBucketID) {
			return nil, nil
		}
		return nil, err
	}
	for i := range buckets {
		b := &buckets[i]
		if (req.BucketID != "" && b.BucketID == req.BucketID) || (req.BucketName != "" && b.BucketName == req.BucketName) {
			return b, nil
		}
	}
	return nil, nil
}

// CreateBucket calls b2_create_bucket. It is never retried: a retry after a
// lost response would fail with duplicate_bucket_name, which callers must
// handle by looking the bucket up.
func (c *Client) CreateBucket(ctx context.Context, req CreateBucketRequest) (*Bucket, error) {
	var resp Bucket
	err := c.call(ctx, "b2_create_bucket", false, func(a *Authorization) any {
		req.AccountID = a.AccountID
		return req
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// UpdateBucket calls b2_update_bucket. Set IfRevisionIs to make it safe to
// retry and to detect concurrent modification (code "conflict").
func (c *Client) UpdateBucket(ctx context.Context, req UpdateBucketRequest) (*Bucket, error) {
	var resp Bucket
	err := c.call(ctx, "b2_update_bucket", req.IfRevisionIs != 0, func(a *Authorization) any {
		req.AccountID = a.AccountID
		return req
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteBucket calls b2_delete_bucket. A bucket that no longer exists is
// reported as success.
func (c *Client) DeleteBucket(ctx context.Context, bucketID string) error {
	err := c.call(ctx, "b2_delete_bucket", true, func(a *Authorization) any {
		return deleteBucketRequest{AccountID: a.AccountID, BucketID: bucketID}
	}, nil)
	if HasCode(err, CodeBadBucketID) {
		return nil
	}
	return err
}

// CreateKey calls b2_create_key. It is never retried, so a lost response can
// leave an orphaned key; callers should record the key name beforehand and
// clean up with FindKeys.
func (c *Client) CreateKey(ctx context.Context, req CreateKeyRequest) (*ApplicationKey, error) {
	var resp ApplicationKey
	err := c.call(ctx, "b2_create_key", false, func(a *Authorization) any {
		req.AccountID = a.AccountID
		return req
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteKey calls b2_delete_key.
func (c *Client) DeleteKey(ctx context.Context, applicationKeyID string) error {
	return c.call(ctx, "b2_delete_key", true, func(*Authorization) any {
		return deleteKeyRequest{ApplicationKeyID: applicationKeyID}
	}, nil)
}

// DeleteKeyIfExists deletes a key, treating one that no longer exists as
// deleted. (B2 currently answers success for a missing key; an older or
// stricter response of 400 is handled the same way after checking.)
func (c *Client) DeleteKeyIfExists(ctx context.Context, applicationKeyID string) error {
	err := c.DeleteKey(ctx, applicationKeyID)
	if apiErr, ok := AsAPIError(err); ok && apiErr.Status == http.StatusBadRequest {
		if exists, xerr := c.KeyExists(ctx, applicationKeyID); xerr == nil && !exists {
			return nil
		}
	}
	return err
}

// ListKeys calls b2_list_keys for one page. It returns the keys and the
// start ID of the next page, or "" when there are no more.
func (c *Client) ListKeys(ctx context.Context, startApplicationKeyID string, maxKeyCount int) ([]ApplicationKey, string, error) {
	var resp listKeysResponse
	err := c.call(ctx, "b2_list_keys", true, func(a *Authorization) any {
		return listKeysRequest{AccountID: a.AccountID, MaxKeyCount: maxKeyCount, StartApplicationKeyID: startApplicationKeyID}
	}, &resp)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if resp.NextApplicationKeyID != nil {
		next = *resp.NextApplicationKeyID
	}
	return resp.Keys, next, nil
}

// FindKeys pages through every key on the account and returns those for
// which match returns true.
func (c *Client) FindKeys(ctx context.Context, match func(ApplicationKey) bool) ([]ApplicationKey, error) {
	var out []ApplicationKey
	start := ""
	for {
		keys, next, err := c.ListKeys(ctx, start, 1000)
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			if match(k) {
				out = append(out, k)
			}
		}
		if next == "" {
			return out, nil
		}
		start = next
	}
}

// KeyExists reports whether an application key with the given ID exists.
func (c *Client) KeyExists(ctx context.Context, applicationKeyID string) (bool, error) {
	keys, _, err := c.ListKeys(ctx, applicationKeyID, 1)
	if err != nil {
		return false, err
	}
	return len(keys) > 0 && keys[0].ApplicationKeyID == applicationKeyID, nil
}

// FindGroupMember returns the member of groupID with the given email, or nil.
func (c *Client) FindGroupMember(ctx context.Context, groupID, email string) (*GroupMember, error) {
	var resp listGroupMembersResponse
	err := c.groupsCall(ctx, "b2_list_group_members", true, func(a *Authorization) any {
		return listGroupMembersRequest{AdminAccountID: a.AccountID, GroupID: groupID, StartEmail: email, MaxMemberCount: 1}
	}, &resp)
	if err != nil {
		return nil, err
	}
	for i := range resp.GroupMembers {
		if strings.EqualFold(resp.GroupMembers[i].Email, email) {
			return &resp.GroupMembers[i], nil
		}
	}
	return nil, nil
}

// CreateGroupMember creates a B2 account in groupID. The returned key is the
// only copy B2 will ever provide; the caller must store it. Never retried.
func (c *Client) CreateGroupMember(ctx context.Context, groupID, email, region string) (*CreateGroupMemberResponse, error) {
	var resp CreateGroupMemberResponse
	err := c.groupsCall(ctx, "b2_create_group_member", false, func(a *Authorization) any {
		return createGroupMemberRequest{AdminAccountID: a.AccountID, GroupID: groupID, MemberEmail: email, Region: region}
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// EjectGroupMember removes an account from groupID. The account and its data
// continue to exist outside the Group.
func (c *Client) EjectGroupMember(ctx context.Context, groupID, memberAccountID string) error {
	return c.groupsCall(ctx, "b2_eject_group_member", true, func(a *Authorization) any {
		return ejectGroupMemberRequest{AdminAccountID: a.AccountID, GroupID: groupID, MemberAccountID: memberAccountID}
	}, nil)
}

func (c *Client) groupsCall(ctx context.Context, op string, idempotent bool, body func(*Authorization) any, out any) error {
	return c.callAt(ctx, op, idempotent, func(a *Authorization) string {
		base := c.opts.BaseURL
		if g := a.APIInfo.GroupsAPI; g != nil && g.GroupsAPIURL != "" {
			base = g.GroupsAPIURL
		}
		return strings.TrimRight(base, "/") + groupsAPIPrefix + op
	}, body, out)
}

// call performs an authorized storage API call. body builds the request from
// the current authorization so it can be rebuilt after re-authorizing.
func (c *Client) call(ctx context.Context, op string, idempotent bool, body func(*Authorization) any, out any) error {
	return c.callAt(ctx, op, idempotent, func(a *Authorization) string {
		return strings.TrimRight(a.APIInfo.StorageAPI.APIURL, "/") + apiPrefix + op
	}, body, out)
}

func (c *Client) callAt(ctx context.Context, op string, idempotent bool, endpoint func(*Authorization) string, body func(*Authorization) any, out any) error {
	reauthorized := false
	for attempt := 0; ; attempt++ {
		auth, err := c.currentAuth(ctx)
		if err != nil {
			return err
		}
		err = c.do(ctx, op, endpoint(auth), auth, body(auth), out)
		if err == nil {
			return nil
		}
		if HasCode(err, CodeExpiredAuthToken, CodeBadAuthToken) && !reauthorized {
			reauthorized = true
			c.invalidate(auth)
			attempt-- // re-authorizing is not a retry of the call itself
			continue
		}
		if !idempotent || !IsRetryable(err) || attempt >= c.opts.MaxRetries || ctx.Err() != nil {
			return err
		}
		if err := c.sleep(ctx, c.backoff(attempt, RetryAfter(err))); err != nil {
			return err
		}
	}
}

func (c *Client) do(ctx context.Context, op, endpoint string, auth *Authorization, reqBody any, out any) error {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("b2 %s: encoding request: %w", op, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("b2 %s: %w", op, err)
	}
	req.Header.Set("Authorization", auth.AuthorizationToken)
	req.Header.Set("Content-Type", "application/json")
	return c.send(op, req, out)
}

func (c *Client) authorize(ctx context.Context) (*Authorization, error) {
	const op = "b2_authorize_account"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.opts.BaseURL+apiPrefix+op, nil)
	if err != nil {
		return nil, fmt.Errorf("b2 %s: %w", op, err)
	}
	req.SetBasicAuth(c.opts.ApplicationKeyID, c.opts.ApplicationKey)

	var auth Authorization
	for attempt := 0; ; attempt++ {
		err = c.send(op, req.Clone(ctx), &auth)
		if err == nil {
			break
		}
		if apiErr, ok := AsAPIError(err); ok && (apiErr.Status == http.StatusUnauthorized || apiErr.Code == CodeBadRequest) {
			return nil, &CredentialsError{Err: apiErr}
		}
		if !IsRetryable(err) || attempt >= c.opts.MaxRetries || ctx.Err() != nil {
			return nil, err
		}
		if err := c.sleep(ctx, c.backoff(attempt, RetryAfter(err))); err != nil {
			return nil, err
		}
	}
	if auth.AuthorizationToken == "" || auth.APIInfo.StorageAPI.APIURL == "" {
		return nil, fmt.Errorf("b2 %s: response missing authorizationToken or apiUrl", op)
	}
	if _, err := url.Parse(auth.APIInfo.StorageAPI.APIURL); err != nil {
		return nil, fmt.Errorf("b2 %s: invalid apiUrl: %w", op, err)
	}
	return &auth, nil
}

func (c *Client) send(op string, req *http.Request, out any) error {
	req.Header.Set("User-Agent", c.opts.UserAgent)
	start := time.Now()
	resp, err := c.opts.HTTPClient.Do(req)
	if err != nil {
		observe(op, "error", start)
		return fmt.Errorf("b2 %s: %w", op, err)
	}
	defer func() { _ = resp.Body.Close() }()
	observe(op, fmt.Sprint(resp.StatusCode), start)

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("b2 %s: reading response: %w", op, err)
	}
	if resp.StatusCode != http.StatusOK {
		apiErr := &APIError{}
		if json.Unmarshal(data, apiErr) != nil || apiErr.Code == "" {
			apiErr.Code = strings.ToLower(strings.ReplaceAll(http.StatusText(resp.StatusCode), " ", "_"))
			apiErr.Message = strings.TrimSpace(string(data))
		}
		// The body's status and our op name are authoritative over the JSON.
		apiErr.Status = resp.StatusCode
		apiErr.Operation = op
		apiErr.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		return apiErr
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("b2 %s: decoding response: %w", op, err)
	}
	return nil
}

func (c *Client) currentAuth(ctx context.Context) (*Authorization, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.auth != nil {
		return c.auth, nil
	}
	auth, err := c.authorize(ctx)
	if err != nil {
		return nil, err
	}
	c.auth = auth
	return auth, nil
}

// invalidate drops the cached token if it is still the one that failed, so
// concurrent callers re-authorize at most once per expiry.
func (c *Client) invalidate(stale *Authorization) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.auth == stale {
		c.auth = nil
	}
}

func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	d := c.opts.BaseBackoff << attempt
	if d <= 0 || d > c.opts.MaxBackoff {
		d = c.opts.MaxBackoff
	}
	// Full jitter, with at least half the nominal delay.
	d = d/2 + rand.N(d/2+1) //nolint:gosec // jitter does not need a cryptographic source
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

func (c *Client) sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}
