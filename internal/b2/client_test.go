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

package b2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/backblaze-b2-samples/b2-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2/b2fake"
)

func newClient(t *testing.T, srv *b2fake.Server) *b2.Client {
	t.Helper()
	return b2.New(b2.Options{
		BaseURL:          srv.URL(),
		ApplicationKeyID: srv.MasterKeyID,
		ApplicationKey:   srv.MasterKey,
		BaseBackoff:      time.Millisecond,
		MaxBackoff:       5 * time.Millisecond,
	})
}

func startFake(t *testing.T) *b2fake.Server {
	t.Helper()
	srv := b2fake.New()
	t.Cleanup(srv.Close)
	return srv
}

func TestAuthorize(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)

	auth, err := c.Authorize(context.Background())
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if auth.AccountID != srv.AccountID {
		t.Errorf("AccountID = %q, want %q", auth.AccountID, srv.AccountID)
	}
	if auth.APIInfo.StorageAPI.S3APIURL != srv.S3APIURL {
		t.Errorf("S3APIURL = %q, want %q", auth.APIInfo.StorageAPI.S3APIURL, srv.S3APIURL)
	}
}

func TestAuthorizeBadCredentialsIsNotRetried(t *testing.T) {
	srv := startFake(t)
	c := b2.New(b2.Options{BaseURL: srv.URL(), ApplicationKeyID: "nope", ApplicationKey: "nope", BaseBackoff: time.Millisecond})

	_, err := c.Authorize(context.Background())
	var credErr *b2.CredentialsError
	if !errors.As(err, &credErr) {
		t.Fatalf("err = %v, want CredentialsError", err)
	}
	if b2.IsRetryable(err) {
		t.Error("credentials error must not be retryable")
	}
	if n := srv.Calls("b2_authorize_account"); n != 1 {
		t.Errorf("authorize calls = %d, want 1", n)
	}
}

func TestReauthorizesOnExpiredToken(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	ctx := context.Background()

	if _, err := c.ListBuckets(ctx, b2.ListBucketsRequest{}); err != nil {
		t.Fatal(err)
	}
	srv.ExpireTokens()
	if _, err := c.ListBuckets(ctx, b2.ListBucketsRequest{}); err != nil {
		t.Fatalf("ListBuckets after expiry: %v", err)
	}
	if n := srv.Calls("b2_authorize_account"); n != 2 {
		t.Errorf("authorize calls = %d, want 2", n)
	}
}

func TestRetriesIdempotentCallsOnServiceUnavailable(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	srv.InjectFault("b2_list_buckets", b2fake.Fault{Status: 503, Code: "service_unavailable", Message: "busy"})
	srv.InjectFault("b2_list_buckets", b2fake.Fault{Status: 429, Code: "too_many_requests", Message: "slow down"})

	if _, err := c.ListBuckets(context.Background(), b2.ListBucketsRequest{}); err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if n := srv.Calls("b2_list_buckets"); n != 3 {
		t.Errorf("list calls = %d, want 3", n)
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	for range 4 {
		srv.InjectFault("b2_list_buckets", b2fake.Fault{Status: 503, Code: "service_unavailable", Message: "busy"})
	}
	_, err := c.ListBuckets(context.Background(), b2.ListBucketsRequest{})
	if !b2.HasCode(err, b2.CodeServiceUnavailable) {
		t.Fatalf("err = %v, want service_unavailable", err)
	}
	if n := srv.Calls("b2_list_buckets"); n != 4 {
		t.Errorf("list calls = %d, want 4 (1 + 3 retries)", n)
	}
}

func TestDoesNotRetryCreates(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	srv.InjectFault("b2_create_key", b2fake.Fault{Status: 503, Code: "service_unavailable", Message: "busy"})

	_, err := c.CreateKey(context.Background(), b2.CreateKeyRequest{KeyName: "k1", Capabilities: []string{"listBuckets"}})
	if !b2.IsRetryable(err) {
		t.Fatalf("err = %v, want a retryable error surfaced to the caller", err)
	}
	if n := srv.Calls("b2_create_key"); n != 1 {
		t.Errorf("create_key calls = %d, want 1", n)
	}
}

func TestRetryAfterIsParsed(t *testing.T) {
	srv := startFake(t)
	c := b2.New(b2.Options{BaseURL: srv.URL(), ApplicationKeyID: srv.MasterKeyID, ApplicationKey: srv.MasterKey, MaxRetries: -1})
	srv.InjectFault("b2_list_buckets", b2fake.Fault{Status: 429, Code: "too_many_requests", Message: "slow", RetryAfter: 7})

	_, err := c.ListBuckets(context.Background(), b2.ListBucketsRequest{})
	if got := b2.RetryAfter(err); got != 7*time.Second {
		t.Errorf("RetryAfter = %v, want 7s", got)
	}
}

func TestBucketLifecycle(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	ctx := context.Background()

	created, err := c.CreateBucket(ctx, b2.CreateBucketRequest{
		BucketName: "team-a-logs", BucketType: b2.BucketTypeAllPrivate,
		BucketInfo:     map[string]string{"Owner": "team-a"},
		LifecycleRules: []b2.LifecycleRule{{FileNamePrefix: "tmp/", DaysFromHidingToDeleting: b2.Ptr[int32](1)}},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if created.BucketInfo["owner"] != "team-a" {
		t.Errorf("bucketInfo keys should be lowercased, got %v", created.BucketInfo)
	}

	got, err := c.GetBucketByName(ctx, "team-a-logs")
	if err != nil || got == nil || got.BucketID != created.BucketID {
		t.Fatalf("GetBucketByName = %v, %v", got, err)
	}
	missing, err := c.GetBucketByName(ctx, "does-not-exist")
	if err != nil || missing != nil {
		t.Fatalf("GetBucketByName(missing) = %v, %v; want nil, nil", missing, err)
	}

	_, err = c.UpdateBucket(ctx, b2.UpdateBucketRequest{BucketID: created.BucketID, BucketType: b2.BucketTypeAllPublic, IfRevisionIs: created.Revision + 5})
	if !b2.HasCode(err, b2.CodeConflict) {
		t.Fatalf("stale revision update err = %v, want conflict", err)
	}
	updated, err := c.UpdateBucket(ctx, b2.UpdateBucketRequest{BucketID: created.BucketID, BucketType: b2.BucketTypeAllPublic, IfRevisionIs: created.Revision})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if updated.BucketType != b2.BucketTypeAllPublic || updated.Revision <= created.Revision {
		t.Errorf("update not applied: %+v", updated)
	}

	srv.SetBucketHasFiles("team-a-logs", true)
	if err := c.DeleteBucket(ctx, created.BucketID); !b2.HasCode(err, b2.CodeCannotDeleteNonEmptyBucket) {
		t.Fatalf("delete non-empty err = %v", err)
	}
	srv.SetBucketHasFiles("team-a-logs", false)
	if err := c.DeleteBucket(ctx, created.BucketID); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if err := c.DeleteBucket(ctx, created.BucketID); err != nil {
		t.Fatalf("DeleteBucket of a missing bucket should succeed, got %v", err)
	}
}

func TestDuplicateBucketName(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	srv.ReserveBucketName("taken-elsewhere")

	_, err := c.CreateBucket(context.Background(), b2.CreateBucketRequest{BucketName: "taken-elsewhere", BucketType: b2.BucketTypeAllPrivate})
	if !b2.HasCode(err, b2.CodeDuplicateBucketName) {
		t.Fatalf("err = %v, want duplicate_bucket_name", err)
	}
	if b2.IsRetryable(err) {
		t.Error("duplicate_bucket_name must not be retryable")
	}
}

func TestKeys(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	ctx := context.Background()

	bkt, err := c.CreateBucket(ctx, b2.CreateBucketRequest{BucketName: "key-test-bucket", BucketType: b2.BucketTypeAllPrivate})
	if err != nil {
		t.Fatal(err)
	}
	k, err := c.CreateKey(ctx, b2.CreateKeyRequest{
		KeyName: "scoped", Capabilities: []string{"listFiles", "readFiles"},
		BucketIDs: []string{bkt.BucketID}, NamePrefix: "reports/", ValidDurationInSeconds: 3600,
	})
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	if k.ApplicationKey == "" || k.ExpirationTimestamp == nil || k.NamePrefix == nil || *k.NamePrefix != "reports/" {
		t.Errorf("unexpected key: %+v", k)
	}

	// The new key can itself authorize, and sees its restrictions.
	kc := b2.New(b2.Options{BaseURL: srv.URL(), ApplicationKeyID: k.ApplicationKeyID, ApplicationKey: k.ApplicationKey})
	auth, err := kc.Authorize(ctx)
	if err != nil {
		t.Fatalf("authorize with new key: %v", err)
	}
	if len(auth.APIInfo.StorageAPI.Allowed.Buckets) != 1 || auth.APIInfo.StorageAPI.Allowed.Buckets[0].ID != bkt.BucketID {
		t.Errorf("allowed buckets = %+v", auth.APIInfo.StorageAPI.Allowed.Buckets)
	}

	exists, err := c.KeyExists(ctx, k.ApplicationKeyID)
	if err != nil || !exists {
		t.Fatalf("KeyExists = %v, %v", exists, err)
	}
	if err := c.DeleteKey(ctx, k.ApplicationKeyID); err != nil {
		t.Fatalf("DeleteKey: %v", err)
	}
	exists, err = c.KeyExists(ctx, k.ApplicationKeyID)
	if err != nil || exists {
		t.Fatalf("KeyExists after delete = %v, %v", exists, err)
	}
}

func TestFindKeysPaginates(t *testing.T) {
	srv := startFake(t)
	c := newClient(t, srv)
	for i := range 1203 {
		srv.AddKey(fmt.Sprintf("bulk-%d", i), []string{"listBuckets"}, nil, "")
	}
	srv.AddKey("needle-1", []string{"listBuckets"}, nil, "")

	found, err := c.FindKeys(context.Background(), func(k b2.ApplicationKey) bool { return k.KeyName == "needle-1" })
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Fatalf("found %d keys, want 1", len(found))
	}
	if n := srv.Calls("b2_list_keys"); n != 2 {
		t.Errorf("list_keys calls = %d, want 2 pages", n)
	}
}

func TestNonJSONErrorBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>bad gateway</html>"))
	}))
	defer ts.Close()
	c := b2.New(b2.Options{BaseURL: ts.URL, MaxRetries: -1})

	_, err := c.Authorize(context.Background())
	apiErr, ok := b2.AsAPIError(err)
	if !ok || apiErr.Status != http.StatusBadGateway || !apiErr.Retryable() {
		t.Fatalf("err = %v, want retryable 502 APIError", err)
	}
}

func TestRevisionDecodesStringOrNumber(t *testing.T) {
	for _, in := range []string{`{"revision": 12}`, `{"revision": "12"}`} {
		var b b2.Bucket
		if err := json.Unmarshal([]byte(in), &b); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if b.Revision != 12 {
			t.Errorf("%s: revision = %d", in, b.Revision)
		}
	}
}

func TestContextCancellationStopsRetries(t *testing.T) {
	srv := startFake(t)
	c := b2.New(b2.Options{BaseURL: srv.URL(), ApplicationKeyID: srv.MasterKeyID, ApplicationKey: srv.MasterKey, BaseBackoff: time.Hour, MaxBackoff: time.Hour})
	if _, err := c.Authorize(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv.InjectFault("b2_list_buckets", b2fake.Fault{Status: 503, Code: "service_unavailable"})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.ListBuckets(ctx, b2.ListBucketsRequest{})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("err = %v after %v; want prompt cancellation", err, time.Since(start))
	}
}
