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

// Package b2fake is an in-memory implementation of the parts of the B2 Native
// API v4 used by the operator. It enforces the documented validation rules
// that matter to the operator (bucket and key naming, global bucket name
// uniqueness, Object Lock irreversibility, revision checks, non-empty bucket
// deletion) and supports fault injection. It is used by unit, integration and
// kind-based end-to-end tests; it is not a general B2 emulator.
package b2fake

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/backblaze-b2-samples/b2-operator/internal/b2"
)

// AllCapabilities is every capability a master key holds.
var AllCapabilities = []string{
	"listKeys", "writeKeys", "deleteKeys", "listAllBucketNames", "listBuckets", "readBuckets",
	"writeBuckets", "deleteBuckets", "readBucketRetentions", "writeBucketRetentions",
	"readBucketEncryption", "writeBucketEncryption", "readBucketNotifications",
	"writeBucketNotifications", "listFiles", "readFiles", "shareFiles", "writeFiles", "deleteFiles",
	"readFileLegalHolds", "writeFileLegalHolds", "readFileRetentions", "writeFileRetentions",
	"bypassGovernance", "readBucketLogging", "writeBucketLogging",
}

var (
	bucketNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{4,61}[A-Za-z0-9]$`)
	ipv4RE       = regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`)
	keyNameRE    = regexp.MustCompile(`^[A-Za-z0-9-]{1,100}$`)
)

// Fault is an injected error response.
type Fault struct {
	Status     int
	Code       string
	Message    string
	RetryAfter int // seconds; 0 omits the header
	// AfterApply performs the operation before returning the error,
	// simulating a response lost after the server acted.
	AfterApply bool
}

type key struct {
	b2.ApplicationKey
	secret string
}

type bucket struct {
	b2.Bucket
	sse      *b2.ServerSideEncryption
	fileLock b2.FileLockValue
	hasFiles bool
}

// Server is a fake B2 API server.
type Server struct {
	// AccountID is the fake account's ID.
	AccountID string
	// MasterKeyID and MasterKey are the credentials of the account's master key.
	MasterKeyID string
	MasterKey   string
	// S3APIURL is returned as apiInfo.storageApi.s3ApiUrl.
	S3APIURL string

	ts  *httptest.Server
	url string

	mu            sync.Mutex
	now           func() time.Time
	keys          map[string]*key   // by applicationKeyId
	tokens        map[string]string // token -> applicationKeyId
	buckets       map[string]*bucket
	reservedNames map[string]bool // names owned by other accounts
	faults        map[string][]Fault
	calls         map[string]int
	nextID        int
}

// New starts a fake server on a random local port. Call Close when done.
func New() *Server {
	s := newServer()
	s.ts = httptest.NewServer(s)
	s.url = s.ts.URL
	return s
}

// NewUnstarted returns a server that is not listening; serve it with
// http.Serve or use it as an http.Handler. URL must be set with SetURL.
func NewUnstarted() *Server { return newServer() }

func newServer() *Server {
	s := &Server{
		AccountID:     "fakeaccount01",
		S3APIURL:      "https://s3.us-west-004.backblazeb2.com",
		now:           time.Now,
		keys:          map[string]*key{},
		tokens:        map[string]string{},
		buckets:       map[string]*bucket{},
		reservedNames: map[string]bool{},
		faults:        map[string][]Fault{},
		calls:         map[string]int{},
	}
	s.MasterKeyID = s.AccountID
	s.MasterKey = randHex(20)
	s.keys[s.MasterKeyID] = &key{
		ApplicationKey: b2.ApplicationKey{
			AccountID: s.AccountID, ApplicationKeyID: s.MasterKeyID, KeyName: "master",
			Capabilities: AllCapabilities,
		},
		secret: s.MasterKey,
	}
	return s
}

// SetMasterKey replaces the master key; the key ID is also the account ID.
func (s *Server) SetMasterKey(id, secret string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, s.MasterKeyID)
	s.AccountID, s.MasterKeyID, s.MasterKey = id, id, secret
	s.keys[id] = &key{
		ApplicationKey: b2.ApplicationKey{AccountID: id, ApplicationKeyID: id, KeyName: "master", Capabilities: AllCapabilities},
		secret:         secret,
	}
}

// URL is the base URL to authorize against.
func (s *Server) URL() string { return s.url }

// SetURL sets the base URL advertised as apiUrl (for NewUnstarted servers).
func (s *Server) SetURL(u string) { s.url = strings.TrimRight(u, "/") }

// Close stops the server.
func (s *Server) Close() {
	if s.ts != nil {
		s.ts.Close()
	}
}

// Listener is the underlying listener address, for servers started with New.
func (s *Server) Listener() net.Listener { return s.ts.Listener }

// SetNow overrides the server clock (used for key expiry).
func (s *Server) SetNow(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// AddKey creates a key directly, bypassing the API, and returns its ID and secret.
func (s *Server) AddKey(name string, capabilities []string, bucketIDs []string, namePrefix string) (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.newKeyLocked(name, capabilities, bucketIDs, namePrefix, nil)
	return k.ApplicationKeyID, k.secret
}

// ReserveBucketName marks a bucket name as taken by another account.
func (s *Server) ReserveBucketName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservedNames[name] = true
}

// SetBucketHasFiles marks a bucket as non-empty so deletion fails.
func (s *Server) SetBucketHasFiles(bucketName string, hasFiles bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.buckets {
		if b.BucketName == bucketName {
			b.hasFiles = hasFiles
		}
	}
}

// ExpireTokens invalidates every issued auth token, forcing re-authorization.
func (s *Server) ExpireTokens() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = map[string]string{}
}

// InjectFault queues a fault for the next call to op (e.g. "b2_create_key").
func (s *Server) InjectFault(op string, f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[op] = append(s.faults[op], f)
}

// Calls returns how many times op was called.
func (s *Server) Calls(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

// Bucket returns a copy of the named bucket as B2 would return it, or nil.
func (s *Server) Bucket(name string) *b2.Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.buckets {
		if b.BucketName == name {
			out := s.renderBucket(b)
			return &out
		}
	}
	return nil
}

// Key returns a copy of the key with the given ID (without its secret), or nil.
func (s *Server) Key(id string) *b2.ApplicationKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[id]
	if !ok {
		return nil
	}
	out := k.ApplicationKey
	return &out
}

// Keys returns copies of all non-master keys.
func (s *Server) Keys() []b2.ApplicationKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []b2.ApplicationKey
	for id, k := range s.keys {
		if id != s.MasterKeyID {
			out = append(out, k.ApplicationKey)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ApplicationKeyID < out[j].ApplicationKeyID })
	return out
}

// DeleteKeyDirect removes a key without going through the API.
func (s *Server) DeleteKeyDirect(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, id)
}

// MutateBucket applies fn to the named bucket out of band (simulating a
// console or CLI change) and bumps its revision.
func (s *Server) MutateBucket(name string, fn func(*b2.Bucket)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.buckets {
		if b.BucketName == name {
			fn(&b.Bucket)
			b.Revision++
		}
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op, ok := strings.CutPrefix(r.URL.Path, "/b2api/v4/")
	if !ok {
		writeErr(w, Fault{Status: 404, Code: "not_found", Message: "unknown path " + r.URL.Path})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[op]++
	var afterApply *Fault
	if q := s.faults[op]; len(q) > 0 {
		s.faults[op] = q[1:]
		if !q[0].AfterApply {
			writeErr(w, q[0])
			return
		}
		afterApply = &q[0]
	}
	if op == "b2_authorize_account" {
		s.authorize(w, r)
		return
	}
	caller, fault := s.authenticate(r)
	if fault != nil {
		writeErr(w, *fault)
		return
	}
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, *badRequest("invalid JSON body: %v", err))
		return
	}
	if acct, ok := body["accountId"]; ok && op != "b2_delete_key" {
		var a string
		_ = json.Unmarshal(acct, &a)
		if a != s.AccountID {
			writeErr(w, Fault{Status: 401, Code: "unauthorized", Message: "accountId does not match"})
			return
		}
	}
	handlers := map[string]struct {
		capability string
		fn         func(*key, map[string]json.RawMessage) (any, *Fault)
	}{
		"b2_list_buckets":  {"listBuckets", s.listBuckets},
		"b2_create_bucket": {"writeBuckets", s.createBucket},
		"b2_update_bucket": {"writeBuckets", s.updateBucket},
		"b2_delete_bucket": {"deleteBuckets", s.deleteBucket},
		"b2_create_key":    {"writeKeys", s.createKey},
		"b2_delete_key":    {"deleteKeys", s.deleteKey},
		"b2_list_keys":     {"listKeys", s.listKeys},
	}
	h, ok := handlers[op]
	if !ok {
		writeErr(w, Fault{Status: 404, Code: "not_found", Message: "unsupported operation " + op})
		return
	}
	if !slices.Contains(caller.Capabilities, h.capability) {
		writeErr(w, Fault{Status: 401, Code: "unauthorized", Message: "key lacks capability " + h.capability})
		return
	}
	out, fault := h.fn(caller, body)
	if fault != nil {
		writeErr(w, *fault)
		return
	}
	if afterApply != nil {
		writeErr(w, *afterApply)
		return
	}
	writeJSON(w, out)
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	k, exists := s.keys[id]
	if !ok || !exists || k.secret != secret || s.expired(k) {
		writeErr(w, Fault{Status: 401, Code: "unauthorized", Message: "invalid application key"})
		return
	}
	token := "4_" + randHex(24)
	s.tokens[token] = id
	allowed := b2.Allowed{Capabilities: k.Capabilities, NamePrefix: k.NamePrefix}
	for _, bid := range k.BucketIDs {
		ab := b2.AllowedBucket{ID: bid}
		if b, ok := s.buckets[bid]; ok {
			ab.Name = b2.Ptr(b.BucketName)
		}
		allowed.Buckets = append(allowed.Buckets, ab)
	}
	writeJSON(w, b2.Authorization{
		AccountID:          s.AccountID,
		AuthorizationToken: token,
		APIInfo: b2.APIInfo{StorageAPI: b2.StorageAPI{
			APIURL: s.url, DownloadURL: s.url, S3APIURL: s.S3APIURL, Allowed: allowed,
		}},
		ApplicationKeyExpirationTimestamp: k.ExpirationTimestamp,
	})
}

func (s *Server) authenticate(r *http.Request) (*key, *Fault) {
	token := r.Header.Get("Authorization")
	id, ok := s.tokens[token]
	if !ok {
		return nil, &Fault{Status: 401, Code: "expired_auth_token", Message: "authorization token expired or invalid"}
	}
	k, ok := s.keys[id]
	if !ok || s.expired(k) {
		return nil, &Fault{Status: 401, Code: "bad_auth_token", Message: "application key no longer valid"}
	}
	return k, nil
}

func (s *Server) expired(k *key) bool {
	return k.ExpirationTimestamp != nil && s.now().UnixMilli() >= *k.ExpirationTimestamp
}

func (s *Server) listBuckets(_ *key, body map[string]json.RawMessage) (any, *Fault) {
	var req b2.ListBucketsRequest
	if f := decode(body, &req); f != nil {
		return nil, f
	}
	var out []b2.Bucket
	for _, b := range s.buckets {
		if req.BucketID != "" && b.BucketID != req.BucketID {
			continue
		}
		if req.BucketName != "" && b.BucketName != req.BucketName {
			continue
		}
		out = append(out, s.renderBucket(b))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BucketName < out[j].BucketName })
	return map[string]any{"buckets": emptyIfNil(out)}, nil
}

func (s *Server) createBucket(_ *key, body map[string]json.RawMessage) (any, *Fault) {
	var req b2.CreateBucketRequest
	if f := decode(body, &req); f != nil {
		return nil, f
	}
	if f := validateBucketName(req.BucketName); f != nil {
		return nil, f
	}
	if s.reservedNames[req.BucketName] {
		return nil, &Fault{Status: 400, Code: b2.CodeDuplicateBucketName, Message: "Bucket name is already in use."}
	}
	for _, b := range s.buckets {
		if b.BucketName == req.BucketName {
			return nil, &Fault{Status: 400, Code: b2.CodeDuplicateBucketName, Message: "Bucket name is already in use."}
		}
	}
	if len(s.buckets) >= 100 {
		return nil, &Fault{Status: 400, Code: b2.CodeTooManyBuckets, Message: "too many buckets"}
	}
	if f := validateBucketType(req.BucketType); f != nil {
		return nil, f
	}
	info, f := normalizeInfo(req.BucketInfo)
	if f != nil {
		return nil, f
	}
	if f := validateLifecycle(req.LifecycleRules); f != nil {
		return nil, f
	}
	if f := validateCORS(req.CORSRules); f != nil {
		return nil, f
	}
	b := &bucket{Bucket: b2.Bucket{
		AccountID: s.AccountID, BucketID: s.newID("bkt"), BucketName: req.BucketName,
		BucketType: req.BucketType, BucketInfo: info, CORSRules: req.CORSRules,
		LifecycleRules: req.LifecycleRules, Revision: 1, Options: []string{"s3"},
	}}
	b.fileLock.IsFileLockEnabled = req.FileLockEnabled
	if req.DefaultServerSideEncryption != nil && req.DefaultServerSideEncryption.Mode != nil {
		b.sse = req.DefaultServerSideEncryption
	}
	s.buckets[b.BucketID] = b
	return s.renderBucket(b), nil
}

func (s *Server) updateBucket(_ *key, body map[string]json.RawMessage) (any, *Fault) {
	var req b2.UpdateBucketRequest
	if f := decode(body, &req); f != nil {
		return nil, f
	}
	b, ok := s.buckets[req.BucketID]
	if !ok {
		return nil, &Fault{Status: 400, Code: b2.CodeBadBucketID, Message: "Invalid bucketId: " + req.BucketID}
	}
	if req.IfRevisionIs != 0 && int64(req.IfRevisionIs) != int64(b.Revision) {
		return nil, &Fault{Status: 409, Code: b2.CodeConflict, Message: "ifRevisionIs does not match"}
	}
	next := *b
	if req.BucketType != "" {
		if f := validateBucketType(req.BucketType); f != nil {
			return nil, f
		}
		next.BucketType = req.BucketType
	}
	if req.BucketInfo != nil {
		info, f := normalizeInfo(*req.BucketInfo)
		if f != nil {
			return nil, f
		}
		next.BucketInfo = info
	}
	if req.LifecycleRules != nil {
		if f := validateLifecycle(*req.LifecycleRules); f != nil {
			return nil, f
		}
		next.LifecycleRules = *req.LifecycleRules
	}
	if req.CORSRules != nil {
		if f := validateCORS(*req.CORSRules); f != nil {
			return nil, f
		}
		next.CORSRules = *req.CORSRules
	}
	if req.FileLockEnabled != nil {
		if !*req.FileLockEnabled && b.fileLock.IsFileLockEnabled {
			return nil, &Fault{Status: 400, Code: b2.CodeFileLockConflict, Message: "Object Lock cannot be disabled once enabled"}
		}
		next.fileLock.IsFileLockEnabled = *req.FileLockEnabled
	}
	if req.DefaultRetention != nil {
		if !next.fileLock.IsFileLockEnabled {
			return nil, badRequest("defaultRetention requires Object Lock to be enabled on the bucket")
		}
		if req.DefaultRetention.Mode == nil {
			next.fileLock.DefaultRetention = nil
		} else {
			m := *req.DefaultRetention.Mode
			p := req.DefaultRetention.Period
			if (m != b2.RetentionModeGovernance && m != b2.RetentionModeCompliance) || p == nil || p.Duration < 1 ||
				(p.Unit != b2.PeriodUnitDays && p.Unit != b2.PeriodUnitYears) {
				return nil, badRequest("invalid defaultRetention")
			}
			next.fileLock.DefaultRetention = req.DefaultRetention
		}
	}
	if req.DefaultServerSideEncryption != nil {
		switch mode := req.DefaultServerSideEncryption.Mode; {
		case mode == nil:
			next.sse = nil
		case *mode != b2.SSEModeB2:
			return nil, badRequest("unsupported default encryption mode")
		default:
			next.sse = req.DefaultServerSideEncryption
		}
	}
	next.Revision++
	*b = next
	return s.renderBucket(b), nil
}

func (s *Server) deleteBucket(_ *key, body map[string]json.RawMessage) (any, *Fault) {
	var req struct {
		BucketID string `json:"bucketId"`
	}
	if f := decode(body, &req); f != nil {
		return nil, f
	}
	b, ok := s.buckets[req.BucketID]
	if !ok {
		return nil, &Fault{Status: 400, Code: b2.CodeBadBucketID, Message: "Invalid bucketId: " + req.BucketID}
	}
	if b.hasFiles {
		return nil, &Fault{Status: 400, Code: b2.CodeCannotDeleteNonEmptyBucket, Message: "Cannot delete non-empty bucket"}
	}
	delete(s.buckets, req.BucketID)
	return s.renderBucket(b), nil
}

func (s *Server) createKey(caller *key, body map[string]json.RawMessage) (any, *Fault) {
	var req b2.CreateKeyRequest
	if f := decode(body, &req); f != nil {
		return nil, f
	}
	if !keyNameRE.MatchString(req.KeyName) {
		return nil, badRequest("keyName must be 1-100 letters, digits or hyphens")
	}
	if len(req.Capabilities) == 0 {
		return nil, badRequest("capabilities is required")
	}
	for _, c := range req.Capabilities {
		if !slices.Contains(AllCapabilities, c) {
			return nil, badRequest("unknown capability %q", c)
		}
		if !slices.Contains(caller.Capabilities, c) {
			return nil, &Fault{Status: 401, Code: "unauthorized", Message: "cannot grant capability not held: " + c}
		}
	}
	if req.ValidDurationInSeconds < 0 || req.ValidDurationInSeconds > 86_400_000 {
		return nil, badRequest("validDurationInSeconds out of range")
	}
	for _, id := range req.BucketIDs {
		if _, ok := s.buckets[id]; !ok {
			return nil, &Fault{Status: 400, Code: b2.CodeBadBucketID, Message: "Invalid bucketId: " + id}
		}
	}
	if req.NamePrefix != "" && len(req.BucketIDs) == 0 {
		return nil, badRequest("namePrefix requires bucketIds")
	}
	var exp *int64
	if req.ValidDurationInSeconds > 0 {
		exp = b2.Ptr(s.now().Add(time.Duration(req.ValidDurationInSeconds) * time.Second).UnixMilli())
	}
	k := s.newKeyLocked(req.KeyName, req.Capabilities, req.BucketIDs, req.NamePrefix, exp)
	out := k.ApplicationKey
	out.ApplicationKey = k.secret
	return out, nil
}

func (s *Server) deleteKey(_ *key, body map[string]json.RawMessage) (any, *Fault) {
	var req struct {
		ApplicationKeyID string `json:"applicationKeyId"`
	}
	if f := decode(body, &req); f != nil {
		return nil, f
	}
	k, ok := s.keys[req.ApplicationKeyID]
	if !ok || req.ApplicationKeyID == s.MasterKeyID {
		return nil, badRequest("applicationKeyId is not valid: %s", req.ApplicationKeyID)
	}
	delete(s.keys, req.ApplicationKeyID)
	for t, id := range s.tokens {
		if id == req.ApplicationKeyID {
			delete(s.tokens, t)
		}
	}
	return k.ApplicationKey, nil
}

func (s *Server) listKeys(_ *key, body map[string]json.RawMessage) (any, *Fault) {
	var req struct {
		MaxKeyCount           int    `json:"maxKeyCount"`
		StartApplicationKeyID string `json:"startApplicationKeyId"`
	}
	if f := decode(body, &req); f != nil {
		return nil, f
	}
	if req.MaxKeyCount <= 0 {
		req.MaxKeyCount = 100
	}
	req.MaxKeyCount = min(req.MaxKeyCount, 1000)
	var ids []string
	for id := range s.keys {
		if id != s.MasterKeyID && id >= req.StartApplicationKeyID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	resp := map[string]any{"keys": []b2.ApplicationKey{}, "nextApplicationKeyId": nil}
	var page []b2.ApplicationKey
	for i, id := range ids {
		if i == req.MaxKeyCount {
			resp["nextApplicationKeyId"] = id
			break
		}
		page = append(page, s.keys[id].ApplicationKey)
	}
	resp["keys"] = emptyIfNil(page)
	return resp, nil
}

func (s *Server) newKeyLocked(name string, caps, bucketIDs []string, prefix string, exp *int64) *key {
	k := &key{
		ApplicationKey: b2.ApplicationKey{
			AccountID: s.AccountID, ApplicationKeyID: s.newID("004key"), KeyName: name,
			Capabilities: slices.Clone(caps), BucketIDs: slices.Clone(bucketIDs),
			ExpirationTimestamp: exp, Options: []string{"s3"},
		},
		secret: "K004" + randB64(22),
	}
	if prefix != "" {
		k.NamePrefix = b2.Ptr(prefix)
	}
	s.keys[k.ApplicationKeyID] = k
	return k
}

func (s *Server) newID(prefix string) string {
	s.nextID++
	return fmt.Sprintf("%s%012d%s", prefix, s.nextID, randHex(4))
}

func (s *Server) renderBucket(b *bucket) b2.Bucket {
	out := b.Bucket
	out.BucketInfo = maps(b.BucketInfo)
	out.CORSRules = emptyIfNil(slices.Clone(b.CORSRules))
	out.LifecycleRules = emptyIfNil(slices.Clone(b.LifecycleRules))
	sse := &b2.ServerSideEncryption{}
	if b.sse != nil {
		sse = b.sse
	}
	out.DefaultServerSideEncryption = &b2.ProtectedSSE{IsClientAuthorizedToRead: true, Value: sse}
	fl := b.fileLock
	if fl.DefaultRetention == nil {
		fl.DefaultRetention = &b2.DefaultRetention{}
	}
	out.FileLockConfiguration = &b2.ProtectedFileLock{IsClientAuthorizedToRead: true, Value: &fl}
	return out
}

func validateBucketName(name string) *Fault {
	switch {
	case !bucketNameRE.MatchString(name),
		strings.HasPrefix(strings.ToLower(name), "b2-"),
		strings.Contains(name, ".."),
		ipv4RE.MatchString(name):
		return &Fault{Status: 400, Code: "invalid_bucket_name", Message: "invalid bucket name: " + name}
	}
	return nil
}

func validateBucketType(t string) *Fault {
	if t != b2.BucketTypeAllPrivate && t != b2.BucketTypeAllPublic {
		return badRequest("invalid bucketType %q", t)
	}
	return nil
}

func normalizeInfo(in map[string]string) (map[string]string, *Fault) {
	if len(in) > 10 {
		return nil, badRequest("bucketInfo may have at most 10 entries")
	}
	out := map[string]string{}
	for k, v := range in {
		out[strings.ToLower(k)] = v
	}
	return out, nil
}

func validateLifecycle(rules []b2.LifecycleRule) *Fault {
	if len(rules) > 100 {
		return badRequest("too many lifecycle rules")
	}
	for _, r := range rules {
		if r.DaysFromHidingToDeleting == nil && r.DaysFromUploadingToHiding == nil && r.DaysFromStartingToCancelingUnfinishedLargeFiles == nil {
			return badRequest("lifecycle rule for %q does nothing", r.FileNamePrefix)
		}
		for _, d := range []*int32{r.DaysFromHidingToDeleting, r.DaysFromUploadingToHiding, r.DaysFromStartingToCancelingUnfinishedLargeFiles} {
			if d != nil && *d < 1 {
				return badRequest("lifecycle day counts must be at least 1")
			}
		}
	}
	return nil
}

func validateCORS(rules []b2.CORSRule) *Fault {
	if len(rules) > 100 {
		return badRequest("too many CORS rules")
	}
	for _, r := range rules {
		if len(r.AllowedOrigins) == 0 || len(r.AllowedOperations) == 0 {
			return badRequest("CORS rule %q needs allowedOrigins and allowedOperations", r.CORSRuleName)
		}
		if r.MaxAgeSeconds < 0 || r.MaxAgeSeconds > 86400 {
			return badRequest("maxAgeSeconds out of range")
		}
	}
	return nil
}

func decode(body map[string]json.RawMessage, out any) *Fault {
	raw, _ := json.Marshal(body)
	if err := json.Unmarshal(raw, out); err != nil {
		return badRequest("invalid request: %v", err)
	}
	return nil
}

func badRequest(format string, args ...any) *Fault {
	return &Fault{Status: 400, Code: b2.CodeBadRequest, Message: fmt.Sprintf(format, args...)}
}

func writeErr(w http.ResponseWriter, f Fault) {
	if f.RetryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprint(f.RetryAfter))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.Status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": f.Status, "code": f.Code, "message": f.Message})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func emptyIfNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func maps(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func randB64(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
