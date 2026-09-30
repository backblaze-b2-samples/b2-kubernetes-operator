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

// Package provider turns ClusterProviderConfigs into authorized B2 clients
// and caches them. Credentials are read with an uncached reader, so the
// operator never needs to watch or cache arbitrary Secrets.
package provider

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

// ErrSecretNotFound is returned when the credentials Secret or one of its
// keys is missing.
var ErrSecretNotFound = errors.New("credentials secret not found")

// ErrInvalidAPIURL is returned for an unusable spec.apiURL.
var ErrInvalidAPIURL = errors.New("invalid apiURL")

// Account is an authorized connection to one B2 account.
type Account struct {
	Client *b2.Client
	// APIURL is the authorization endpoint, for verifying other keys.
	APIURL       string
	AccountID    string
	S3Endpoint   string
	S3Region     string
	Capabilities []string
	// PartnerAPI reports whether the account may use the Partner (Groups) API.
	PartnerAPI bool
	// KeyExpirationMillis is the operator key's expiry, if any.
	KeyExpirationMillis *int64
}

// Registry caches one Account per ClusterProviderConfig.
type Registry struct {
	// APIReader reads Secrets directly from the API server.
	APIReader client.Reader
	// UserAgent is sent to B2.
	UserAgent string
	// AllowInsecureAPIURL permits http:// API URLs (for testing only).
	AllowInsecureAPIURL bool

	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	uid        types.UID
	generation int64
	secretRV   string
	account    *Account
}

// Get returns the Account for pc, building and authorizing it on first use.
// It does not re-read the credentials Secret if a client for the same
// generation of pc is cached; Refresh does that.
func (r *Registry) Get(ctx context.Context, pc *b2v1.ClusterProviderConfig) (*Account, error) {
	r.mu.Lock()
	e, ok := r.entries[pc.Name]
	r.mu.Unlock()
	if ok && e.uid == pc.UID && e.generation == pc.Generation {
		return e.account, nil
	}
	return r.Refresh(ctx, pc)
}

// Refresh re-reads pc's credentials, rebuilds the client if they changed, and
// authorizes it, returning up-to-date account details.
func (r *Registry) Refresh(ctx context.Context, pc *b2v1.ClusterProviderConfig) (*Account, error) {
	if err := r.validateAPIURL(pc.Spec.APIURL); err != nil {
		return nil, err
	}
	keyID, key, rv, err := r.readCredentials(ctx, pc)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	e, ok := r.entries[pc.Name]
	r.mu.Unlock()
	var c *b2.Client
	if ok && e.uid == pc.UID && e.generation == pc.Generation && e.secretRV == rv {
		c = e.account.Client
	} else {
		c = b2.New(b2.Options{
			BaseURL:          pc.Spec.APIURL,
			ApplicationKeyID: keyID,
			ApplicationKey:   key,
			UserAgent:        r.UserAgent,
		})
	}

	auth, err := c.Authorize(ctx)
	if err != nil {
		return nil, err
	}
	acct := &Account{
		Client:              c,
		APIURL:              pc.Spec.APIURL,
		AccountID:           auth.AccountID,
		S3Endpoint:          auth.APIInfo.StorageAPI.S3APIURL,
		S3Region:            S3Region(auth.APIInfo.StorageAPI.S3APIURL),
		Capabilities:        auth.APIInfo.StorageAPI.Allowed.Capabilities,
		KeyExpirationMillis: auth.ApplicationKeyExpirationTimestamp,
		PartnerAPI:          auth.APIInfo.GroupsAPI != nil,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = map[string]*entry{}
	}
	r.entries[pc.Name] = &entry{uid: pc.UID, generation: pc.Generation, secretRV: rv, account: acct}
	return acct, nil
}

// Forget drops the cached client for a deleted provider config.
func (r *Registry) Forget(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, name)
}

func (r *Registry) validateAPIURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%w %q", ErrInvalidAPIURL, raw)
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !r.AllowInsecureAPIURL) {
		return fmt.Errorf("%w %q: must use https (http is only allowed when the operator runs with --allow-insecure-api-url)", ErrInvalidAPIURL, raw)
	}
	return nil
}

func (r *Registry) readCredentials(ctx context.Context, pc *b2v1.ClusterProviderConfig) (keyID, key, rv string, err error) {
	ref := pc.Spec.CredentialsSecretRef
	var secret corev1.Secret
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &secret); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return "", "", "", fmt.Errorf("%w: %s/%s", ErrSecretNotFound, ref.Namespace, ref.Name)
		}
		return "", "", "", fmt.Errorf("reading credentials secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	idKey := defaultString(ref.ApplicationKeyIDKey, "applicationKeyId")
	keyKey := defaultString(ref.ApplicationKeyKey, "applicationKey")
	id := strings.TrimSpace(string(secret.Data[idKey]))
	k := strings.TrimSpace(string(secret.Data[keyKey]))
	if id == "" || k == "" {
		return "", "", "", fmt.Errorf("%w: %s/%s must contain non-empty %q and %q", ErrSecretNotFound, ref.Namespace, ref.Name, idKey, keyKey)
	}
	return id, k, secret.ResourceVersion, nil
}

// S3Region extracts the region from an S3 endpoint such as
// https://s3.us-west-004.backblazeb2.com.
func S3Region(s3URL string) string {
	u, err := url.Parse(s3URL)
	if err != nil {
		return ""
	}
	host := strings.TrimPrefix(u.Hostname(), "s3.")
	region, _, _ := strings.Cut(host, ".")
	return region
}

func defaultString(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
