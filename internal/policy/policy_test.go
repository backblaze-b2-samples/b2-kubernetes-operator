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

package policy

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	b2v1 "github.com/backblaze-b2-samples/b2-operator/api/v1alpha1"
)

func TestGlob(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"exact", "exact", true},
		{"exact", "exactly", false},
		{"*", "", true},
		{"*", "anything", true},
		{"acme-*", "acme-logs", true},
		{"acme-*", "acme-", true},
		{"acme-*", "evil-acme-logs", false},
		{"*-logs", "team-logs", true},
		{"*-logs", "team-logs-2", false},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXcYYb", false},
		{"a*a", "a", false},
		{"a*a", "aa", true},
	}
	for _, c := range cases {
		if got := Glob(c.pattern, c.name); got != c.want {
			t.Errorf("Glob(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

func TestMatchesAnySubstitutesNamespace(t *testing.T) {
	if !MatchesAny([]string{"acme-{namespace}-*"}, "team-a", "acme-team-a-data") {
		t.Error("expected match")
	}
	if MatchesAny([]string{"acme-{namespace}-*"}, "team-a", "acme-team-b-data") {
		t.Error("namespace substitution must not match another namespace's buckets")
	}
}

func policy(name string, sel map[string]string, mutate func(*b2v1.B2AccessPolicySpec)) b2v1.B2AccessPolicy {
	p := b2v1.B2AccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: b2v1.B2AccessPolicySpec{
			NamespaceSelector: metav1.LabelSelector{MatchLabels: sel},
			ProviderConfigs:   []string{"default"},
			Buckets:           b2v1.BucketPolicy{NamePatterns: []string{"acme-{namespace}-*"}},
			Keys: b2v1.KeyPolicy{AllowedCapabilities: []b2v1.Capability{
				"listBuckets", "listFiles", "readFiles", "writeFiles", "deleteFiles",
			}},
		},
	}
	if mutate != nil {
		mutate(&p.Spec)
	}
	return p
}

func TestEvaluateBucket(t *testing.T) {
	teamA := labels.Set{"team": "a"}
	base := policy("tenants", map[string]string{"team": "a"}, nil)
	permissive := policy("platform", map[string]string{"team": "a"}, func(s *b2v1.B2AccessPolicySpec) {
		s.Buckets.AllowPublic = true
		s.Buckets.AllowDeletion = true
	})

	cases := []struct {
		name       string
		policies   []b2v1.B2AccessPolicy
		labels     labels.Set
		req        BucketRequest
		allow      bool
		reasonHint string
	}{
		{
			name: "no policies", labels: teamA,
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x"},
			reasonHint: "no B2AccessPolicy allows namespace",
		},
		{
			name: "selector mismatch", policies: []b2v1.B2AccessPolicy{base}, labels: labels.Set{"team": "b"},
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x"},
			reasonHint: "no B2AccessPolicy allows namespace",
		},
		{
			name: "provider config not listed", policies: []b2v1.B2AccessPolicy{base}, labels: teamA,
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "prod", BucketName: "acme-ns1-x"},
			reasonHint: `provider config "prod"`,
		},
		{
			name: "allowed private bucket", policies: []b2v1.B2AccessPolicy{base}, labels: teamA,
			req:   BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x"},
			allow: true,
		},
		{
			name: "name outside namespace pattern", policies: []b2v1.B2AccessPolicy{base}, labels: teamA,
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns2-x"},
			reasonHint: "matches none of",
		},
		{
			name: "public denied", policies: []b2v1.B2AccessPolicy{base}, labels: teamA,
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Public: true},
			reasonHint: "public buckets are not allowed",
		},
		{
			name: "adoption denied by default", policies: []b2v1.B2AccessPolicy{base}, labels: teamA,
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Adopt: true},
			reasonHint: "adopting",
		},
		{
			name: "second policy allows public", policies: []b2v1.B2AccessPolicy{base, permissive}, labels: teamA,
			req:   BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Public: true, Delete: true},
			allow: true,
		},
		{
			name: "permissions do not combine across policies",
			policies: []b2v1.B2AccessPolicy{
				policy("public-only", map[string]string{"team": "a"}, func(s *b2v1.B2AccessPolicySpec) { s.Buckets.AllowPublic = true }),
				policy("delete-only", map[string]string{"team": "a"}, func(s *b2v1.B2AccessPolicySpec) { s.Buckets.AllowDeletion = true }),
			},
			labels:     teamA,
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Public: true, Delete: true},
			reasonHint: "policy delete-only",
		},
		{
			name: "wildcard provider config", labels: teamA,
			policies: []b2v1.B2AccessPolicy{policy("any", nil, func(s *b2v1.B2AccessPolicySpec) { s.ProviderConfigs = []string{"*"} })},
			req:      BucketRequest{Namespace: "ns1", ProviderConfig: "prod", BucketName: "acme-ns1-x"},
			allow:    true,
		},
		{
			name: "compliance retention denied", policies: []b2v1.B2AccessPolicy{base}, labels: teamA,
			req:        BucketRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", ComplianceRetention: true},
			reasonHint: "compliance",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := EvaluateBucket(c.policies, c.labels, c.req)
			if d.Allowed != c.allow {
				t.Fatalf("Allowed = %v (%s), want %v", d.Allowed, d.Reason, c.allow)
			}
			if !c.allow && !strings.Contains(d.Reason, c.reasonHint) {
				t.Errorf("Reason %q does not contain %q", d.Reason, c.reasonHint)
			}
		})
	}
}

func TestEvaluateKey(t *testing.T) {
	teamA := labels.Set{"team": "a"}
	base := policy("tenants", map[string]string{"team": "a"}, nil)
	strict := policy("strict", map[string]string{"team": "a"}, func(s *b2v1.B2AccessPolicySpec) {
		s.Keys.MaxValidity = &metav1.Duration{Duration: 24 * time.Hour}
	})
	external := policy("external", map[string]string{"team": "a"}, func(s *b2v1.B2AccessPolicySpec) {
		s.Keys.AllowExternalBuckets = true
	})

	cases := []struct {
		name       string
		policies   []b2v1.B2AccessPolicy
		req        KeyRequest
		allow      bool
		reasonHint string
	}{
		{
			name: "scoped key allowed", policies: []b2v1.B2AccessPolicy{base},
			req:   KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Capabilities: []b2v1.Capability{"readFiles"}},
			allow: true,
		},
		{
			name: "capability not allowed", policies: []b2v1.B2AccessPolicy{base},
			req:        KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Capabilities: []b2v1.Capability{"readFiles", "writeBuckets"}},
			reasonHint: `capability "writeBuckets"`,
		},
		{
			name: "account-wide denied", policies: []b2v1.B2AccessPolicy{base},
			req:        KeyRequest{Namespace: "ns1", ProviderConfig: "default", Capabilities: []b2v1.Capability{"listBuckets"}},
			reasonHint: "restricted to a bucket",
		},
		{
			name: "external bucket denied", policies: []b2v1.B2AccessPolicy{base},
			req:        KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", External: true, Capabilities: []b2v1.Capability{"readFiles"}},
			reasonHint: "not managed in this namespace",
		},
		{
			name: "external bucket allowed within patterns", policies: []b2v1.B2AccessPolicy{external},
			req:   KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-shared", External: true, Capabilities: []b2v1.Capability{"readFiles"}},
			allow: true,
		},
		{
			name: "external bucket outside patterns", policies: []b2v1.B2AccessPolicy{external},
			req:        KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "prod-billing", External: true, Capabilities: []b2v1.Capability{"readFiles"}},
			reasonHint: "matches none of",
		},
		{
			name: "expiry required", policies: []b2v1.B2AccessPolicy{strict},
			req:        KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Capabilities: []b2v1.Capability{"readFiles"}},
			reasonHint: "must set validFor",
		},
		{
			name: "expiry too long", policies: []b2v1.B2AccessPolicy{strict},
			req:        KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Capabilities: []b2v1.Capability{"readFiles"}, ValidFor: 48 * time.Hour},
			reasonHint: "exceeds the maximum",
		},
		{
			name: "expiry within limit", policies: []b2v1.B2AccessPolicy{strict},
			req:   KeyRequest{Namespace: "ns1", ProviderConfig: "default", BucketName: "acme-ns1-x", Capabilities: []b2v1.Capability{"readFiles"}, ValidFor: time.Hour},
			allow: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := EvaluateKey(c.policies, teamA, c.req)
			if d.Allowed != c.allow {
				t.Fatalf("Allowed = %v (%s), want %v", d.Allowed, d.Reason, c.allow)
			}
			if !c.allow && !strings.Contains(d.Reason, c.reasonHint) {
				t.Errorf("Reason %q does not contain %q", d.Reason, c.reasonHint)
			}
		})
	}
}

func TestEvaluatorBlocksKeyManagementCapabilities(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = b2v1.AddToScheme(scheme)
	p := policy("admins", nil, func(s *b2v1.B2AccessPolicySpec) {
		s.Keys.AllowedCapabilities = append(s.Keys.AllowedCapabilities, "writeKeys")
		s.Keys.AllowAccountWide = true
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}}, &p,
	).Build()
	req := KeyRequest{Namespace: "ns1", ProviderConfig: "default", Capabilities: []b2v1.Capability{"writeKeys"}}

	d, err := (&Evaluator{Reader: c}).CheckKey(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || !strings.Contains(d.Reason, "disabled by the operator") {
		t.Errorf("expected operator-level denial, got %+v", d)
	}

	d, err = (&Evaluator{Reader: c, AllowKeyManagement: true}).CheckKey(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Allowed {
		t.Errorf("expected allow when the operator permits key management and the policy allows it, got %+v", d)
	}
}
