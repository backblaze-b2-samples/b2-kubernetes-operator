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

// Package policy evaluates B2AccessPolicy objects. It is the tenancy
// boundary of the operator: controllers call it before every B2 mutation, so
// it is enforced even if admission webhooks are absent or bypassed.
//
// Semantics: a request is allowed if at least one policy that selects the
// namespace and lists the provider config allows every part of it.
// Permissions are never combined across policies, so each policy can be read
// on its own to understand what it grants.
package policy

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

const maxReasonLen = 1024

// Decision is the result of an evaluation.
type Decision struct {
	Allowed bool
	// Reason explains a denial.
	Reason string
}

// BucketRequest describes a Bucket operation to authorize.
type BucketRequest struct {
	Namespace           string
	ProviderConfig      string
	BucketName          string
	Public              bool
	Adopt               bool
	Delete              bool
	ComplianceRetention bool
	Unencrypted         bool
	Replication         bool
}

// KeyRequest describes an ApplicationKey to authorize.
type KeyRequest struct {
	Namespace      string
	ProviderConfig string
	// BucketName is the bucket the key is scoped to; empty for account-wide.
	BucketName string
	// External is true when the bucket is not a Bucket resource in the
	// same namespace.
	External     bool
	Capabilities []b2v1.Capability
	// ValidFor is the requested key lifetime; zero means no expiry.
	ValidFor time.Duration
	// DeliverTo is the remote cluster and namespace the Secret goes to, if
	// not the local namespace.
	DeliverTo *b2v1.DeliveryTarget
}

// Evaluator loads policies and namespace labels from the cluster.
type Evaluator struct {
	Reader client.Reader
	// AllowKeyManagement permits listKeys/writeKeys/deleteKeys when a policy
	// also allows them. When false they are always denied.
	AllowKeyManagement bool
}

// CheckBucket authorizes a Bucket operation.
func (e *Evaluator) CheckBucket(ctx context.Context, req BucketRequest) (Decision, error) {
	policies, nsLabels, err := e.load(ctx, req.Namespace)
	if err != nil {
		return Decision{}, err
	}
	return EvaluateBucket(policies, nsLabels, req), nil
}

// CheckKey authorizes an ApplicationKey.
func (e *Evaluator) CheckKey(ctx context.Context, req KeyRequest) (Decision, error) {
	if !e.AllowKeyManagement {
		for _, c := range req.Capabilities {
			if slices.Contains(b2v1.KeyManagementCapabilities, c) {
				return Decision{Reason: fmt.Sprintf("capability %q is disabled by the operator (--allow-key-management-capabilities=false)", c)}, nil
			}
		}
	}
	policies, nsLabels, err := e.load(ctx, req.Namespace)
	if err != nil {
		return Decision{}, err
	}
	return EvaluateKey(policies, nsLabels, req), nil
}

func (e *Evaluator) load(ctx context.Context, namespace string) ([]b2v1.B2AccessPolicy, labels.Set, error) {
	var list b2v1.B2AccessPolicyList
	if err := e.Reader.List(ctx, &list); err != nil {
		return nil, nil, fmt.Errorf("listing B2AccessPolicies: %w", err)
	}
	var ns corev1.Namespace
	if err := e.Reader.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
		return nil, nil, fmt.Errorf("getting namespace %q: %w", namespace, err)
	}
	return list.Items, labels.Set(ns.Labels), nil
}

// EvaluateBucket decides a BucketRequest against the given policies.
func EvaluateBucket(policies []b2v1.B2AccessPolicy, nsLabels labels.Set, req BucketRequest) Decision {
	applicable, d := applicablePolicies(policies, nsLabels, req.Namespace, req.ProviderConfig)
	if applicable == nil {
		return d
	}
	var reasons []string
	for _, p := range applicable {
		bp := p.Spec.Buckets
		var why []string
		if !MatchesAny(bp.NamePatterns, req.Namespace, req.BucketName) {
			why = append(why, fmt.Sprintf("bucket name %q matches none of %v", req.BucketName, bp.NamePatterns))
		}
		if req.Public && !bp.AllowPublic {
			why = append(why, "public buckets are not allowed")
		}
		if req.Adopt && !bp.AllowAdoption {
			why = append(why, "adopting existing buckets is not allowed")
		}
		if req.Delete && !bp.AllowDeletion {
			why = append(why, "deletionPolicy Delete is not allowed")
		}
		if req.ComplianceRetention && !bp.AllowComplianceRetention {
			why = append(why, "compliance-mode retention is not allowed")
		}
		if req.Unencrypted && !bp.AllowUnencrypted {
			why = append(why, "unencrypted buckets are not allowed")
		}
		if req.Replication && !bp.AllowReplication {
			why = append(why, "replication is not allowed")
		}
		if len(why) == 0 {
			return Decision{Allowed: true}
		}
		reasons = append(reasons, fmt.Sprintf("policy %s: %s", p.Name, strings.Join(why, ", ")))
	}
	return deny(reasons)
}

// EvaluateKey decides a KeyRequest against the given policies. It does not
// apply the operator-wide key-management block; see Evaluator.CheckKey.
func EvaluateKey(policies []b2v1.B2AccessPolicy, nsLabels labels.Set, req KeyRequest) Decision {
	applicable, d := applicablePolicies(policies, nsLabels, req.Namespace, req.ProviderConfig)
	if applicable == nil {
		return d
	}
	var reasons []string
	for _, p := range applicable {
		kp := p.Spec.Keys
		var why []string
		for _, c := range req.Capabilities {
			if !slices.Contains(kp.AllowedCapabilities, c) {
				why = append(why, fmt.Sprintf("capability %q is not allowed", c))
			}
		}
		switch {
		case req.BucketName == "":
			if !kp.AllowAccountWide {
				why = append(why, "keys must be restricted to a bucket")
			}
		case req.External && !kp.AllowExternalBuckets:
			why = append(why, "keys for buckets not managed in this namespace are not allowed")
		case !MatchesAny(p.Spec.Buckets.NamePatterns, req.Namespace, req.BucketName):
			why = append(why, fmt.Sprintf("bucket name %q matches none of %v", req.BucketName, p.Spec.Buckets.NamePatterns))
		}
		if t := req.DeliverTo; t != nil && !deliveryAllowed(kp.AllowedDeliveryTargets, req.Namespace, t) {
			why = append(why, fmt.Sprintf("delivery to namespace %q of remote cluster %q is not allowed", t.Namespace, t.RemoteCluster))
		}
		if kp.MaxValidity != nil {
			if req.ValidFor == 0 {
				why = append(why, fmt.Sprintf("keys must set validFor (at most %s)", kp.MaxValidity.Duration))
			} else if req.ValidFor > kp.MaxValidity.Duration {
				why = append(why, fmt.Sprintf("validFor %s exceeds the maximum %s", req.ValidFor, kp.MaxValidity.Duration))
			}
		}
		if len(why) == 0 {
			return Decision{Allowed: true}
		}
		reasons = append(reasons, fmt.Sprintf("policy %s: %s", p.Name, strings.Join(why, ", ")))
	}
	return deny(reasons)
}

func deliveryAllowed(patterns []b2v1.DeliveryTargetPattern, namespace string, t *b2v1.DeliveryTarget) bool {
	for _, p := range patterns {
		if Glob(p.RemoteCluster, t.RemoteCluster) && MatchesAny(p.Namespaces, namespace, t.Namespace) {
			return true
		}
	}
	return false
}

func applicablePolicies(policies []b2v1.B2AccessPolicy, nsLabels labels.Set, namespace, providerConfig string) ([]b2v1.B2AccessPolicy, Decision) {
	var out []b2v1.B2AccessPolicy
	for _, p := range policies {
		sel, err := metav1.LabelSelectorAsSelector(&p.Spec.NamespaceSelector)
		if err != nil || !sel.Matches(nsLabels) {
			continue
		}
		if !slices.Contains(p.Spec.ProviderConfigs, "*") && !slices.Contains(p.Spec.ProviderConfigs, providerConfig) {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, Decision{Reason: fmt.Sprintf("no B2AccessPolicy allows namespace %q to use provider config %q", namespace, providerConfig)}
	}
	slices.SortFunc(out, func(a, b b2v1.B2AccessPolicy) int { return strings.Compare(a.Name, b.Name) })
	return out, Decision{}
}

func deny(reasons []string) Decision {
	r := strings.Join(reasons, "; ")
	if len(r) > maxReasonLen {
		r = r[:maxReasonLen-3] + "..."
	}
	return Decision{Reason: r}
}

// MatchesAny reports whether name matches any of the glob patterns after
// substituting {namespace}.
func MatchesAny(patterns []string, namespace, name string) bool {
	for _, p := range patterns {
		if Glob(strings.ReplaceAll(p, "{namespace}", namespace), name) {
			return true
		}
	}
	return false
}

// Glob matches name against pattern, where '*' matches any (possibly empty)
// run of characters and every other character matches itself.
func Glob(pattern, name string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == name
	}
	if !strings.HasPrefix(name, parts[0]) {
		return false
	}
	name = name[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, part := range parts[1 : len(parts)-1] {
		i := strings.Index(name, part)
		if i < 0 {
			return false
		}
		name = name[i+len(part):]
	}
	return strings.HasSuffix(name, last)
}
