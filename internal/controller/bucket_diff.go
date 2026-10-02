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
	"cmp"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

// diffBucket builds the update that brings observed to the spec. changed
// names the fields being corrected; warnings are differences that cannot be
// corrected (unreadable settings, or Object Lock that cannot be turned off).
func diffBucket(bkt *b2v1.Bucket, observed *b2.Bucket) (upd b2.UpdateBucketRequest, changed, warnings []string) {
	upd = b2.UpdateBucketRequest{BucketID: observed.BucketID, IfRevisionIs: observed.Revision}

	if want := bucketType(bkt); observed.BucketType != want {
		upd.BucketType = want
		changed = append(changed, "bucketType")
	}
	if want := desiredInfo(bkt); !maps.Equal(observed.BucketInfo, want) {
		upd.BucketInfo = &want
		changed = append(changed, "bucketInfo")
	}
	if want := desiredLifecycle(bkt); !reflect.DeepEqual(normalizeLifecycle(observed.LifecycleRules), normalizeLifecycle(want)) {
		upd.LifecycleRules = &want
		changed = append(changed, "lifecycleRules")
	}
	if want := desiredCORS(bkt); !reflect.DeepEqual(normalizeCORS(observed.CORSRules), normalizeCORS(want)) {
		upd.CORSRules = &want
		changed = append(changed, "corsRules")
	}

	if e := bkt.Spec.DefaultEncryption; e != nil {
		sse := observed.DefaultServerSideEncryption
		if sse == nil || !sse.IsClientAuthorizedToRead {
			warnings = append(warnings, "cannot read default encryption (operator key lacks readBucketEncryption)")
		} else if want := desiredEncryption(e); encryptionMode(sse.Value) != encryptionMode(want) {
			upd.DefaultServerSideEncryption = want
			changed = append(changed, "defaultEncryption")
		}
	}

	if ol := bkt.Spec.ObjectLock; ol != nil {
		fl := observed.FileLockConfiguration
		if fl == nil || !fl.IsClientAuthorizedToRead {
			warnings = append(warnings, "cannot read Object Lock settings (operator key lacks readBucketRetentions)")
			return upd, changed, warnings
		}
		enabled := fl.Value != nil && fl.Value.IsFileLockEnabled
		switch {
		case ol.Enabled && !enabled:
			upd.FileLockEnabled = b2.Ptr(true)
			changed = append(changed, "objectLock.enabled")
			enabled = true
		case !ol.Enabled && enabled:
			warnings = append(warnings, "Object Lock is enabled in B2 and cannot be disabled")
		}
		if ol.Enabled && enabled {
			var current *b2.DefaultRetention
			if fl.Value != nil {
				current = fl.Value.DefaultRetention
			}
			if want := desiredRetention(ol); !retentionEqual(current, want) {
				upd.DefaultRetention = want
				changed = append(changed, "objectLock.defaultRetention")
			}
		}
	}
	return upd, changed, warnings
}

// newBucketRequest is the b2_create_bucket request for the spec. Default
// retention cannot be set at creation; the first update applies it.
func newBucketRequest(bkt *b2v1.Bucket) b2.CreateBucketRequest {
	req := b2.CreateBucketRequest{
		BucketName:     bkt.Spec.BucketName,
		BucketType:     bucketType(bkt),
		BucketInfo:     desiredInfo(bkt),
		LifecycleRules: desiredLifecycle(bkt),
		CORSRules:      desiredCORS(bkt),
	}
	if ol := bkt.Spec.ObjectLock; ol != nil && ol.Enabled {
		req.FileLockEnabled = true
	}
	if e := bkt.Spec.DefaultEncryption; e != nil && e.Mode == b2v1.EncryptionModeSSEB2 {
		req.DefaultServerSideEncryption = desiredEncryption(e)
	}
	return req
}

func bucketType(bkt *b2v1.Bucket) string {
	if bkt.Spec.BucketType == b2v1.BucketTypeAllPublic {
		return b2.BucketTypeAllPublic
	}
	return b2.BucketTypeAllPrivate
}

// desiredInfo is the spec's bucket info plus the operator's ownership mark.
func desiredInfo(bkt *b2v1.Bucket) map[string]string {
	info := make(map[string]string, len(bkt.Spec.BucketInfo)+1)
	for k, v := range bkt.Spec.BucketInfo {
		info[strings.ToLower(k)] = v
	}
	info[OwnerInfoKey] = string(bkt.UID)
	return info
}

func desiredLifecycle(bkt *b2v1.Bucket) []b2.LifecycleRule {
	out := make([]b2.LifecycleRule, 0, len(bkt.Spec.LifecycleRules))
	for _, r := range bkt.Spec.LifecycleRules {
		out = append(out, b2.LifecycleRule{
			FileNamePrefix:                                  r.FileNamePrefix,
			DaysFromUploadingToHiding:                       r.DaysFromUploadingToHiding,
			DaysFromHidingToDeleting:                        r.DaysFromHidingToDeleting,
			DaysFromStartingToCancelingUnfinishedLargeFiles: r.DaysFromStartingToCancelingUnfinishedLargeFiles,
		})
	}
	return out
}

func desiredCORS(bkt *b2v1.Bucket) []b2.CORSRule {
	out := make([]b2.CORSRule, 0, len(bkt.Spec.CORSRules))
	for _, r := range bkt.Spec.CORSRules {
		ops := make([]string, 0, len(r.AllowedOperations))
		for _, op := range r.AllowedOperations {
			ops = append(ops, string(op))
		}
		out = append(out, b2.CORSRule{
			CORSRuleName:      r.Name,
			AllowedOrigins:    r.AllowedOrigins,
			AllowedOperations: ops,
			AllowedHeaders:    r.AllowedHeaders,
			ExposeHeaders:     r.ExposeHeaders,
			MaxAgeSeconds:     r.MaxAgeSeconds,
		})
	}
	return out
}

// desiredEncryption is SSE-B2, or {"mode": null} for no default encryption.
func desiredEncryption(e *b2v1.DefaultEncryption) *b2.ServerSideEncryption {
	if e.Mode == b2v1.EncryptionModeSSEB2 {
		return &b2.ServerSideEncryption{Mode: b2.Ptr(b2.SSEModeB2), Algorithm: b2.Ptr(b2.SSEAlgorithmAES)}
	}
	return &b2.ServerSideEncryption{}
}

func encryptionMode(sse *b2.ServerSideEncryption) string {
	if sse == nil || sse.Mode == nil {
		return ""
	}
	return *sse.Mode
}

// desiredRetention is the spec's default retention, or {"mode": null} to
// clear it.
func desiredRetention(ol *b2v1.ObjectLock) *b2.DefaultRetention {
	if ol.DefaultRetention == nil {
		return &b2.DefaultRetention{}
	}
	return &b2.DefaultRetention{
		Mode:   b2.Ptr(string(ol.DefaultRetention.Mode)),
		Period: &b2.RetentionPeriod{Duration: ol.DefaultRetention.Duration, Unit: string(ol.DefaultRetention.Unit)},
	}
}

// retentionEqual compares retention settings, treating a missing default
// and {"mode": null, "period": null} (how B2 reports none) as equal.
func retentionEqual(a, b *b2.DefaultRetention) bool {
	mode := func(r *b2.DefaultRetention) string {
		if r == nil || r.Mode == nil {
			return ""
		}
		return *r.Mode
	}
	if mode(a) != mode(b) {
		return false
	}
	return mode(a) == "" || reflect.DeepEqual(a.Period, b.Period)
}

func fileLockEnabled(b *b2.Bucket) bool {
	fl := b.FileLockConfiguration
	return fl != nil && fl.Value != nil && fl.Value.IsFileLockEnabled
}

// normalizeLifecycle sorts rules so that order does not count as drift.
func normalizeLifecycle(rules []b2.LifecycleRule) []b2.LifecycleRule {
	if len(rules) == 0 {
		return nil
	}
	days := func(p *int32) string {
		if p == nil {
			return "-"
		}
		return fmt.Sprint(*p)
	}
	key := func(r b2.LifecycleRule) string {
		return r.FileNamePrefix + "\x00" + days(r.DaysFromUploadingToHiding) + days(r.DaysFromHidingToDeleting) +
			days(r.DaysFromStartingToCancelingUnfinishedLargeFiles)
	}
	out := slices.Clone(rules)
	slices.SortFunc(out, func(a, b b2.LifecycleRule) int { return cmp.Compare(key(a), key(b)) })
	return out
}

// normalizeCORS sorts rules and their lists so that order does not count as
// drift.
func normalizeCORS(rules []b2.CORSRule) []b2.CORSRule {
	if len(rules) == 0 {
		return nil
	}
	sorted := func(s []string) []string {
		if len(s) == 0 {
			return nil
		}
		return slices.Sorted(slices.Values(s))
	}
	out := make([]b2.CORSRule, 0, len(rules))
	for _, r := range rules {
		r.AllowedOrigins = sorted(r.AllowedOrigins)
		r.AllowedOperations = sorted(r.AllowedOperations)
		r.AllowedHeaders = sorted(r.AllowedHeaders)
		r.ExposeHeaders = sorted(r.ExposeHeaders)
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b b2.CORSRule) int { return cmp.Compare(a.CORSRuleName, b.CORSRuleName) })
	return out
}
