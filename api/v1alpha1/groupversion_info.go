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

// Package v1alpha1 contains the b2.backblaze.com/v1alpha1 API.
// +kubebuilder:object:generate=true
// +groupName=b2.backblaze.com
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "b2.backblaze.com", Version: "v1alpha1"}

	// SchemeBuilder registers the types in this group-version.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&ClusterProviderConfig{}, &ClusterProviderConfigList{},
		&B2AccessPolicy{}, &B2AccessPolicyList{},
		&Bucket{}, &BucketList{},
		&ApplicationKey{}, &ApplicationKeyList{},
	)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
