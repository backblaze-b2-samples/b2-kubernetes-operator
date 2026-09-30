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

// Package version holds build information set with -ldflags.
package version

// Version is the operator version, e.g. "v0.1.0". Set at build time with
// -ldflags "-X github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/version.Version=v0.1.0".
var Version = "dev"

// Commit is the git commit the binary was built from.
var Commit = "unknown"

// UserAgent is sent to B2 with every request.
func UserAgent() string {
	return "b2-kubernetes-operator/" + Version
}
