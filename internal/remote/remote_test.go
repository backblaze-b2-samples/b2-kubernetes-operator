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

package remote

import (
	"errors"
	"strings"
	"testing"
)

const base = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://remote.example.com:6443
    CLUSTER_EXTRA
users:
- name: u
  user:
    USER_EXTRA
contexts:
- name: ctx
  context: {cluster: c, user: u}
current-context: ctx
`

func kubeconfig(cluster, user string) []byte {
	return []byte(strings.NewReplacer("CLUSTER_EXTRA", cluster, "USER_EXTRA", user).Replace(base))
}

func TestRestConfigAcceptsInlineCredentials(t *testing.T) {
	cfg, err := restConfig(kubeconfig("insecure-skip-tls-verify: false", "token: abc123"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BearerToken != "abc123" || cfg.Host != "https://remote.example.com:6443" || cfg.Timeout == 0 {
		t.Errorf("unexpected config: host=%s token=%q timeout=%v", cfg.Host, cfg.BearerToken, cfg.Timeout)
	}
}

func TestRestConfigRejectsCommandsAndLocalFiles(t *testing.T) {
	cases := map[string][]byte{
		"exec plugin":   kubeconfig("insecure-skip-tls-verify: false", "exec: {apiVersion: client.authentication.k8s.io/v1, command: /bin/sh, args: [-c, id]}"),
		"auth provider": kubeconfig("insecure-skip-tls-verify: false", "auth-provider: {name: oidc}"),
		// e.g. the operator's own service account token
		"token file":       kubeconfig("insecure-skip-tls-verify: false", "tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token"),
		"client cert file": kubeconfig("insecure-skip-tls-verify: false", "client-certificate: /etc/x.crt"),
		"CA file":          kubeconfig("certificate-authority: /etc/ca.crt", "token: abc"),
	}
	for name, raw := range cases {
		if _, err := restConfig(raw); !errors.Is(err, ErrInvalidKubeconfig) {
			t.Errorf("%s: err = %v, want ErrInvalidKubeconfig", name, err)
		}
	}
}
