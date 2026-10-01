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

// Package remote builds clients for RemoteClusters, the clusters that
// ApplicationKey Secrets can be delivered to.
package remote

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

var (
	// ErrSecretNotFound means the kubeconfig Secret or key is missing.
	ErrSecretNotFound = errors.New("kubeconfig secret not found")
	// ErrInvalidKubeconfig means the kubeconfig cannot or must not be used.
	ErrInvalidKubeconfig = errors.New("invalid kubeconfig")
)

// Registry caches a client per RemoteCluster.
type Registry struct {
	// APIReader reads kubeconfig Secrets directly from the API server.
	APIReader client.Reader

	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	uid        types.UID
	generation int64
	secretRV   string
	client     client.Client
}

var scheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	return s
}()

// Get returns a client for rc, building it on first use.
func (r *Registry) Get(ctx context.Context, rc *b2v1.RemoteCluster) (client.Client, error) {
	r.mu.Lock()
	e, ok := r.entries[rc.Name]
	r.mu.Unlock()
	if ok && e.uid == rc.UID && e.generation == rc.Generation {
		return e.client, nil
	}
	c, _, err := r.Refresh(ctx, rc)
	return c, err
}

// Refresh re-reads rc's kubeconfig, rebuilds the client if it changed, and
// checks the connection, returning the remote server version.
func (r *Registry) Refresh(ctx context.Context, rc *b2v1.RemoteCluster) (client.Client, string, error) {
	ref := rc.Spec.KubeconfigSecretRef
	var s corev1.Secret
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &s); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return nil, "", fmt.Errorf("%w: %s/%s", ErrSecretNotFound, ref.Namespace, ref.Name)
		}
		return nil, "", err
	}
	key := ref.Key
	if key == "" {
		key = "kubeconfig"
	}
	raw := s.Data[key]
	if len(raw) == 0 {
		return nil, "", fmt.Errorf("%w: %s/%s has no %q", ErrSecretNotFound, ref.Namespace, ref.Name, key)
	}

	r.mu.Lock()
	e, ok := r.entries[rc.Name]
	r.mu.Unlock()
	var cfgClient client.Client
	if ok && e.uid == rc.UID && e.generation == rc.Generation && e.secretRV == s.ResourceVersion {
		cfgClient = e.client
	}

	restCfg, err := restConfig(raw)
	if err != nil {
		return nil, "", err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrInvalidKubeconfig, err)
	}
	v, err := dc.ServerVersion()
	if err != nil {
		return nil, "", fmt.Errorf("connecting to the remote cluster: %w", err)
	}
	if cfgClient == nil {
		if cfgClient, err = client.New(restCfg, client.Options{Scheme: scheme}); err != nil {
			return nil, "", fmt.Errorf("%w: %w", ErrInvalidKubeconfig, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = map[string]*entry{}
	}
	r.entries[rc.Name] = &entry{uid: rc.UID, generation: rc.Generation, secretRV: s.ResourceVersion, client: cfgClient}
	return cfgClient, v.GitVersion, nil
}

// Forget drops the client for a deleted RemoteCluster.
func (r *Registry) Forget(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, name)
}

// restConfig parses a kubeconfig, refusing anything that would make the
// operator run commands or read local files: exec and auth-provider plugins,
// and token, certificate or key file paths. A kubeconfig must carry its
// credentials inline. Otherwise whoever writes the kubeconfig Secret could,
// for example, point a token file at the operator's own service account
// token and have it sent to a server they control.
func restConfig(raw []byte) (*rest.Config, error) {
	cfg, err := clientcmd.Load(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKubeconfig, err)
	}
	for name, a := range cfg.AuthInfos {
		switch {
		case a.Exec != nil:
			return nil, fmt.Errorf("%w: user %q uses an exec plugin; use an inline token or client certificate", ErrInvalidKubeconfig, name)
		case a.AuthProvider != nil:
			return nil, fmt.Errorf("%w: user %q uses an auth provider plugin; use an inline token or client certificate", ErrInvalidKubeconfig, name)
		case a.TokenFile != "" || a.ClientCertificate != "" || a.ClientKey != "":
			return nil, fmt.Errorf("%w: user %q references local files; embed the credentials", ErrInvalidKubeconfig, name)
		}
	}
	for name, c := range cfg.Clusters {
		if c.CertificateAuthority != "" {
			return nil, fmt.Errorf("%w: cluster %q references a local CA file; embed certificate-authority-data", ErrInvalidKubeconfig, name)
		}
	}
	restCfg, err := clientcmd.NewDefaultClientConfig(*cfg, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKubeconfig, err)
	}
	restCfg.Timeout = 30 * time.Second
	restCfg.UserAgent = "b2-kubernetes-operator"
	return restCfg, nil
}
