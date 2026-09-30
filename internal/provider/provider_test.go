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

package provider

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	b2v1 "github.com/backblaze-b2-samples/b2-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2"
	"github.com/backblaze-b2-samples/b2-operator/internal/b2/b2fake"
)

func TestS3Region(t *testing.T) {
	for in, want := range map[string]string{
		"https://s3.us-west-004.backblazeb2.com":     "us-west-004",
		"https://s3.eu-central-003.backblazeb2.com/": "eu-central-003",
		"":            "",
		"::not a url": "",
	} {
		if got := S3Region(in); got != want {
			t.Errorf("S3Region(%q) = %q, want %q", in, got, want)
		}
	}
}

func setup(t *testing.T, objs ...client.Object) (*Registry, *b2fake.Server) {
	t.Helper()
	srv := b2fake.New()
	t.Cleanup(srv.Close)
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = b2v1.AddToScheme(scheme)
	objs = append(objs, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "ops", ResourceVersion: "1"},
		Data:       map[string][]byte{"applicationKeyId": []byte(srv.MasterKeyID), "applicationKey": []byte(srv.MasterKey)},
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &Registry{APIReader: c, AllowInsecureAPIURL: true}, srv
}

func pc(apiURL, secret string) *b2v1.ClusterProviderConfig {
	return &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "u1", Generation: 1},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               apiURL,
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: "ops", Name: secret},
		},
	}
}

func TestGetAuthorizesAndCaches(t *testing.T) {
	reg, srv := setup(t)
	ctx := context.Background()
	p := pc(srv.URL(), "creds")

	acct, err := reg.Get(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if acct.AccountID != srv.AccountID || acct.S3Region != "us-west-004" {
		t.Errorf("unexpected account %+v", acct)
	}
	again, err := reg.Get(ctx, p)
	if err != nil || again != acct {
		t.Fatalf("second Get should hit the cache: %v", err)
	}
	if n := srv.Calls("b2_authorize_account"); n != 1 {
		t.Errorf("authorize calls = %d, want 1", n)
	}

	// A new generation rebuilds.
	p.Generation = 2
	if _, err := reg.Get(ctx, p); err != nil {
		t.Fatal(err)
	}
	if n := srv.Calls("b2_authorize_account"); n != 2 {
		t.Errorf("authorize calls after generation change = %d, want 2", n)
	}
}

func TestMissingSecret(t *testing.T) {
	reg, srv := setup(t)
	_, err := reg.Get(context.Background(), pc(srv.URL(), "nope"))
	if !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("err = %v, want ErrSecretNotFound", err)
	}
}

func TestIncompleteSecret(t *testing.T) {
	reg, srv := setup(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: "ops"},
		Data:       map[string][]byte{"applicationKeyId": []byte("x")},
	})
	_, err := reg.Get(context.Background(), pc(srv.URL(), "partial"))
	if !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("err = %v, want ErrSecretNotFound", err)
	}
}

func TestBadCredentials(t *testing.T) {
	reg, srv := setup(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "wrong", Namespace: "ops"},
		Data:       map[string][]byte{"applicationKeyId": []byte("x"), "applicationKey": []byte("y")},
	})
	_, err := reg.Get(context.Background(), pc(srv.URL(), "wrong"))
	var credErr *b2.CredentialsError
	if !errors.As(err, &credErr) {
		t.Fatalf("err = %v, want CredentialsError", err)
	}
}

func TestInsecureAPIURLRejectedByDefault(t *testing.T) {
	reg, srv := setup(t)
	reg.AllowInsecureAPIURL = false
	_, err := reg.Get(context.Background(), pc(srv.URL(), "creds"))
	if !errors.Is(err, ErrInvalidAPIURL) {
		t.Fatalf("err = %v, want ErrInvalidAPIURL for an http:// URL", err)
	}
}
