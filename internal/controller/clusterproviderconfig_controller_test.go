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
	"context"
	"testing"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
)

func TestProviderConfigReady(t *testing.T) {
	g := requireEnv(t)
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	eventuallyReason(g, pc, b2v1.ReasonReconciled)
	g.Expect(pc.Status.AccountID).To(Equal(fakeB2.AccountID))
	g.Expect(pc.Status.S3Region).To(Equal("us-west-004"))
	g.Expect(pc.Status.Capabilities).To(ContainElement("writeKeys"))
}

func TestProviderConfigBadCredentials(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	suffix := newNamespace(t, false) // unique per run
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-creds-" + suffix, Namespace: operatorNS},
		StringData: map[string]string{"applicationKeyId": "nope", "applicationKey": "nope"},
	})).To(Succeed())
	bad := &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-" + suffix},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               fakeB2.URL(),
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "bad-creds-" + suffix},
		},
	}
	missing := &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-" + suffix},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               fakeB2.URL(),
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "does-not-exist"},
		},
	}
	g.Expect(k8s.Create(ctx, bad)).To(Succeed())
	g.Expect(k8s.Create(ctx, missing)).To(Succeed())
	eventuallyReason(g, bad, b2v1.ReasonInvalidCredentials)
	eventuallyReason(g, missing, b2v1.ReasonCredentialsSecretNotFound)
}

func TestPartnerConfigRequiresMasterKey(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	name := uniqueCustomer("app-key-partner-")
	id, secret := fakeB2.AddKey("not-master", []string{"listBuckets", "writeKeys"}, nil, "")
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: name},
		StringData: map[string]string{"applicationKeyId": id, "applicationKey": secret},
	})).To(Succeed())
	pc := partnerConfigFor(name, name)
	g.Expect(k8s.Create(ctx, pc)).To(Succeed())
	eventuallyReason(g, pc, b2v1.ReasonPartnerRequiresMasterKey)
	g.Expect(pc.Status.KeyType).To(Equal(b2v1.KeyTypeApplication))
}

func TestPartnerAPINotEnabled(t *testing.T) {
	g := requireEnv(t)
	ctx := context.Background()
	c := uniqueCustomer("cust3003")
	// A customer account's master key: a master key, but not a Group admin.
	acct := newAccount(c, c, "us-west")
	g.Expect(k8s.Create(ctx, acct)).To(Succeed())
	eventuallyReason(g, acct, b2v1.ReasonReconciled)
	id := acct.Status.AccountID
	g.Expect(k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: operatorNS, Name: c + "-master"},
		StringData: map[string]string{"applicationKeyId": id, "applicationKey": fakeB2.AccountMasterKey(id)},
	})).To(Succeed())
	pc := partnerConfigFor(c+"-as-partner", c+"-master")
	g.Expect(k8s.Create(ctx, pc)).To(Succeed())
	eventuallyReason(g, pc, b2v1.ReasonPartnerAPINotEnabled)
	g.Expect(pc.Status.KeyType).To(Equal(b2v1.KeyTypeMaster))
	g.Expect(readyCondition(g, pc).Message).To(ContainSubstring("sales"))
}

func TestMasterKeyForBucketManagementIsFlagged(t *testing.T) {
	g := requireEnv(t)
	pc := &b2v1.ClusterProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	eventuallyReason(g, pc, b2v1.ReasonReconciled)
	g.Expect(pc.Status.KeyType).To(Equal(b2v1.KeyTypeMaster))
	g.Expect(readyCondition(g, pc).Message).To(ContainSubstring("use a restricted application key"))
}

func TestPartnerEmailTemplateValidation(t *testing.T) {
	g := requireEnv(t)
	for _, tpl := range []string{"{customer}@hosting.example.com", "{customer}-{region}", "{customer}-{region}@localhost"} {
		pc := &b2v1.ClusterProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-template"},
			Spec: b2v1.ClusterProviderConfigSpec{
				CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: "x"},
				Partner:              &b2v1.PartnerSettings{GroupID: "g", MemberEmailTemplate: tpl},
			},
		}
		err := k8s.Create(context.Background(), pc)
		g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "template %q: err = %v", tpl, err)
	}
}

func partnerConfigFor(name, secret string) *b2v1.ClusterProviderConfig {
	return &b2v1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: b2v1.ClusterProviderConfigSpec{
			APIURL:               fakeB2.URL(),
			CredentialsSecretRef: b2v1.CredentialsSecretReference{Namespace: operatorNS, Name: secret},
			Partner:              &b2v1.PartnerSettings{GroupID: groupID, MemberEmailTemplate: "{customer}-{region}@hosting.example.com"},
		},
	}
}
