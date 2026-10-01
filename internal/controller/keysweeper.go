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
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

var orphansRevoked = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "b2_operator_orphaned_keys_revoked_total",
	Help: "B2 keys created by this cluster whose ApplicationKey no longer exists, revoked by the sweep.",
})

func init() { metrics.Registry.MustRegister(orphansRevoked) }

// KeySweeper periodically revokes B2 keys that this cluster created for
// ApplicationKeys that no longer exist, for example because someone removed
// the finalizer before the key could be revoked. Only keys whose names carry
// this cluster's ID are considered, so clusters sharing a B2 account do not
// interfere. It runs only on the leader.
type KeySweeper struct {
	Deps
	// APIReader lists ApplicationKeys uncached, so a resource created moments
	// ago is never mistaken for a missing one.
	APIReader client.Reader
	Interval  time.Duration
}

// Start implements manager.Runnable.
func (s *KeySweeper) Start(ctx context.Context) error {
	if s.Interval <= 0 {
		return nil
	}
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.Sweep(ctx); err != nil {
				log.FromContext(ctx).Error(err, "orphaned key sweep failed")
			}
		}
	}
}

// Sweep runs one pass over every ready provider config.
func (s *KeySweeper) Sweep(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("key-sweeper")
	var pcs b2v1.ClusterProviderConfigList
	if err := s.APIReader.List(ctx, &pcs); err != nil {
		return err
	}
	for i := range pcs.Items {
		pc := &pcs.Items[i]
		if !meta.IsStatusConditionTrue(pc.Status.Conditions, b2v1.ConditionReady) {
			continue
		}
		acct, err := s.Registry.Get(ctx, pc)
		if err != nil {
			logger.Error(err, "skipping provider config", "providerConfig", pc.Name)
			continue
		}
		// List resources after keys: a key created after the resource list
		// was taken belongs to a resource that is in it.
		prefix := KeyNamePrefix + "-" + s.Options.ClusterID + "-"
		keys, err := acct.Client.FindKeys(ctx, func(k b2.ApplicationKey) bool { return strings.HasPrefix(k.KeyName, prefix) })
		if err != nil {
			logger.Error(err, "listing keys", "providerConfig", pc.Name)
			continue
		}
		if len(keys) == 0 {
			continue
		}
		// Keys belong to ApplicationKeys, Buckets (replication keys) or
		// B2Accounts (operations keys).
		var aks b2v1.ApplicationKeyList
		if err := s.APIReader.List(ctx, &aks); err != nil {
			return err
		}
		var buckets b2v1.BucketList
		if err := s.APIReader.List(ctx, &buckets); err != nil {
			return err
		}
		live := map[string]bool{}
		for _, ak := range aks.Items {
			live[uid8(ak.UID)] = true
		}
		for _, b := range buckets.Items {
			live[uid8(b.UID)] = true
		}
		var accounts b2v1.B2AccountList
		if err := s.APIReader.List(ctx, &accounts); err != nil {
			return err
		}
		for _, a := range accounts.Items {
			live[uid8(a.UID)] = true
		}
		for _, k := range keys {
			parts := strings.SplitN(strings.TrimPrefix(k.KeyName, prefix), "-", 2)
			if len(parts) < 2 || live[parts[0]] {
				continue
			}
			if err := acct.Client.DeleteKey(ctx, k.ApplicationKeyID); err != nil && !b2.HasCode(err, b2.CodeBadRequest) {
				logger.Error(err, "revoking orphaned key", "keyID", k.ApplicationKeyID)
				continue
			}
			orphansRevoked.Inc()
			logger.Info("revoked orphaned key", "keyID", k.ApplicationKeyID, "keyName", k.KeyName, "providerConfig", pc.Name)
		}
	}
	return nil
}
