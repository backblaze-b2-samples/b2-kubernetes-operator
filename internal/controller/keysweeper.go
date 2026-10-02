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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	b2v1 "github.com/backblaze-b2-samples/b2-kubernetes-operator/api/v1alpha1"
	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

var orphansRevoked = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "b2_operator_orphaned_keys_revoked_total",
	Help: "B2 keys created by this cluster whose owning resource no longer exists, revoked by the sweep.",
})

func init() { metrics.Registry.MustRegister(orphansRevoked) }

// KeySweeper periodically revokes B2 keys this cluster created for resources
// that no longer exist, for example because someone removed a finalizer by
// hand. Every key the operator creates is named
// b2op-<cluster>-<owner uid>-..., so only this cluster's keys are touched
// and each can be traced to the ApplicationKey, Bucket (replication keys) or
// B2Account (operations key) that owns it. It runs only on the leader.
type KeySweeper struct {
	Deps
	Interval time.Duration
}

// Start implements manager.Runnable.
func (s *KeySweeper) Start(ctx context.Context) error {
	if s.Interval <= 0 {
		return nil
	}
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
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
	prefix := KeyNamePrefix + "-" + s.Options.ClusterID + "-"
	for i := range pcs.Items {
		pc := &pcs.Items[i]
		if !isReady(pc) {
			continue
		}
		acct, err := s.Accounts.Get(ctx, pc)
		if err != nil {
			logger.Error(err, "skipping provider config", "providerConfig", pc.Name)
			continue
		}
		keys, err := acct.Client.FindKeys(ctx, func(k b2.ApplicationKey) bool { return strings.HasPrefix(k.KeyName, prefix) })
		if err != nil {
			logger.Error(err, "listing keys", "providerConfig", pc.Name)
			continue
		}
		if len(keys) == 0 {
			continue
		}
		// Owners are listed after the keys: any key created later belongs
		// to an owner that is already in the list.
		owners, err := s.liveOwners(ctx)
		if err != nil {
			return err
		}
		for _, k := range keys {
			owner, _, _ := strings.Cut(strings.TrimPrefix(k.KeyName, prefix), "-")
			if owners[owner] {
				continue
			}
			if err := acct.Client.DeleteKeyIfExists(ctx, k.ApplicationKeyID); err != nil {
				logger.Error(err, "revoking orphaned key", "keyID", k.ApplicationKeyID)
				continue
			}
			orphansRevoked.Inc()
			logger.Info("revoked orphaned key", "keyID", k.ApplicationKeyID, "keyName", k.KeyName, "providerConfig", pc.Name)
		}
	}
	return nil
}

// liveOwners returns the short UIDs of every resource that can own keys,
// read uncached so a resource created moments ago is never missed.
func (s *KeySweeper) liveOwners(ctx context.Context) (map[string]bool, error) {
	owners := map[string]bool{}
	for _, list := range []client.ObjectList{&b2v1.ApplicationKeyList{}, &b2v1.BucketList{}, &b2v1.B2AccountList{}} {
		if err := s.APIReader.List(ctx, list); err != nil {
			return nil, err
		}
		items, err := apimeta.ExtractList(list)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			owners[uid8(item.(client.Object).GetUID())] = true
		}
	}
	return owners, nil
}
