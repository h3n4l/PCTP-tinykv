// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeInfoSlice []*core.StoreInfo

func (s storeInfoSlice) Len() int           { return len(s) }
func (s storeInfoSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s storeInfoSlice) Less(i, j int) bool { return s[i].GetRegionSize() < s[j].GetRegionSize() }

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := make(storeInfoSlice, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() {
			if store.DownTime() < cluster.GetMaxStoreDownTime() {
				stores = append(stores, store)
			}
		}
	}
	if len(stores) < 2 {
		return nil
	}
	var regionInfo *core.RegionInfo
	var originStoreInfo, destStoreInfo *core.StoreInfo
	// sort so we can get the max size region and check it

	sort.Sort(stores)
	var i int
	for i = len(stores) - 1; i > 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		regionInfo = regions.RandomRegion(nil, nil)
		if regionInfo != nil {
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		regionInfo = regions.RandomRegion(nil, nil)
		if regionInfo != nil {
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		regionInfo = regions.RandomRegion(nil, nil)
		if regionInfo != nil {
			break
		}
	}
	if regionInfo == nil {
		return nil
	}
	originStoreInfo = stores[i]

	storeIds := regionInfo.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}
	for k := 0; k < i; k++ {
		if _, ok := storeIds[stores[k].GetID()]; !ok {
			destStoreInfo = stores[k]
			break
		}
	}
	if destStoreInfo == nil {
		return nil
	}
	if originStoreInfo.GetRegionSize()-destStoreInfo.GetRegionSize() < 2*regionInfo.GetApproximateSize() {
		return nil
	}
	peer, err := cluster.AllocPeer(destStoreInfo.GetID())
	if err != nil {
		return nil
	}
	desc := fmt.Sprintf("move to %d from %d", destStoreInfo.GetID(), originStoreInfo.GetID())
	oper, err := operator.CreateMovePeerOperator(desc, cluster, regionInfo, operator.OpBalance, originStoreInfo.GetID(), destStoreInfo.GetID(), peer.GetId())
	if err != nil {
		return nil
	}
	return oper
}