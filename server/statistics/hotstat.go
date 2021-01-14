// Copyright 2020 TiKV Project Authors.
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

package statistics

// HotStat contains cluster's hotspot statistics.
type HotStat struct {
	*HotCache
	*StoresStats
}

// NewHotStat creates the container to hold cluster's hotspot statistics.
func NewHotStat() *HotStat {
	return &HotStat{
		HotCache:    NewHotCache(),
		StoresStats: NewStoresStats(),
	}
}

// GetStoresLoads returns all stores loads for scheduling.
// It specially handles `StoreSum*` by summarying region peer stats.
func (s *HotStat) GetStoresLoads() map[uint64][]float64 {
	ret := s.StoresStats.getStoresLoads()
	// To get close to the true store total value, hotDegree filter is not used here.
	peers := s.HotCache.RegionStats(WriteFlow, 0)
	for id := range ret {
		var sumLeaderBytes, sumLeaderKeys, sumPeerBytes, sumPeerKeys float64
		for _, peer := range peers[id] {
			sumPeerBytes += peer.ByteRate
			sumPeerKeys += peer.KeyRate
			if peer.isLeader {
				sumLeaderBytes += peer.ByteRate
				sumLeaderKeys += peer.KeyRate
			}
		}
		ret[id][StoreSumLeaderWriteBytes] = sumLeaderBytes
		ret[id][StoreSumLeaderWriteKeys] = sumLeaderKeys
		ret[id][StoreSumPeerWriteBytes] = sumPeerBytes
		ret[id][StoreSumPeerWriteKeys] = sumPeerKeys
	}
	return ret
}
