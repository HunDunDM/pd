// Copyright 2025 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package affinity

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// infoSyncer is used to synchronize information with RegionLabeler and storage.
// It has its own lock separate from the Manager, so that synchronizing data does not lock the entire Manager.
// It ensures all write tasks are executed sequentially. The Manager should first validate the data,
// and only after infoSyncer successfully writes it should the Manager update its in-memory information.
type infoSyncer struct {
	syncutil.RWMutex
	ctx           context.Context
	storage       endpoint.AffinityStorage
	regionLabeler *labeler.RegionLabeler // region labeler for syncing key ranges

	keyRanges map[string][]GroupKeyRange // {group_id} -> key ranges, cached in memory to reduce labeler lock contention
}

func newInfoSyncer(ctx context.Context, storage endpoint.AffinityStorage, regionLabeler *labeler.RegionLabeler) *infoSyncer {
	return &infoSyncer{
		ctx:           ctx,
		storage:       storage,
		regionLabeler: regionLabeler,
	}
}

func (s *infoSyncer) Initialize(f func(group *Group)) error {
	s.RLock()
	defer s.RUnlock()

	return s.storage.LoadAllAffinityGroups(func(k, v string) {
		group := &Group{}
		if err := json.Unmarshal([]byte(v), group); err != nil {
			log.Error("failed to unmarshal affinity group, skipping",
				zap.String("key", k),
				zap.Error(errs.ErrLoadRule.Wrap(err)))
			return
		}
		f(group)
	})
}
