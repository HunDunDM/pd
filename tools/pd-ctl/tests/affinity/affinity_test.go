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

package affinity_test

import (
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/schedule/affinity"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestAffinityCommands(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	// Prepare stores for peer updates.
	for id := uint64(1); id <= 2; id++ {
		pdTests.MustPutStore(re, cluster, &metapb.Store{Id: id, State: metapb.StoreState_Up})
	}

	manager, err := leaderServer.GetServer().GetAffinityManager()
	re.NoError(err)

	// Pre-create groups so CLI commands can operate on them.
	const tableGroup = "pd-test-table-group"
	const tableID = uint64(1001)
	const partitionID = uint64(3)
	tGroup, err := command.FormatGroupID("", tableID, 0)
	re.NoError(err)
	ptGroup, err := command.FormatGroupID("", tableID, partitionID)
	re.NoError(err)
	tgGroup, err := command.FormatGroupID(tableGroup, 0, 0)
	re.NoError(err)
	ptgGroup, err := command.FormatGroupID(tableGroup, 0, partitionID)
	re.NoError(err)
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{
		{GroupID: tGroup},
		{GroupID: ptGroup},
		{GroupID: tgGroup},
		{GroupID: ptgGroup},
	}))

	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	// show should return both groups with the expected IDs.
	groups := make(map[string]*pd.AffinityGroupState)
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "config", "affinity", "show"}, &groups)
	re.Contains(groups, tGroup)
	re.Contains(groups, ptGroup)
	re.Contains(groups, tgGroup)
	re.Contains(groups, ptgGroup)

	// show table affinity group
	var state pd.AffinityGroupState
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "show",
		"--table-id", strconv.FormatUint(tableID, 10),
	}, &state)
	re.Equal(tGroup, state.ID)

	// show partitioned table affinity group
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "show",
		"--table-id", strconv.FormatUint(tableID, 10),
		"--partition-id", strconv.FormatUint(partitionID, 10),
	}, &state)
	re.Equal(ptGroup, state.ID)

	// show tablegroup affinity group
	cmd = ctl.GetRootCmd() // reset cmd to clean args
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "show",
		"--tablegroup", tableGroup,
	}, &state)
	re.Equal(tgGroup, state.ID)

	// show partitioned tablegroup affinity group
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "show",
		"--tablegroup", tableGroup,
		"--partition-id", strconv.FormatUint(partitionID, 10),
	}, &state)
	re.Equal(ptgGroup, state.ID)

	// Specifying both --tablegroup and --table-id at the same time is not allowed
	out := tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "show",
		"--tablegroup", tableGroup,
		"--table-id", strconv.FormatUint(tableID, 10),
		"--partition-id", strconv.FormatUint(partitionID, 10),
	}, nil)
	re.Contains(out, "only one of --tablegroup or --table-id is required")

	// update peers for the partitioned table affinity group
	cmd = ctl.GetRootCmd() // reset cmd to clean args
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "update",
		"--table-id", strconv.FormatUint(tableID, 10),
		"--partition-id", strconv.FormatUint(partitionID, 10),
		"--leader", "1",
		"--voters", "1,2",
	}, &state)
	re.Equal(ptGroup, state.ID)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1, 2}, state.VoterStoreIDs)

	// delete the normal table affinity group
	cmd = ctl.GetRootCmd() // reset cmd to clean args
	out = tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "delete",
		"--table-id", strconv.FormatUint(tableID, 10),
	}, nil)
	re.Contains(out, tGroup)

	// ensure only the partitioned table affinity group remains.
	groups = make(map[string]*pd.AffinityGroupState)
	cmd = ctl.GetRootCmd() // reset cmd to clean args
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "config", "affinity", "show"}, &groups)
	re.NotContains(groups, tGroup)
	re.Contains(groups, ptGroup)
}

func TestAffinityRebalanceCommand(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	manager, err := leaderServer.GetServer().GetAffinityManager()
	re.NoError(err)

	for id := uint64(1); id <= 6; id++ {
		pdTests.MustPutStore(re, cluster, &metapb.Store{Id: id, State: metapb.StoreState_Up})
	}
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{
		{GroupID: "test1"},
		{GroupID: "test2"},
		{GroupID: "test3"},
	}))
	_, err = manager.UpdateAffinityGroupPeers("test1", 1, []uint64{1, 2, 3})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("test2", 1, []uint64{1, 2, 3})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("test3", 1, []uint64{1, 2, 3})
	re.NoError(err)

	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()
	_ = tests.MustExec(re, cmd, []string{"-u", pdAddr, "config", "affinity", "rebalance"}, nil)

	group := manager.GetAffinityGroupState("test1")
	re.NotNil(group)
	re.Equal(uint64(1), group.LeaderStoreID)
	re.True(slices.Equal([]uint64{1, 2, 3}, group.VoterStoreIDs))

	group = manager.GetAffinityGroupState("test2")
	re.NotNil(group)
	re.Equal(uint64(4), group.LeaderStoreID)
	re.True(slices.Equal([]uint64{4, 5, 6}, group.VoterStoreIDs))

	group = manager.GetAffinityGroupState("test3")
	re.NotNil(group)
	re.Equal(uint64(2), group.LeaderStoreID)
	re.True(slices.Equal([]uint64{1, 2, 3}, group.VoterStoreIDs))
}
