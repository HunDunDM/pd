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

package command

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

// NewAffinityCommand creates the affinity command.
func NewAffinityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "affinity",
		Short:             "affinity group commands",
		Long:              `Manage affinity groups for a single table by table ID and optional partition ID.`,
		PersistentPreRunE: requirePDClient,
	}
	cmd.PersistentFlags().Uint64("table-id", 0, "table ID for the affinity group")
	cmd.PersistentFlags().String("tablegroup", "", "tablegroup name for the affinity group, takes precedence over table-id")
	cmd.PersistentFlags().Uint64("partition-id", 0, "partition ID for partitioned tables")

	cmd.AddCommand(
		newAffinityShowCommand(),
		newAffinityDeleteCommand(),
		newAffinityUpdatePeersCommand(),
		newAffinityRebalanceCommand(),
	)
	return cmd
}

func newAffinityShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show affinity group state (all groups if no --table-id is specified)",
		Run:   affinityShowCommandFunc,
	}
	return cmd
}

func newAffinityDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete the affinity group for the target ID",
		Run:   affinityDeleteCommandFunc,
	}
	cmd.Flags().Bool("force", false, "force delete the affinity group even if it has key ranges")
	return cmd
}

func newAffinityUpdatePeersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update leader and voters for a specific affinity group",
		Run:   affinityUpdatePeersCommandFunc,
	}
	cmd.Flags().Uint64("leader", 0, "leader store ID")
	cmd.Flags().String("voters", "", "comma separated voter store IDs, e.g. 1,2,3")
	return cmd
}

func newAffinityRebalanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebalance",
		Short: "rebalance the leader and voters for all affinity groups",
		Run:   affinityRebalanceCommandFunc,
	}
	return cmd
}

func affinityShowCommandFunc(cmd *cobra.Command, _ []string) {
	tableID, _ := cmd.Flags().GetUint64("table-id")
	tableGroup, _ := cmd.Flags().GetString("tablegroup")

	// If no table-id or tablegroup is provided, show all affinity groups
	if tableID == 0 && tableGroup == "" {
		groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
		if err != nil {
			cmd.Printf("Failed to get affinity groups: %v\n", err)
			return
		}
		jsonPrint(cmd, groups)
		return
	}

	// Otherwise, show the specific affinity group
	groupID, err := getGroupID(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	state, err := PDCli.GetAffinityGroup(cmd.Context(), groupID)
	if err != nil {
		cmd.Printf("Failed to show affinity group %s: %v\n", groupID, err)
		return
	}
	jsonPrint(cmd, state)
}

func affinityDeleteCommandFunc(cmd *cobra.Command, _ []string) {
	groupID, err := getGroupID(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	force, _ := cmd.Flags().GetBool("force")
	if err := PDCli.DeleteAffinityGroup(cmd.Context(), groupID, force); err != nil {
		cmd.Printf("Failed to delete affinity group: %v\n", err)
		return
	}
	cmd.Printf("Affinity group %s deleted\n", groupID)
}

func affinityUpdatePeersCommandFunc(cmd *cobra.Command, _ []string) {
	groupID, err := getGroupID(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	leader, _ := cmd.Flags().GetUint64("leader")
	if leader == 0 {
		cmd.Println("leader is required")
		return
	}

	voterStr, _ := cmd.Flags().GetString("voters")
	voters, err := parseUint64List(voterStr)
	if err != nil {
		cmd.Printf("Failed to parse voters: %v\n", err)
		return
	}
	if len(voters) == 0 {
		cmd.Println("voters is required")
		return
	}

	state, err := PDCli.UpdateAffinityGroupPeers(cmd.Context(), groupID, leader, voters)
	if err != nil {
		cmd.Printf("Failed to update affinity group peers: %v\n", err)
		return
	}
	jsonPrint(cmd, state)
}

type storeStat struct {
	ID          uint64
	VoterCount  int
	LeaderCount int
}

func affinityRebalanceCommandFunc(cmd *cobra.Command, _ []string) {
	groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get affinity groups: %v\n", err)
		return
	}

	storesInfo, err := PDCli.GetStores(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get stores: %v\n", err)
		return
	}
	stores := storesInfo.Stores

	if len(groups) == 0 {
		cmd.Println("No affinity groups found")
		return
	}

	for _, group := range groups {
		if group.LeaderStoreID == 0 || len(group.VoterStoreIDs) == 0 {
			cmd.Printf("Group %s is not affinity, please wait for auto affinity or fix the issue", group.ID)
			return
		}
	}

	storeStats := make([]*storeStat, len(stores))
	for i, store := range stores {
		storeStats[i] = &storeStat{ID: uint64(store.Store.ID)}
	}

	sortedGroups := make([]*http.AffinityGroupState, 0, len(groups))
	for _, group := range groups {
		sortedGroups = append(sortedGroups, group)
	}
	slices.SortFunc(sortedGroups, func(a, b *http.AffinityGroupState) int {
		return strings.Compare(a.ID, b.ID)
	})

	for _, group := range sortedGroups {
		// Find the stores with the fewest voters
		slices.SortFunc(storeStats, func(a, b *storeStat) int {
			if a.VoterCount == b.VoterCount {
				return int(a.ID - b.ID)
			}
			return a.VoterCount - b.VoterCount
		})
		voterCount := len(group.VoterStoreIDs)
		voterStores := slices.Clone(storeStats[:voterCount])
		// Find the store with the fewest leader
		slices.SortFunc(voterStores, func(a, b *storeStat) int {
			if a.LeaderCount == b.LeaderCount {
				return int(a.ID - b.ID)
			}
			return a.LeaderCount - b.LeaderCount
		})
		leaderStore := voterStores[0]
		// Update the statistics
		for _, store := range voterStores {
			store.VoterCount++
		}
		leaderStore.LeaderCount++
		// Assign to the current Group.
		voterStoreIDs := make([]uint64, len(voterStores))
		voterStoreStrings := make([]string, len(voterStores))
		for i, store := range voterStores {
			voterStoreIDs[i] = store.ID
			voterStoreStrings[i] = strconv.FormatUint(store.ID, 10)
		}
		if _, err = PDCli.UpdateAffinityGroupPeers(cmd.Context(), group.ID, leaderStore.ID, voterStoreIDs); err != nil {
			cmd.Printf("Failed to update affinity group %s peers: %v\n", group.ID, err)
			return
		}
		cmd.Printf("Successfully updated affinity group %s peers. leader: %d stores: %s\n",
			group.ID,
			leaderStore.ID,
			strings.Join(voterStoreStrings, ","))
	}
}

func getGroupID(cmd *cobra.Command) (string, error) {
	tableGroup, _ := cmd.Flags().GetString("tablegroup")
	tableID, _ := cmd.Flags().GetUint64("table-id")
	partitionID, _ := cmd.Flags().GetUint64("partition-id")
	groupID, err := FormatGroupID(tableGroup, tableID, partitionID)
	if err != nil {
		return "", err
	}
	if err = affinity.ValidateGroupID(groupID); err != nil {
		return "", err
	}
	return groupID, nil
}

// FormatGroupID builds the affinity group ID from table/partition IDs following TiDB convention.
func FormatGroupID(tableGroup string, tableID, partitionID uint64) (string, error) {
	switch {
	case tableGroup != "" && tableID > 0:
		return "", errors.New("only one of --tablegroup or --table-id is required")
	case tableGroup != "" && partitionID > 0:
		return fmt.Sprintf("_tidb_ptg_%s_p%d", tableGroup, partitionID), nil
	case tableGroup != "" && partitionID == 0:
		return fmt.Sprintf("_tidb_tg_%s", tableGroup), nil
	case tableID > 0 && partitionID > 0:
		return fmt.Sprintf("_tidb_pt_%d_p%d", tableID, partitionID), nil
	case tableID > 0 && partitionID == 0:
		return fmt.Sprintf("_tidb_t_%d", tableID), nil
	default:
		return "", errors.New("--tablegroup or --table-id is required")
	}
}

func parseUint64List(input string) ([]uint64, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	parts := strings.Split(input, ",")
	res := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		res = append(res, v)
	}
	return res, nil
}
