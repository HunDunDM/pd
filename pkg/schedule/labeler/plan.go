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

package labeler

import (
	"github.com/tikv/pd/pkg/storage/kv"
)

// Plan is an execution plan for the RegionLabeler.
// It is used to perform transactional commits together with other modules.
type Plan struct {
	labeler      *RegionLabeler
	err          error
	saveOps      []func(txn kv.Txn) error
	setLockedOps []func()
	isSaved      bool
	isSet        bool
}

// NewPlan creates a new execution plan.
func (l *RegionLabeler) NewPlan() *Plan {
	return &Plan{
		labeler:      l,
		err:          nil,
		saveOps:      make([]func(txn kv.Txn) error, 0),
		setLockedOps: make([]func(), 0),
	}
}

func (p *Plan) SetLabelRule(rule *LabelRule) error {
	if p.err != nil {
		return p.err
	}
	if err := rule.checkAndAdjust(); err != nil {
		p.err = err
		return err
	}
	p.saveOps = append(p.saveOps, func(txn kv.Txn) error {
		return p.labeler.storage.SaveRegionRule(txn, rule.ID, rule)
	})
	p.setLockedOps = append(p.setLockedOps, func() {
		p.labeler.labelRules[rule.ID] = rule
	})
	return nil
}

func (p *Plan) DeleteLabelRule(id string) error {
	if p.err != nil {
		return p.err
	}
	p.saveOps = append(p.saveOps, func(txn kv.Txn) error {
		return p.labeler.storage.DeleteRegionRule(txn, id)
	})
	p.setLockedOps = append(p.setLockedOps, func() {
		if _, ok := p.labeler.labelRules[id]; !ok {
			return
		}
		delete(p.labeler.labelRules, id)
	})
	return nil
}

func (p *Plan) SaveOps(txn kv.Txn) error {
	if p.err != nil || p.isSaved {
		return p.err
	}
	p.isSaved = true
	for _, op := range p.saveOps {
		p.err = op(txn)
		if p.err != nil {
			return p.err
		}
	}
	return nil
}

func (p *Plan) Set() error {
	if p.err != nil || p.isSet {
		return p.err
	}
	p.isSet = true

	p.labeler.Lock()
	defer p.labeler.Unlock()
	for _, op := range p.setLockedOps {
		op()
	}
	p.labeler.BuildRangeListLocked()
	return nil
}
