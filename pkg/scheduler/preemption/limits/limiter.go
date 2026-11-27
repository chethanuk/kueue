/*
Copyright The Kubernetes Authors.

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

package limits

import (
	"fmt"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/util/expectations"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
)

// PreemptionLimiter enforces the PreemptionLimits of a snapshot that apply to
// a PreemptionConfig, during a single scheduling attempt.
//
// A nil PreemptionLimiter imposes no limits.
type PreemptionLimiter struct {
	log          logr.Logger
	matchers     map[policy.PreemptionLimitReference]*preemptionLimitMatcher
	expectations *expectations.Store
	snapshot     *PreemptionLimitsSnapshot
}

// NewPreemptionLimiter returns a PreemptionLimiter for the PreemptionLimits of
// snapshot that apply to config. snapshot, config, and preemptionExpectations must not be nil.
func NewPreemptionLimiter(
	log logr.Logger,
	snapshot *PreemptionLimitsSnapshot,
	config *kueuealpha.PreemptionConfig,
	preemptionExpectations *expectations.Store,
) (*PreemptionLimiter, error) {
	matchers := make(map[policy.PreemptionLimitReference]*preemptionLimitMatcher)
	for ref, state := range snapshot.limits {
		if state.limit == nil {
			continue
		}
		matcher, err := newPreemptionLimitMatcher(state.limit, config)
		if err != nil {
			return nil, err
		}
		if matcher != nil {
			matchers[ref] = matcher
		}
	}
	return &PreemptionLimiter{
		log:          log,
		matchers:     matchers,
		expectations: preemptionExpectations,
		snapshot:     snapshot,
	}, nil
}

// consumedLimits returns the limits and aggregation keys that preempting target for
// preemptor consumes, or no limits if target's preemption is already in progress.
func (l *PreemptionLimiter) consumedLimits(
	preemptor, target *workload.Info,
	preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot,
) policy.ConsumedLimits {
	if l == nil || l.preemptionInProgress(target) {
		return nil
	}
	consumedLimits := make(policy.ConsumedLimits)
	for ref, matcher := range l.matchers {
		if matcher.matchesClusterQueues(preemptorCQ, targetCQ) {
			consumedLimits[ref] = policy.NewConsumedLimit(matcher.limit, matcher.aggregationKey(preemptor, target))
		}
	}
	return consumedLimits
}

// preemptionInProgress reports whether target's eviction is already in progress.
// A waiting preemptor re-selects such a target every cycle until the target's
// quota is released, and IssuePreemptions does not evict or record it again.
func (l *PreemptionLimiter) preemptionInProgress(target *workload.Info) bool {
	return workloadevict.IsEvicted(target.Obj) ||
		!l.expectations.Satisfied(l.log, client.ObjectKeyFromObject(target.Obj))
}

// preemptionLimitMatcher determines whether a PreemptionLimit applies to a
// preemption and which aggregation key it counts against.
type preemptionLimitMatcher struct {
	limit                         *kueuealpha.PreemptionLimit
	preemptorClusterQueueSelector labels.Selector
	preempteeClusterQueueSelector labels.Selector
}

// selectorError reports an invalid selector on a PreemptionLimit.
type selectorError struct {
	PreemptionLimit string
	Selector        string
	Err             error
}

func (e *selectorError) Error() string {
	return fmt.Sprintf("preemptionLimit %q %s: %v", e.PreemptionLimit, e.Selector, e.Err)
}

func (e *selectorError) Unwrap() error {
	return e.Err
}

// newPreemptionLimitMatcher returns the matcher for limit, or nil if limit
// doesn't apply to config. It returns an error if a selector it needs is invalid.
func newPreemptionLimitMatcher(limit *kueuealpha.PreemptionLimit, config *kueuealpha.PreemptionConfig) (*preemptionLimitMatcher, error) {
	configSelector, err := metav1.LabelSelectorAsSelector(&limit.Spec.ConfigSelector)
	if err != nil {
		return nil, &selectorError{PreemptionLimit: limit.Name, Selector: "configSelector", Err: err}
	}
	if !configSelector.Matches(labels.Set(config.Labels)) {
		return nil, nil
	}
	preemptorClusterQueueSelector, err := selectorOrEverything(limit.Spec.PreemptorClusterQueueSelector)
	if err != nil {
		return nil, &selectorError{PreemptionLimit: limit.Name, Selector: "preemptorClusterQueueSelector", Err: err}
	}
	preempteeClusterQueueSelector, err := selectorOrEverything(limit.Spec.PreempteeClusterQueueSelector)
	if err != nil {
		return nil, &selectorError{PreemptionLimit: limit.Name, Selector: "preempteeClusterQueueSelector", Err: err}
	}
	return &preemptionLimitMatcher{
		limit:                         limit,
		preemptorClusterQueueSelector: preemptorClusterQueueSelector,
		preempteeClusterQueueSelector: preempteeClusterQueueSelector,
	}, nil
}

// selectorOrEverything is metav1.LabelSelectorAsSelector, except that nil selects everything.
func selectorOrEverything(ls *metav1.LabelSelector) (labels.Selector, error) {
	if ls == nil {
		return labels.Everything(), nil
	}
	return metav1.LabelSelectorAsSelector(ls)
}

// matchesClusterQueues returns whether the limit's ClusterQueue selectors match
// preemptorCQ and targetCQ.
func (m *preemptionLimitMatcher) matchesClusterQueues(preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot) bool {
	return m.preemptorClusterQueueSelector.Matches(labels.Set(preemptorCQ.Labels)) &&
		m.preempteeClusterQueueSelector.Matches(labels.Set(targetCQ.Labels))
}

// aggregationKey returns an aggregation key that identifies the preemption within the limit saved in the matcher.
func (m *preemptionLimitMatcher) aggregationKey(preemptor, target *workload.Info) policy.PreemptionLimitAggregationKey {
	switch m.limit.Spec.Aggregation {
	case kueuealpha.ByPreemptorCQLimitAggregation:
		return policy.PreemptionLimitAggregationKey(preemptor.ClusterQueue)
	case kueuealpha.ByPreempteeCQLimitAggregation:
		return policy.PreemptionLimitAggregationKey(target.ClusterQueue)
	case kueuealpha.ByPreempteeWorkloadLimitAggregation:
		return policy.PreemptionLimitAggregationKey(workload.Key(target.Obj))
	default:
		// GlobalLimitAggregation, as the API admits no other aggregation.
		return policy.GlobalAggregationKey
	}
}
