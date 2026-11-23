// Copyright 2019-2026 The Liqo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package offloading

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8shelper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	liqoconst "github.com/liqotech/liqo/pkg/consts"
	"github.com/liqotech/liqo/pkg/utils"
	virtualnodeutils "github.com/liqotech/liqo/pkg/utils/virtualnode"
)

// DefaultMaxMergedSelectorTerms is the default upper bound for the number of terms produced by the
// cartesian product of the pod required node affinity and the selector imposed by the
// NamespaceOffloading (e.g., 3 pod terms and 4 cluster terms produce 12 merged terms).
// It is only a default: callers can tune it through Input.MaxMergedSelectorTerms.
const DefaultMaxMergedSelectorTerms = 64

// ReasonSource identifies the placement layer responsible for the exclusion of a node.
type ReasonSource string

const (
	// ReasonSourceStrategy marks exclusions due to the PodOffloadingStrategy (e.g., a local node
	// cannot host a pod when the strategy is Remote).
	ReasonSourceStrategy ReasonSource = "PodOffloadingStrategy"
	// ReasonSourceClusterSelector marks exclusions due to the NamespaceOffloading ClusterSelector.
	ReasonSourceClusterSelector ReasonSource = "ClusterSelector"
	// ReasonSourcePodNodeSelector marks exclusions due to the pod spec.nodeSelector.
	ReasonSourcePodNodeSelector ReasonSource = "PodNodeSelector"
	// ReasonSourcePodAffinity marks exclusions due to the pod required node affinity.
	ReasonSourcePodAffinity ReasonSource = "PodNodeAffinity"
	// ReasonSourceTaint marks exclusions due to a node taint not tolerated by the pod.
	ReasonSourceTaint ReasonSource = "NodeTaint"
)

// Reason describes a single cause of the exclusion of a node.
type Reason struct {
	Source  ReasonSource
	Message string
}

// NodeView carries the labels and taints of a candidate node, either local or virtual.
type NodeView struct {
	Name    string
	Virtual bool
	Labels  map[string]string
	Taints  []corev1.Taint
}

// NodeExclusion lists the reasons preventing a node from hosting the pod.
type NodeExclusion struct {
	Node    string
	Virtual bool
	Reasons []Reason
}

// Input groups the inputs of the placement resolution. It is never modified by ResolvePlacement.
type Input struct {
	// NamespaceOffloading is the NamespaceOffloading resource of the pod namespace.
	NamespaceOffloading *offloadingv1beta1.NamespaceOffloading
	// PodNodeSelector is the pod spec.nodeSelector (ANDed with every other condition).
	PodNodeSelector map[string]string
	// RequiredNodeAffinity is the pod required node affinity, if any. A nil selector, or one with
	// zero terms, is treated as absent, mirroring the webhook, which replaces it with the imposed one.
	RequiredNodeAffinity *corev1.NodeSelector
	// Tolerations are the pod spec.tolerations.
	Tolerations []corev1.Toleration
	// Nodes are the candidate nodes, local and virtual, in evaluation order. The same order is
	// preserved in the output diagnostics.
	Nodes []NodeView
	// RuntimeClassName is the pod spec.runtimeClassName, if set.
	RuntimeClassName *string
	// LiqoRuntimeClassName is the name of the Liqo runtime class: pods using it are not mutated
	// by the webhook, and their placement is resolved against their own conditions only.
	LiqoRuntimeClassName string
	// MaxMergedSelectorTerms bounds the cartesian product of the pod affinity terms and the imposed
	// selector terms. When zero or negative, DefaultMaxMergedSelectorTerms is used.
	MaxMergedSelectorTerms int
}

// Result is the outcome of the placement resolution.
type Result struct {
	// Strategy is the PodOffloadingStrategy of the NamespaceOffloading resource.
	Strategy offloadingv1beta1.PodOffloadingStrategyType
	// Bypassed reports whether the pod uses the Liqo runtime class, in which case the webhook
	// applies no mutation and the pod is evaluated against its own conditions only.
	Bypassed bool
	// BypassReason explains, when Bypassed is true, why no constraint is imposed.
	BypassReason string
	// AddedToleration is the virtual-node toleration the webhook would add to the pod, if any.
	AddedToleration *corev1.Toleration
	// ImposedNodeSelector is the node selector imposed by the NamespaceOffloading (strategy and
	// ClusterSelector), if any.
	ImposedNodeSelector *corev1.NodeSelector
	// EffectiveNodeSelector is the final required node affinity of the mutated pod, i.e., the
	// cartesian product of the pod required affinity and the imposed selector, if any.
	EffectiveNodeSelector *corev1.NodeSelector
	// Candidates are the names of the nodes that can host the pod, in input order.
	Candidates []string
	// Exclusions detail, per node and in input order, why a node cannot host the pod.
	Exclusions []NodeExclusion
}

// VirtualNodeToleration returns the toleration injected by the webhook into the pods of namespaces
// offloaded with the Remote or LocalAndRemote strategies, allowing them to run on virtual nodes.
func VirtualNodeToleration() corev1.Toleration {
	return corev1.Toleration{
		Key:      liqoconst.VirtualNodeTolerationKey,
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoExecute,
	}
}

// VirtualNodeTaint returns the taint carried by the Liqo virtual nodes.
func VirtualNodeTaint() corev1.Taint {
	return corev1.Taint{
		Key:    liqoconst.VirtualNodeTolerationKey,
		Value:  "true",
		Effect: corev1.TaintEffectNoExecute,
	}
}

// ImposedToleration returns the toleration imposed by the given PodOffloadingStrategy, or nil when
// the strategy does not require any (Local). An error is returned for unknown strategies.
func ImposedToleration(strategy offloadingv1beta1.PodOffloadingStrategyType) (*corev1.Toleration, error) {
	switch strategy {
	case offloadingv1beta1.RemotePodOffloadingStrategyType, offloadingv1beta1.LocalAndRemotePodOffloadingStrategyType:
		toleration := VirtualNodeToleration()
		return &toleration, nil
	case offloadingv1beta1.LocalPodOffloadingStrategyType:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown PodOffloadingStrategyType %q", strategy)
	}
}

// ImposedNodeSelector builds the node selector imposed by the NamespaceOffloading resource,
// according to its PodOffloadingStrategy:
//   - Local: no selector is imposed (nil), the pod keeps its own conditions;
//   - Remote: the liqo.io/type=virtual-node requirement is appended to every ClusterSelector term
//     (or to a single new term when the ClusterSelector is empty), to exclude the local nodes;
//   - LocalAndRemote: when the ClusterSelector is not empty, an additional term is appended to
//     allow scheduling on local nodes as well; an empty ClusterSelector imposes no constraint.
//
// The returned selector is built over a deep copy of the ClusterSelector: the input object, which
// may be backed by the informer cache, is never modified.
func ImposedNodeSelector(nsoff *offloadingv1beta1.NamespaceOffloading) (*corev1.NodeSelector, error) {
	nodeSelector := nsoff.Spec.ClusterSelector.DeepCopy()
	switch nsoff.Spec.PodOffloadingStrategy {
	case offloadingv1beta1.RemotePodOffloadingStrategyType:
		// To ensure that the pod is not scheduled on local nodes, a requirement of the
		// liqo.io/type=virtual-node label is added to every term.
		if len(nodeSelector.NodeSelectorTerms) == 0 {
			nodeSelector.NodeSelectorTerms = []corev1.NodeSelectorTerm{{}}
		}
		for i := range nodeSelector.NodeSelectorTerms {
			nodeSelector.NodeSelectorTerms[i].MatchExpressions = append(nodeSelector.NodeSelectorTerms[i].MatchExpressions,
				corev1.NodeSelectorRequirement{
					Key:      liqoconst.TypeLabel,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{liqoconst.TypeNode},
				})
		}
	case offloadingv1beta1.LocalAndRemotePodOffloadingStrategyType:
		// An empty selector already allows scheduling on all nodes, no modification is needed.
		if len(nodeSelector.NodeSelectorTerms) == 0 {
			return nil, nil
		}
		// Otherwise, add a term allowing the scheduling on local nodes too.
		nodeSelector.NodeSelectorTerms = append(nodeSelector.NodeSelectorTerms, corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      liqoconst.TypeLabel,
				Operator: corev1.NodeSelectorOpNotIn,
				Values:   []string{liqoconst.TypeNode},
			}},
		})
	case offloadingv1beta1.LocalPodOffloadingStrategyType:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown PodOffloadingStrategyType %q", nsoff.Spec.PodOffloadingStrategy)
	}
	return nodeSelector, nil
}

// LoadVirtualNodeViews builds the NodeViews of the given virtual nodes, forging their fake node
// representation (labels) through ForgeFakeNodeFromVirtualNode, and attaching the taint carried by
// the Liqo virtual nodes. The liqo.io/type=virtual-node label is added when missing, since the
// nodes created by the virtual kubelet always carry it.
func LoadVirtualNodeViews(ctx context.Context, cl client.Client,
	virtualNodes []offloadingv1beta1.VirtualNode) ([]NodeView, error) {
	views := make([]NodeView, 0, len(virtualNodes))
	for i := range virtualNodes {
		node, err := virtualnodeutils.ForgeFakeNodeFromVirtualNode(ctx, cl, &virtualNodes[i])
		if err != nil {
			return nil, fmt.Errorf("failed to forge fake node from VirtualNode %q: %w", virtualNodes[i].Name, err)
		}
		nodeLabels := map[string]string{}
		for k, v := range node.Labels {
			nodeLabels[k] = v
		}
		if _, ok := nodeLabels[liqoconst.TypeLabel]; !ok {
			nodeLabels[liqoconst.TypeLabel] = liqoconst.TypeNode
		}
		views = append(views, NodeView{
			Name:    node.Name,
			Virtual: true,
			Labels:  nodeLabels,
			Taints:  []corev1.Taint{VirtualNodeTaint()},
		})
	}
	return views, nil
}

// ResolvePlacement resolves the placement constraints of a pod in an offloaded namespace against
// the given candidate nodes. It is a pure function: the input is never modified, and the same
// input always produces the same output, with diagnostics in a stable order (nodes in input order,
// reasons grouped by layer: strategy, ClusterSelector, pod nodeSelector, pod affinity, taints).
func ResolvePlacement(in *Input) (*Result, error) {
	if in == nil || in.NamespaceOffloading == nil {
		return nil, fmt.Errorf("a NamespaceOffloading resource is required to resolve the placement")
	}
	nsoff := in.NamespaceOffloading
	strategy := nsoff.Spec.PodOffloadingStrategy

	maxTerms := in.MaxMergedSelectorTerms
	if maxTerms <= 0 {
		maxTerms = DefaultMaxMergedSelectorTerms
	}

	res := &Result{Strategy: strategy}

	// Mirror the webhook: pods with the Liqo runtime class are not mutated, hence no toleration is
	// added and no selector is imposed; the pod is evaluated against its own conditions only.
	bypassed := in.RuntimeClassName != nil && *in.RuntimeClassName == in.LiqoRuntimeClassName

	tolerations := in.Tolerations
	var imposed *corev1.NodeSelector
	switch {
	case strategy == offloadingv1beta1.LocalPodOffloadingStrategyType:
		// Nothing is imposed: the pod keeps its own conditions, and virtual nodes are naturally
		// excluded by their taint, since no toleration is added.
	case bypassed:
		res.Bypassed = true
		res.BypassReason = fmt.Sprintf("the pod uses the Liqo runtime class %q: the webhook does not mutate "+
			"its node affinity and tolerations", in.LiqoRuntimeClassName)
	default:
		toleration, err := ImposedToleration(strategy)
		if err != nil {
			return nil, err
		}
		res.AddedToleration = toleration
		tolerations = append(append([]corev1.Toleration{}, in.Tolerations...), *toleration)

		imposed, err = ImposedNodeSelector(nsoff)
		if err != nil {
			return nil, err
		}
		res.ImposedNodeSelector = imposed
	}

	// Compute the effective required node affinity of the mutated pod, mirroring the webhook:
	// a pod affinity with zero terms is replaced by the imposed selector, otherwise the two are
	// merged through the cartesian product of their terms, bounded by the configured limit.
	effective, err := effectiveNodeSelector(in.RequiredNodeAffinity, imposed, maxTerms)
	if err != nil {
		return nil, err
	}
	res.EffectiveNodeSelector = effective

	for i := range in.Nodes {
		reasons := evaluateNode(&in.Nodes[i], in, imposed, tolerations)
		if len(reasons) == 0 {
			res.Candidates = append(res.Candidates, in.Nodes[i].Name)
		} else {
			res.Exclusions = append(res.Exclusions, NodeExclusion{
				Node:    in.Nodes[i].Name,
				Virtual: in.Nodes[i].Virtual,
				Reasons: reasons,
			})
		}
	}

	return res, nil
}

// effectiveNodeSelector merges the pod required affinity with the imposed selector, like the
// webhook does. The merge is rejected, never truncated, when the cartesian product of the terms
// exceeds maxTerms.
func effectiveNodeSelector(podAffinity, imposed *corev1.NodeSelector, maxTerms int) (*corev1.NodeSelector, error) {
	podHasAffinity := podAffinity != nil && len(podAffinity.NodeSelectorTerms) > 0
	switch {
	case !podHasAffinity && imposed == nil:
		return nil, nil
	case !podHasAffinity:
		return imposed.DeepCopy(), nil
	case imposed == nil:
		return podAffinity.DeepCopy(), nil
	default:
		merged, err := utils.MergeNodeSelectorWithLimit(podAffinity, imposed, maxTerms)
		if err != nil {
			return nil, fmt.Errorf("failed to merge the pod node affinity with the imposed node selector: %w", err)
		}
		return &merged, nil
	}
}

// evaluateNode checks a single node against every placement layer, in a fixed order, and returns
// the exclusion reasons (empty when the node is a candidate).
func evaluateNode(node *NodeView, in *Input, imposed *corev1.NodeSelector, tolerations []corev1.Toleration) []Reason {
	var reasons []Reason

	// Layer 1: the selector imposed by the NamespaceOffloading (strategy + ClusterSelector).
	if imposed != nil {
		if match, err := matchNodeSelectorTerms(node, imposed); err != nil {
			reasons = append(reasons, Reason{ReasonSourceStrategy, fmt.Sprintf("invalid imposed node selector: %v", err)})
		} else if !match {
			reasons = append(reasons, imposedExclusionReasons(node, in.NamespaceOffloading)...)
		}
	}

	// Layer 2: the pod spec.nodeSelector.
	if mismatched := mismatchedNodeSelectorKeys(node, in.PodNodeSelector); len(mismatched) > 0 {
		reasons = append(reasons, Reason{ReasonSourcePodNodeSelector,
			fmt.Sprintf("the node labels do not match the pod nodeSelector (mismatched keys: %s)", strings.Join(mismatched, ", "))})
	}

	// Layer 3: the pod required node affinity. A nil or empty-terms affinity is replaced by the
	// webhook with the imposed selector (already evaluated above), hence it is skipped here.
	if in.RequiredNodeAffinity != nil && len(in.RequiredNodeAffinity.NodeSelectorTerms) > 0 {
		if match, err := matchNodeSelectorTerms(node, in.RequiredNodeAffinity); err != nil {
			reasons = append(reasons, Reason{ReasonSourcePodAffinity, fmt.Sprintf("invalid pod node affinity: %v", err)})
		} else if !match {
			reasons = append(reasons, Reason{ReasonSourcePodAffinity,
				"the node does not match any term of the pod required node affinity"})
		}
	}

	// Layer 4: the node taints not tolerated by the pod (accounting for the added toleration).
	for i := range node.Taints {
		taint := &node.Taints[i]
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !toleratesTaint(tolerations, taint) {
			reasons = append(reasons, Reason{ReasonSourceTaint,
				fmt.Sprintf("the node taint %s is not tolerated by the pod", taintString(taint))})
		}
	}

	return reasons
}

// imposedExclusionReasons attributes the failure to match the imposed selector to the strategy
// and/or to the ClusterSelector. The imposed selector is the conjunction of the ClusterSelector
// terms with the strategy requirement (Remote), or the ClusterSelector terms plus a term allowing
// local nodes (LocalAndRemote), hence a failure always stems from one of the two, or both.
func imposedExclusionReasons(node *NodeView, nsoff *offloadingv1beta1.NamespaceOffloading) []Reason {
	var reasons []Reason
	isVirtual := node.Labels[liqoconst.TypeLabel] == liqoconst.TypeNode

	if nsoff.Spec.PodOffloadingStrategy == offloadingv1beta1.RemotePodOffloadingStrategyType && !isVirtual {
		reasons = append(reasons, Reason{ReasonSourceStrategy,
			"the Remote pod offloading strategy restricts the scheduling to virtual nodes (liqo.io/type=virtual-node)"})
	}

	if len(nsoff.Spec.ClusterSelector.NodeSelectorTerms) > 0 {
		if match, err := matchNodeSelectorTerms(node, &nsoff.Spec.ClusterSelector); err != nil {
			reasons = append(reasons, Reason{ReasonSourceClusterSelector, fmt.Sprintf("invalid ClusterSelector: %v", err)})
		} else if !match {
			reasons = append(reasons, Reason{ReasonSourceClusterSelector,
				"the node does not match any term of the NamespaceOffloading ClusterSelector"})
		}
	}

	if len(reasons) == 0 {
		// Defensive fallback: the imposed selector was not matched, but neither the strategy
		// requirement nor the ClusterSelector alone explain it (it should never happen).
		reasons = append(reasons, Reason{ReasonSourceStrategy,
			"the node does not match the node selector imposed by the pod offloading strategy"})
	}
	return reasons
}

// matchNodeSelectorTerms evaluates a node selector against the node view, through the same helper
// used by the Kubernetes scheduler: terms are ORed, requirements inside a term are ANDed, empty
// terms match no objects, and MatchFields (e.g., metadata.name) are honored.
func matchNodeSelectorTerms(node *NodeView, selector *corev1.NodeSelector) (bool, error) {
	return k8shelper.MatchNodeSelectorTerms(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node.Name, Labels: node.Labels},
	}, selector)
}

// mismatchedNodeSelectorKeys returns the sorted keys of the pod nodeSelector not satisfied by the
// node labels.
func mismatchedNodeSelectorKeys(node *NodeView, podNodeSelector map[string]string) []string {
	var mismatched []string
	for key, value := range podNodeSelector {
		if actual, ok := node.Labels[key]; !ok || actual != value {
			mismatched = append(mismatched, key)
		}
	}
	sort.Strings(mismatched)
	return mismatched
}

// toleratesTaint checks whether at least one of the tolerations tolerates the given taint,
// through the same helper used by the Kubernetes scheduler (comparison operators disabled,
// as per the default scheduler configuration).
func toleratesTaint(tolerations []corev1.Toleration, taint *corev1.Taint) bool {
	return k8shelper.TolerationsTolerateTaint(klog.Background(), tolerations, taint, false)
}

// taintString renders a taint in the key[=value]:effect form.
func taintString(taint *corev1.Taint) string {
	if taint.Value != "" {
		return fmt.Sprintf("%s=%s:%s", taint.Key, taint.Value, taint.Effect)
	}
	return fmt.Sprintf("%s:%s", taint.Key, taint.Effect)
}
