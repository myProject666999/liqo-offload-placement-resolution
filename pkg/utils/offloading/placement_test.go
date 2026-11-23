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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	liqoconst "github.com/liqotech/liqo/pkg/consts"
	nsoffctrl "github.com/liqotech/liqo/pkg/liqo-controller-manager/offloading/namespaceoffloading-controller"
)

// --- builders ---------------------------------------------------------------

func nsOff(strategy offloadingv1beta1.PodOffloadingStrategyType, terms ...corev1.NodeSelectorTerm) *offloadingv1beta1.NamespaceOffloading {
	return &offloadingv1beta1.NamespaceOffloading{
		ObjectMeta: metav1.ObjectMeta{Name: "offloading", Namespace: "test"},
		Spec: offloadingv1beta1.NamespaceOffloadingSpec{
			PodOffloadingStrategy: strategy,
			ClusterSelector:       corev1.NodeSelector{NodeSelectorTerms: terms},
		},
	}
}

func term(reqs ...corev1.NodeSelectorRequirement) corev1.NodeSelectorTerm {
	return corev1.NodeSelectorTerm{MatchExpressions: reqs}
}

func fieldTerm(reqs ...corev1.NodeSelectorRequirement) corev1.NodeSelectorTerm {
	return corev1.NodeSelectorTerm{MatchFields: reqs}
}

func in(key string, values ...string) corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpIn, Values: values}
}

func notIn(key string, values ...string) corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpNotIn, Values: values}
}

func exists(key string) corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpExists}
}

func localNode(name string, labels map[string]string, taints ...corev1.Taint) NodeView {
	return NodeView{Name: name, Virtual: false, Labels: labels, Taints: taints}
}

func virtualNode(name string, labels map[string]string) NodeView {
	merged := map[string]string{liqoconst.TypeLabel: liqoconst.TypeNode}
	for k, v := range labels {
		merged[k] = v
	}
	return NodeView{Name: name, Virtual: true, Labels: merged, Taints: []corev1.Taint{VirtualNodeTaint()}}
}

func baseInput(nsoff *offloadingv1beta1.NamespaceOffloading, nodes ...NodeView) *Input {
	return &Input{
		NamespaceOffloading:  nsoff,
		Nodes:                nodes,
		LiqoRuntimeClassName: "liqo",
	}
}

// reasonSources flattens the sources of the reasons of an exclusion, for assertions.
func reasonSources(ex NodeExclusion) []ReasonSource {
	out := make([]ReasonSource, 0, len(ex.Reasons))
	for _, r := range ex.Reasons {
		out = append(out, r.Source)
	}
	return out
}

func exclusionFor(res *Result, node string) *NodeExclusion {
	for i := range res.Exclusions {
		if res.Exclusions[i].Node == node {
			return &res.Exclusions[i]
		}
	}
	return nil
}

func hasCandidate(res *Result, node string) bool {
	for _, c := range res.Candidates {
		if c == node {
			return true
		}
	}
	return false
}

// --- strategy x clusterSelector matrix --------------------------------------

func TestResolvePlacementStrategies(t *testing.T) {
	clusterTerms := []corev1.NodeSelectorTerm{term(in("region", "eu"))}
	local := localNode("local-1", map[string]string{"region": "eu"})
	vnMatch := virtualNode("vn-match", map[string]string{"region": "eu"})
	vnNoMatch := virtualNode("vn-nomatch", map[string]string{"region": "us"})

	tests := []struct {
		name            string
		nsoff           *offloadingv1beta1.NamespaceOffloading
		nodes           []NodeView
		wantCandidates  []string
		wantExclSources map[string][]ReasonSource
		wantToleration  bool
		wantImposed     bool
	}{
		{
			name:           "Local with empty clusterSelector keeps the pod conditions",
			nsoff:          nsOff(offloadingv1beta1.LocalPodOffloadingStrategyType),
			nodes:          []NodeView{local, vnMatch},
			wantCandidates: []string{"local-1"},
			// The virtual node is excluded by its taint, since no toleration is added.
			wantExclSources: map[string][]ReasonSource{"vn-match": {ReasonSourceTaint}},
		},
		{
			name:            "Local ignores the clusterSelector for pod placement",
			nsoff:           nsOff(offloadingv1beta1.LocalPodOffloadingStrategyType, clusterTerms...),
			nodes:           []NodeView{local, vnMatch},
			wantCandidates:  []string{"local-1"},
			wantExclSources: map[string][]ReasonSource{"vn-match": {ReasonSourceTaint}},
			wantImposed:     false,
		},
		{
			name:            "Remote with empty clusterSelector excludes local nodes only",
			nsoff:           nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType),
			nodes:           []NodeView{local, vnMatch, vnNoMatch},
			wantCandidates:  []string{"vn-match", "vn-nomatch"},
			wantExclSources: map[string][]ReasonSource{"local-1": {ReasonSourceStrategy}},
			wantToleration:  true,
			wantImposed:     true,
		},
		{
			name:           "Remote with clusterSelector excludes local nodes and non matching virtual nodes",
			nsoff:          nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType, clusterTerms...),
			nodes:          []NodeView{local, vnMatch, vnNoMatch},
			wantCandidates: []string{"vn-match"},
			wantExclSources: map[string][]ReasonSource{
				// The local node matches the clusterSelector (region=eu), hence the only
				// exclusion reason is the strategy.
				"local-1":    {ReasonSourceStrategy},
				"vn-nomatch": {ReasonSourceClusterSelector},
			},
			wantToleration: true,
			wantImposed:    true,
		},
		{
			name:           "LocalAndRemote with empty clusterSelector allows every node",
			nsoff:          nsOff(offloadingv1beta1.LocalAndRemotePodOffloadingStrategyType),
			nodes:          []NodeView{local, vnMatch},
			wantCandidates: []string{"local-1", "vn-match"},
			wantToleration: true,
			wantImposed:    false,
		},
		{
			name:            "LocalAndRemote with clusterSelector provides the local branch",
			nsoff:           nsOff(offloadingv1beta1.LocalAndRemotePodOffloadingStrategyType, clusterTerms...),
			nodes:           []NodeView{local, vnMatch, vnNoMatch},
			wantCandidates:  []string{"local-1", "vn-match"},
			wantExclSources: map[string][]ReasonSource{"vn-nomatch": {ReasonSourceClusterSelector}},
			wantToleration:  true,
			wantImposed:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frozen := tt.nsoff.DeepCopy()
			res, err := ResolvePlacement(baseInput(tt.nsoff, tt.nodes...))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			for _, node := range tt.nodes {
				want := false
				for _, c := range tt.wantCandidates {
					if c == node.Name {
						want = true
					}
				}
				if got := hasCandidate(res, node.Name); got != want {
					t.Errorf("node %q: candidate=%v, want %v (result: %+v)", node.Name, got, want, res)
				}
			}
			for node, sources := range tt.wantExclSources {
				ex := exclusionFor(res, node)
				if ex == nil {
					t.Errorf("node %q: expected exclusion, got none", node)
					continue
				}
				if !reflect.DeepEqual(reasonSources(*ex), sources) {
					t.Errorf("node %q: reason sources = %v, want %v", node, reasonSources(*ex), sources)
				}
			}
			if (res.AddedToleration != nil) != tt.wantToleration {
				t.Errorf("added toleration = %v, want %v", res.AddedToleration, tt.wantToleration)
			}
			if (res.ImposedNodeSelector != nil) != tt.wantImposed {
				t.Errorf("imposed selector = %v, want imposed=%v", res.ImposedNodeSelector, tt.wantImposed)
			}
			if res.Bypassed {
				t.Errorf("unexpected bypass")
			}
			if !reflect.DeepEqual(tt.nsoff, frozen) {
				t.Errorf("the input NamespaceOffloading has been mutated:\nbefore: %+v\nafter:  %+v", frozen.Spec, tt.nsoff.Spec)
			}
		})
	}
}

// --- OR between terms, AND inside a term ------------------------------------

func TestResolvePlacementOrBetweenTerms(t *testing.T) {
	// Two cluster terms: {region=eu} OR {region=us}. A node in region us must be selected:
	// ANDing the terms (the wrong semantics) would select nothing.
	nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType,
		term(in("region", "eu")), term(in("region", "us")))
	vnUS := virtualNode("vn-us", map[string]string{"region": "us"})

	res, err := ResolvePlacement(baseInput(nsoff, vnUS))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasCandidate(res, "vn-us") {
		t.Fatalf("terms must be ORed: vn-us should be a candidate, got exclusions %+v", res.Exclusions)
	}
}

func TestResolvePlacementAndInsideTerm(t *testing.T) {
	// One cluster term with two requirements: region=eu AND provider exists. A node matching
	// only the first requirement must be excluded: ORing the requirements (the wrong semantics)
	// would select it.
	nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType,
		term(in("region", "eu"), exists("provider")))
	vnPartial := virtualNode("vn-partial", map[string]string{"region": "eu"})

	res, err := ResolvePlacement(baseInput(nsoff, vnPartial))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasCandidate(res, "vn-partial") {
		t.Fatalf("requirements inside a term must be ANDed: vn-partial should be excluded")
	}
	ex := exclusionFor(res, "vn-partial")
	if ex == nil || !reflect.DeepEqual(reasonSources(*ex), []ReasonSource{ReasonSourceClusterSelector}) {
		t.Fatalf("unexpected reasons: %+v", ex)
	}
}

// --- pod own conditions ------------------------------------------------------

func TestResolvePlacementPodConditionsStack(t *testing.T) {
	nsoff := nsOff(offloadingv1beta1.LocalAndRemotePodOffloadingStrategyType, term(in("region", "eu")))
	input := baseInput(nsoff,
		localNode("local-nodisk", map[string]string{"region": "eu"}),
		localNode("local-nozone", map[string]string{"region": "eu", "disk": "ssd"}),
		localNode("local-ok", map[string]string{"region": "eu", "disk": "ssd", "zone": "z1"}),
		virtualNode("vn-ok", map[string]string{"region": "eu", "disk": "ssd", "zone": "z1"}),
	)
	// spec.nodeSelector and the required node affinity stack: both must be satisfied.
	input.PodNodeSelector = map[string]string{"disk": "ssd"}
	input.RequiredNodeAffinity = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{term(in("zone", "z1"))}}

	res, err := ResolvePlacement(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, node := range []string{"local-ok", "vn-ok"} {
		if !hasCandidate(res, node) {
			t.Errorf("node %q should be a candidate, exclusions: %+v", node, res.Exclusions)
		}
	}
	if ex := exclusionFor(res, "local-nodisk"); ex == nil ||
		!reflect.DeepEqual(reasonSources(*ex), []ReasonSource{ReasonSourcePodNodeSelector, ReasonSourcePodAffinity}) {
		t.Errorf("local-nodisk: unexpected reasons %+v", ex)
	}
	if ex := exclusionFor(res, "local-nozone"); ex == nil ||
		!reflect.DeepEqual(reasonSources(*ex), []ReasonSource{ReasonSourcePodAffinity}) {
		t.Errorf("local-nozone: unexpected reasons %+v", ex)
	}
}

func TestResolvePlacementMatchFields(t *testing.T) {
	// The pod required affinity constrains metadata.name through MatchFields: only the node
	// with that exact name can host the pod. Dropping the MatchFields during the merge (the
	// old MergeNodeSelector behavior) would silently discard the constraint.
	nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType)
	input := baseInput(nsoff,
		virtualNode("vn-1", map[string]string{"region": "eu"}),
		virtualNode("vn-2", map[string]string{"region": "eu"}),
	)
	input.RequiredNodeAffinity = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
		fieldTerm(in("metadata.name", "vn-2")),
	}}

	res, err := ResolvePlacement(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasCandidate(res, "vn-1") || !hasCandidate(res, "vn-2") {
		t.Fatalf("MatchFields not honored: candidates=%v, exclusions=%+v", res.Candidates, res.Exclusions)
	}
	if ex := exclusionFor(res, "vn-1"); ex == nil ||
		!reflect.DeepEqual(reasonSources(*ex), []ReasonSource{ReasonSourcePodAffinity}) {
		t.Fatalf("vn-1: unexpected reasons %+v", ex)
	}
	// The effective selector must retain the MatchFields in the merged terms.
	if res.EffectiveNodeSelector == nil ||
		len(res.EffectiveNodeSelector.NodeSelectorTerms[0].MatchFields) == 0 {
		t.Fatalf("the effective selector lost the MatchFields: %+v", res.EffectiveNodeSelector)
	}
}

// --- merged selector size limit ----------------------------------------------

func terms(n int) []corev1.NodeSelectorTerm {
	out := make([]corev1.NodeSelectorTerm, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, term(in("key", "value")))
	}
	return out
}

func TestResolvePlacementMergedSelectorLimit(t *testing.T) {
	// 5 pod terms x 13 cluster terms = 65 > 64 (the default limit): the resolution must be
	// rejected, not truncated.
	nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType, terms(13)...)
	input := baseInput(nsoff, virtualNode("vn", nil))
	input.RequiredNodeAffinity = &corev1.NodeSelector{NodeSelectorTerms: terms(5)}

	if _, err := ResolvePlacement(input); err == nil ||
		!strings.Contains(err.Error(), "65") || !strings.Contains(err.Error(), "64") {
		t.Fatalf("expected a rejection mentioning 65 terms over the limit of 64, got: %v", err)
	}

	// The limit is configurable: raising it makes the same input resolve.
	input.MaxMergedSelectorTerms = 128
	res, err := ResolvePlacement(input)
	if err != nil {
		t.Fatalf("unexpected error with a raised limit: %v", err)
	}
	if got := len(res.EffectiveNodeSelector.NodeSelectorTerms); got != 65 {
		t.Fatalf("expected 65 merged terms, got %d", got)
	}

	// And lowering it rejects smaller products as well.
	input.MaxMergedSelectorTerms = 12
	if _, err := ResolvePlacement(input); err == nil {
		t.Fatalf("expected a rejection with a limit of 12")
	}
}

// --- runtime class bypass -----------------------------------------------------

func TestResolvePlacementRuntimeClassBypass(t *testing.T) {
	nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType, term(in("region", "eu")))
	liqoRuntime := "liqo"
	otherRuntime := "my-custom-runtime"

	t.Run("liqo runtime class bypasses the mutation", func(t *testing.T) {
		in := baseInput(nsoff,
			localNode("local", map[string]string{"region": "eu"}),
			virtualNode("vn", map[string]string{"region": "eu"}),
		)
		in.RuntimeClassName = &liqoRuntime

		res, err := ResolvePlacement(in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Bypassed || res.BypassReason == "" {
			t.Fatalf("expected a visible bypass, got %+v", res)
		}
		if res.AddedToleration != nil || res.ImposedNodeSelector != nil {
			t.Fatalf("no toleration/selector must be imposed when bypassed: %+v", res)
		}
		// The pod keeps its own conditions: the local node is fine, the virtual node is
		// excluded by its taint, since the virtual-node toleration is not added.
		if !hasCandidate(res, "local") {
			t.Fatalf("local node should be a candidate when bypassed")
		}
		if ex := exclusionFor(res, "vn"); ex == nil ||
			!reflect.DeepEqual(reasonSources(*ex), []ReasonSource{ReasonSourceTaint}) {
			t.Fatalf("vn: unexpected reasons %+v", ex)
		}
	})

	t.Run("a different runtime class does not bypass", func(t *testing.T) {
		in := baseInput(nsoff,
			localNode("local", map[string]string{"region": "eu"}),
			virtualNode("vn", map[string]string{"region": "eu"}),
		)
		in.RuntimeClassName = &otherRuntime

		res, err := ResolvePlacement(in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Bypassed {
			t.Fatalf("unexpected bypass")
		}
		if !hasCandidate(res, "vn") || hasCandidate(res, "local") {
			t.Fatalf("unexpected outcome: candidates=%v", res.Candidates)
		}
	})
}

// --- all candidates excluded ---------------------------------------------------

func TestResolvePlacementAllExcluded(t *testing.T) {
	// Every candidate node is excluded, each with its own specific reason: the outcome is
	// not a generic "no available nodes".
	nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType, term(in("region", "eu")))
	in := baseInput(nsoff,
		localNode("local", map[string]string{"region": "eu"}),
		virtualNode("vn-wrong-region", map[string]string{"region": "us"}),
		virtualNode("vn-wrong-disk", map[string]string{"region": "eu", "disk": "hdd"}),
		localNode("local-tainted", map[string]string{"region": "eu", "disk": "ssd"},
			corev1.Taint{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}),
	)
	in.PodNodeSelector = map[string]string{"disk": "ssd"}

	res, err := ResolvePlacement(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Candidates) != 0 {
		t.Fatalf("expected no candidates, got %v", res.Candidates)
	}
	if len(res.Exclusions) != 4 {
		t.Fatalf("expected 4 exclusions, got %+v", res.Exclusions)
	}

	want := map[string][]ReasonSource{
		"local":           {ReasonSourceStrategy, ReasonSourcePodNodeSelector},
		"vn-wrong-region": {ReasonSourceClusterSelector, ReasonSourcePodNodeSelector},
		"vn-wrong-disk":   {ReasonSourcePodNodeSelector},
		"local-tainted":   {ReasonSourceStrategy, ReasonSourceTaint},
	}
	// The exclusions must be reported in input order, with reasons grouped by layer.
	nodes := []string{"local", "vn-wrong-region", "vn-wrong-disk", "local-tainted"}
	for i, ex := range res.Exclusions {
		if ex.Node != nodes[i] {
			t.Fatalf("exclusion %d: expected node %q, got %q", i, nodes[i], ex.Node)
		}
		if !reflect.DeepEqual(reasonSources(ex), want[ex.Node]) {
			t.Errorf("node %q: reason sources = %v, want %v", ex.Node, reasonSources(ex), want[ex.Node])
		}
	}
}

// --- empty and contradictory terms --------------------------------------------

func TestResolvePlacementEmptyAndContradictoryTerms(t *testing.T) {
	t.Run("an empty clusterSelector term matches no object", func(t *testing.T) {
		// Real Kubernetes logic: an empty term selects nothing. Under Remote, the strategy
		// requirement is appended to the term, which then matches all the virtual nodes.
		nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType, corev1.NodeSelectorTerm{})
		res, err := ResolvePlacement(baseInput(nsoff, virtualNode("vn", nil)))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !hasCandidate(res, "vn") {
			t.Fatalf("the type requirement appended to the empty term should match virtual nodes")
		}
	})

	t.Run("an empty pod affinity term matches no object", func(t *testing.T) {
		nsoff := nsOff(offloadingv1beta1.LocalPodOffloadingStrategyType)
		in := baseInput(nsoff, localNode("local", nil))
		in.RequiredNodeAffinity = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
			corev1.NodeSelectorTerm{},
		}}
		res, err := ResolvePlacement(in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hasCandidate(res, "local") {
			t.Fatalf("an empty affinity term must match no object")
		}
		if ex := exclusionFor(res, "local"); ex == nil ||
			!reflect.DeepEqual(reasonSources(*ex), []ReasonSource{ReasonSourcePodAffinity}) {
			t.Fatalf("local: unexpected reasons %+v", ex)
		}
	})

	t.Run("a contradictory term matches no object", func(t *testing.T) {
		nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType,
			term(in("region", "eu"), notIn("region", "eu")))
		res, err := ResolvePlacement(baseInput(nsoff, virtualNode("vn", map[string]string{"region": "eu"})))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res.Candidates) != 0 {
			t.Fatalf("a contradictory term must match no object")
		}
		if ex := exclusionFor(res, "vn"); ex == nil ||
			!reflect.DeepEqual(reasonSources(*ex), []ReasonSource{ReasonSourceClusterSelector}) {
			t.Fatalf("vn: unexpected reasons %+v", ex)
		}
	})
}

// --- invalid strategy -----------------------------------------------------------

func TestResolvePlacementInvalidStrategy(t *testing.T) {
	nsoff := nsOff(offloadingv1beta1.PodOffloadingStrategyType("Bogus"))
	if _, err := ResolvePlacement(baseInput(nsoff, localNode("local", nil))); err == nil {
		t.Fatalf("expected an error for an unknown strategy")
	}
	if _, err := ResolvePlacement(&Input{}); err == nil {
		t.Fatalf("expected an error for a nil NamespaceOffloading")
	}
}

// --- virtual node views ---------------------------------------------------------

func TestLoadVirtualNodeViews(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := offloadingv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	vn := offloadingv1beta1.VirtualNode{
		ObjectMeta: metav1.ObjectMeta{Name: "vn-1"},
		Spec: offloadingv1beta1.VirtualNodeSpec{
			ClusterID: "cluster-1",
			Labels:    map[string]string{"region": "eu"},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&vn).Build()

	views, err := LoadVirtualNodeViews(context.Background(), cl, []offloadingv1beta1.VirtualNode{vn})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(views))
	}
	view := views[0]
	if !view.Virtual || view.Name != "vn-1" {
		t.Fatalf("unexpected view: %+v", view)
	}
	if view.Labels["region"] != "eu" || view.Labels[liqoconst.RemoteClusterID] != "cluster-1" {
		t.Fatalf("unexpected labels: %v", view.Labels)
	}
	if len(view.Taints) != 1 || view.Taints[0].Key != liqoconst.VirtualNodeTolerationKey {
		t.Fatalf("unexpected taints: %v", view.Taints)
	}

	// Consistency check: the resolution of the ClusterSelector layer must agree with
	// MatchVirtualNodeSelectorTerms, reused by the NamespaceOffloading controller.
	nsoff := nsOff(offloadingv1beta1.RemotePodOffloadingStrategyType, term(in("region", "eu")))
	match, err := nsoffctrl.MatchVirtualNodeSelectorTerms(context.Background(), cl, &vn, &nsoff.Spec.ClusterSelector)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res, err := ResolvePlacement(&Input{
		NamespaceOffloading: nsoff,
		Nodes:               views,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := hasCandidate(res, "vn-1"); got != match {
		t.Fatalf("inconsistent outcome: resolver=%v, MatchVirtualNodeSelectorTerms=%v", got, match)
	}
}
