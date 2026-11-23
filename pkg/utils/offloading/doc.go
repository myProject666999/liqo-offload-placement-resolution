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

// Package offloading provides a pure, offline resolution of the Liqo pod placement constraints.
//
// The placement of a pod in an offloaded namespace is decided by the combination of:
//   - the PodOffloadingStrategy of the NamespaceOffloading resource (Local, Remote, LocalAndRemote);
//   - the NamespaceOffloading ClusterSelector (ORed terms, ANDed requirements inside each term);
//   - the pod own conditions: spec.nodeSelector (ANDed with everything) and the required node
//     affinity (ORed terms, ANDed requirements and fields inside each term);
//   - the taints of the candidate nodes and the tolerations of the pod (including the virtual-node
//     toleration injected by the webhook for the Remote and LocalAndRemote strategies).
//
// ResolvePlacement evaluates those layers against a set of candidate nodes (local and virtual) and
// reports, for every node, either its candidacy or the list of the per-layer exclusion reasons, so
// that operators can understand why a pod cannot be placed somewhere, instead of getting a generic
// "no available nodes" outcome. It does NOT implement resource binpacking nor a full scheduler.
//
// Semantics preserved from the webhook and the Kubernetes scheduler:
//   - terms are ORed, requirements inside a term are ANDed: confusing the two changes the result
//     (e.g., cluster terms {region=A},{region=B} select a node in region B, while ANDing them
//     would select nothing);
//   - the pod required affinity and the selector imposed by the NamespaceOffloading are merged
//     through the cartesian product of their terms (utils.MergeNodeSelector), e.g. 3 pod terms
//     and 4 cluster terms produce 12 merged terms; the product is bounded by a configurable limit
//     (DefaultMaxMergedSelectorTerms) and rejected, never truncated, when exceeded;
//   - MatchFields (e.g., metadata.name), empty terms (matching no objects), contradictory terms
//     (matching no objects) and the stacking of spec.nodeSelector with the node affinity all follow
//     the real Kubernetes logic, evaluated through k8s.io/component-helpers;
//   - pods using the Liqo runtime class are not mutated by the webhook: the bypass is reported
//     explicitly (Result.Bypassed) and the pod is evaluated against its own conditions only.
//
// Compatibility changes introduced together with this package:
//   - utils.MergeNodeSelector now preserves the MatchFields of the merged terms; previously they
//     were silently dropped, so a required node affinity constraining e.g. metadata.name was lost
//     when merged with the NamespaceOffloading selector;
//   - the selector imposed by the NamespaceOffloading is built on a deep copy of the
//     ClusterSelector; previously the webhook appended the liqo.io/type requirement to the
//     (possibly cache-backed) NamespaceOffloading object in place.
package offloading
