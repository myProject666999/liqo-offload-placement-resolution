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

package pod

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	"github.com/liqotech/liqo/pkg/webhooks/pod/testutils"
)

var _ = Describe("NamespaceOffloading input immutability", func() {

	// The NamespaceOffloading object retrieved by the webhook may be backed by the informer cache:
	// mutating it in place would poison the cached object for every other consumer.
	DescribeTable("createNodeSelectorFromNamespaceOffloading does not mutate the input object",
		func(strategy offloadingv1beta1.PodOffloadingStrategyType) {
			nsoff := testutils.GetNamespaceOffloading(strategy)
			frozen := nsoff.DeepCopy()

			_, err := createNodeSelectorFromNamespaceOffloading(&nsoff)
			Expect(err).ToNot(HaveOccurred())

			By("checking the ClusterSelector terms are untouched")
			Expect(nsoff.Spec.ClusterSelector).To(Equal(frozen.Spec.ClusterSelector))
			Expect(nsoff.Spec).To(Equal(frozen.Spec))
		},
		Entry("RemotePodOffloadingStrategyType", offloadingv1beta1.RemotePodOffloadingStrategyType),
		Entry("LocalAndRemotePodOffloadingStrategyType", offloadingv1beta1.LocalAndRemotePodOffloadingStrategyType),
		Entry("LocalPodOffloadingStrategyType", offloadingv1beta1.LocalPodOffloadingStrategyType),
	)

	It("does not share memory between the returned selector and the input ClusterSelector", func() {
		nsoff := testutils.GetNamespaceOffloading(offloadingv1beta1.RemotePodOffloadingStrategyType)

		out, err := createNodeSelectorFromNamespaceOffloading(&nsoff)
		Expect(err).ToNot(HaveOccurred())
		Expect(out).ToNot(BeNil())

		By("mutating the output and checking the input is unaffected")
		out.NodeSelectorTerms[0].MatchExpressions[0].Values[0] = "tampered"
		Expect(nsoff.Spec.ClusterSelector.NodeSelectorTerms[0].MatchExpressions[0].Values[0]).ToNot(Equal("tampered"))
	})

	It("mutatePod does not mutate the NamespaceOffloading object", func() {
		nsoff := testutils.GetNamespaceOffloading(offloadingv1beta1.RemotePodOffloadingStrategyType)
		frozen := nsoff.DeepCopy()
		podNodeSelector := testutils.GetPodNodeSelector()
		p := corev1.Pod{
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: podNodeSelector.DeepCopy(),
					},
				},
			},
		}

		Expect(mutatePod(&nsoff, &p, "liqo")).To(Succeed())
		Expect(nsoff.Spec).To(Equal(frozen.Spec))
	})
})
