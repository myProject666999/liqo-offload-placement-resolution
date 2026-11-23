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

package offload

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/cli-runtime/pkg/printers"
	"sigs.k8s.io/controller-runtime/pkg/client"

	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
	"github.com/liqotech/liqo/pkg/liqoctl/factory"
	"github.com/liqotech/liqo/pkg/liqoctl/output"
	"github.com/liqotech/liqo/pkg/liqoctl/wait"
	"github.com/liqotech/liqo/pkg/utils/resource"
)

// Options encapsulates the arguments of the offload namespace command.
type Options struct {
	*factory.Factory

	Namespaces               []string
	LabelSelector            string
	OverrideExisting         bool
	PodOffloadingStrategy    offloadingv1beta1.PodOffloadingStrategyType
	NamespaceMappingStrategy offloadingv1beta1.NamespaceMappingStrategyType
	RemoteNamespaceName      string
	ClusterSelector          [][]metav1.LabelSelectorRequirement

	OutputFormat string

	Timeout time.Duration
}

// ParseClusterSelectors parses the cluster selector.
func (o *Options) ParseClusterSelectors(selectors []string) error {
	for _, selector := range selectors {
		s, err := metav1.ParseToLabelSelector(selector)
		if err != nil {
			return err
		}

		// Convert MatchLabels into MatchExpressions
		for key, value := range s.MatchLabels {
			req := metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpIn, Values: []string{value}}
			s.MatchExpressions = append(s.MatchExpressions, req)
		}

		o.ClusterSelector = append(o.ClusterSelector, s.MatchExpressions)
	}

	return nil
}

// Run implements the offload namespace command.
func (o *Options) Run(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	offloadNamespaces, skippedNamespaces, err := o.selectNamespaces(ctx)
	if err != nil {
		return err
	}

	if len(skippedNamespaces) > 0 {
		o.Printer.Warning.Printfln("Skipping namespaces already offloaded (use --override-existing to update them): %s",
			strings.Join(skippedNamespaces, ", "))
	}

	if len(offloadNamespaces) == 0 {
		return fmt.Errorf("no namespaces can be offloaded")
	}

	// Offload each namespace
	var errors []error
	for _, ns := range offloadNamespaces {
		if err := o.runOffload(ctx, ns); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("encountered %d errors during offloading, error details: %v", len(errors), errors)
	}

	return nil
}

// selectNamespaces resolves the namespaces to offload: the explicitly named ones, plus the ones
// matched by the label selector (supporting '=', '==' and '!=' only), when specified.
//
// The selector is evaluated once, when the command runs: namespaces labeled afterwards are not
// automatically offloaded. Namespaces matched through the selector that are already offloaded
// (i.e., a NamespaceOffloading resource already exists) are skipped and returned in the skipped
// list, unless OverrideExisting is set; explicitly named namespaces are always processed, since
// naming them is already an explicit authorization to update them.
//
// An error is returned when no namespace is selected at all (e.g., the selector matches nothing).
func (o *Options) selectNamespaces(ctx context.Context) (selected, skipped []string, err error) {
	selected = append(selected, o.Namespaces...)

	if o.LabelSelector == "" {
		if len(selected) == 0 {
			return nil, nil, fmt.Errorf("no namespaces can be offloaded: no namespace name nor selector specified")
		}
		return selected, nil, nil
	}

	// Parse the label selector
	selector, err := labels.Parse(o.LabelSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid label selector: %w", err)
	}

	// List namespaces
	var selectedNsList corev1.NamespaceList
	if err := o.CRClient.List(ctx, &selectedNsList, &client.ListOptions{LabelSelector: selector}); err != nil {
		return nil, nil, fmt.Errorf("cannot list namespace objects: %w", err)
	}

	if len(selectedNsList.Items) == 0 && len(selected) == 0 {
		return nil, nil, fmt.Errorf("no namespaces can be offloaded: the selector %q matched no namespaces", o.LabelSelector)
	}

	for i := range selectedNsList.Items {
		ns := &selectedNsList.Items[i]
		if slices.Contains(selected, ns.Name) || slices.Contains(skipped, ns.Name) {
			continue
		}

		if !o.OverrideExisting {
			alreadyOffloaded, err := o.isAlreadyOffloaded(ctx, ns.Name)
			if err != nil {
				return nil, nil, err
			}
			if alreadyOffloaded {
				skipped = append(skipped, ns.Name)
				continue
			}
		}

		selected = append(selected, ns.Name)
	}

	return selected, skipped, nil
}

// isAlreadyOffloaded returns whether the given namespace already hosts a NamespaceOffloading resource.
func (o *Options) isAlreadyOffloaded(ctx context.Context, namespace string) (bool, error) {
	nsoff := &offloadingv1beta1.NamespaceOffloading{}
	err := o.CRClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: consts.DefaultNamespaceOffloadingName}, nsoff)
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err):
		return false, nil
	default:
		return false, fmt.Errorf("cannot check the offloading status of namespace %q: %w", namespace, err)
	}
}

// runOffload encapsulates the core logic of the offload namespace command.
func (o *Options) runOffload(ctx context.Context, namespace string) error {
	// Output the NamespaceOffloading resource, instead of applying it.
	if o.OutputFormat != "" {
		o.Printer.CheckErr(o.output(namespace))
		return nil
	}

	o.Namespace = namespace

	s := o.Printer.StartSpinner(fmt.Sprintf("Enabling namespace offloading for %q", o.Namespace))

	nsoff := &offloadingv1beta1.NamespaceOffloading{ObjectMeta: metav1.ObjectMeta{
		Name: consts.DefaultNamespaceOffloadingName, Namespace: o.Namespace}}

	var oldStrategy offloadingv1beta1.PodOffloadingStrategyType
	_, err := resource.CreateOrUpdate(ctx, o.CRClient, nsoff, func() error {
		oldStrategy = nsoff.Spec.PodOffloadingStrategy
		nsoff.Spec.PodOffloadingStrategy = o.PodOffloadingStrategy
		nsoff.Spec.NamespaceMappingStrategy = o.NamespaceMappingStrategy
		nsoff.Spec.RemoteNamespaceName = o.RemoteNamespaceName
		nsoff.Spec.ClusterSelector = toNodeSelector(o.ClusterSelector)
		return nil
	})
	if err != nil {
		s.Fail(fmt.Sprintf("Failed enabling namespace offloading: %v", output.PrettyErr(err)))
		return err
	}
	s.Success(fmt.Sprintf("Offloading of namespace %q correctly enabled", o.Namespace))

	switch {
	case oldStrategy == "", // The NamespaceOffloading has just been created
		o.PodOffloadingStrategy == oldStrategy,                                               // The pod offloading strategy has not been changed
		o.PodOffloadingStrategy == offloadingv1beta1.LocalAndRemotePodOffloadingStrategyType: // The new pod offloading strategy is less restrictive
		break
	default:
		o.Printer.Warning.Println("The PodOffloadingStrategy was mutated to a more restrictive setting")
		o.Printer.Warning.Println("Existing pods violating this policy might still be running")
	}

	waiter := wait.NewWaiterFromFactory(o.Factory)
	if err := waiter.ForOffloading(ctx, o.Namespace); err != nil {
		return err
	}

	return nil
}

// output implements the logic to output the generated NamespaceOffloading resource.
func (o *Options) output(namespace string) error {
	nsoff := offloadingv1beta1.NamespaceOffloading{
		TypeMeta:   metav1.TypeMeta{APIVersion: offloadingv1beta1.SchemeGroupVersion.String(), Kind: "NamespaceOffloading"},
		ObjectMeta: metav1.ObjectMeta{Name: consts.DefaultNamespaceOffloadingName, Namespace: namespace},
		Spec: offloadingv1beta1.NamespaceOffloadingSpec{
			PodOffloadingStrategy:    o.PodOffloadingStrategy,
			NamespaceMappingStrategy: o.NamespaceMappingStrategy,
			RemoteNamespaceName:      o.RemoteNamespaceName,
			ClusterSelector:          toNodeSelector(o.ClusterSelector),
		},
	}

	switch o.OutputFormat {
	case "yaml":
		var buf bytes.Buffer
		printer := &printers.YAMLPrinter{}
		if err := printer.PrintObj(&nsoff, &buf); err != nil {
			return err
		}
		if _, err := buf.WriteString("---\n"); err != nil {
			return err
		}
		_, err := buf.WriteTo(os.Stdout)
		return err
	case "json":
		printer := &printers.JSONPrinter{}
		return printer.PrintObj(&nsoff, os.Stdout)
	default:
		return fmt.Errorf("unsupported output format %q", o.OutputFormat)
	}
}

func toNodeSelector(selectors [][]metav1.LabelSelectorRequirement) corev1.NodeSelector {
	terms := []corev1.NodeSelectorTerm{}

	for _, selector := range selectors {
		var requirements []corev1.NodeSelectorRequirement

		for _, r := range selector {
			requirements = append(requirements, corev1.NodeSelectorRequirement{
				Key:      r.Key,
				Operator: corev1.NodeSelectorOperator(r.Operator),
				Values:   r.Values,
			})
		}

		terms = append(terms, corev1.NodeSelectorTerm{MatchExpressions: requirements})
	}

	return corev1.NodeSelector{NodeSelectorTerms: terms}
}
