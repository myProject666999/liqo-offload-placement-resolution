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
	"context"
	"io"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
	"github.com/liqotech/liqo/pkg/liqoctl/factory"
	"github.com/liqotech/liqo/pkg/liqoctl/output"
)

func namespace(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func namespaceOffloading(namespace string) *offloadingv1beta1.NamespaceOffloading {
	return &offloadingv1beta1.NamespaceOffloading{
		ObjectMeta: metav1.ObjectMeta{Name: consts.DefaultNamespaceOffloadingName, Namespace: namespace},
	}
}

func setupOptions(t *testing.T, objs ...client.Object) *Options {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := offloadingv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return &Options{Factory: &factory.Factory{
		CRClient: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		Printer:  output.NewFakePrinter(io.Discard),
	}}
}

func TestSelectNamespaces(t *testing.T) {
	ctx := context.Background()

	t.Run("explicit namespaces are always selected", func(t *testing.T) {
		o := setupOptions(t, namespace("foo", nil), namespaceOffloading("foo"))
		o.Namespaces = []string{"foo"}

		selected, skipped, err := o.selectNamespaces(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Naming a namespace explicitly is an explicit authorization to update it.
		if len(selected) != 1 || selected[0] != "foo" || len(skipped) != 0 {
			t.Fatalf("selected=%v skipped=%v", selected, skipped)
		}
	})

	t.Run("a selector matching no namespaces returns an error", func(t *testing.T) {
		o := setupOptions(t, namespace("foo", map[string]string{"team": "a"}))
		o.LabelSelector = "team=b"

		if _, _, err := o.selectNamespaces(ctx); err == nil {
			t.Fatalf("expected an error when the selector matches no namespaces")
		}
	})

	t.Run("no namespaces at all returns an error", func(t *testing.T) {
		o := setupOptions(t)
		if _, _, err := o.selectNamespaces(ctx); err == nil {
			t.Fatalf("expected an error when no namespace is specified")
		}
	})

	t.Run("already offloaded namespaces are skipped and listed", func(t *testing.T) {
		o := setupOptions(t,
			namespace("foo", map[string]string{"team": "a"}),
			namespace("bar", map[string]string{"team": "a"}),
			namespaceOffloading("bar"),
		)
		o.LabelSelector = "team=a"

		selected, skipped, err := o.selectNamespaces(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(selected) != 1 || selected[0] != "foo" {
			t.Fatalf("selected=%v, expected [foo]", selected)
		}
		if len(skipped) != 1 || skipped[0] != "bar" {
			t.Fatalf("skipped=%v, expected [bar]", skipped)
		}
	})

	t.Run("override-existing updates already offloaded namespaces", func(t *testing.T) {
		o := setupOptions(t,
			namespace("foo", map[string]string{"team": "a"}),
			namespace("bar", map[string]string{"team": "a"}),
			namespaceOffloading("bar"),
		)
		o.LabelSelector = "team=a"
		o.OverrideExisting = true

		selected, skipped, err := o.selectNamespaces(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(selected) != 2 || len(skipped) != 0 {
			t.Fatalf("selected=%v skipped=%v", selected, skipped)
		}
	})

	t.Run("inequality selectors keep working", func(t *testing.T) {
		o := setupOptions(t,
			namespace("foo", map[string]string{"team": "a"}),
			namespace("bar", map[string]string{"team": "b"}),
		)
		o.LabelSelector = "team!=b"

		selected, _, err := o.selectNamespaces(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(selected) != 1 || selected[0] != "foo" {
			t.Fatalf("selected=%v, expected [foo]", selected)
		}
	})

	t.Run("an invalid selector returns an error", func(t *testing.T) {
		o := setupOptions(t)
		o.LabelSelector = "==invalid=="
		if _, _, err := o.selectNamespaces(ctx); err == nil {
			t.Fatalf("expected an error for an invalid selector")
		}
	})
}

func TestRunFailsWhenNothingToOffload(t *testing.T) {
	o := setupOptions(t, namespace("foo", map[string]string{"team": "a"}))
	o.LabelSelector = "team=b"
	o.Timeout = time.Minute

	err := o.Run(context.Background())
	if err == nil {
		t.Fatalf("expected Run to fail when the selector matches no namespaces")
	}
	if got := err.Error(); !strings.Contains(got, "matched no namespaces") {
		t.Fatalf("unexpected error: %v", err)
	}
}
