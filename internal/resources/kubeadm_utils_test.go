// Copyright 2022 Clastix Labs
// SPDX-License-Identifier: Apache-2.0

package resources

import (
	"context"
	"errors"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const apiServerKubeletBindingName = "kubeadm:apiserver-kubelet-client"

func getAPIServerKubeletBinding(t *testing.T, c client.Client) *rbacv1.ClusterRoleBinding {
	t.Helper()

	binding := &rbacv1.ClusterRoleBinding{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: apiServerKubeletBindingName}, binding); err != nil {
		t.Fatalf("expected binding to exist: %v", err)
	}

	return binding
}

func TestEnsureAPIServerKubeletRBAC(t *testing.T) {
	t.Run("creates the binding when absent", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

		if err := ensureAPIServerKubeletRBAC(context.Background(), c); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		binding := getAPIServerKubeletBinding(t, c)
		if binding.RoleRef.Name != "system:kubelet-api-admin" || binding.RoleRef.Kind != "ClusterRole" {
			t.Fatalf("unexpected RoleRef: %#v", binding.RoleRef)
		}
		if len(binding.Subjects) != 1 ||
			binding.Subjects[0].Kind != rbacv1.UserKind ||
			binding.Subjects[0].Name != "kube-apiserver-kubelet-client" {
			t.Fatalf("unexpected Subjects: %#v", binding.Subjects)
		}
	})

	t.Run("is a no-op when the binding already exists", func(t *testing.T) {
		// Seed a binding whose RoleRef differs from what we would create. Because
		// RoleRef is immutable, any Update attempt would fail; the helper must not
		// issue one. The existing object must be left untouched.
		existing := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: apiServerKubeletBindingName},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     "some-other-role",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(existing).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
					t.Fatal("ensureAPIServerKubeletRBAC must not Update an existing binding")

					return nil
				},
			}).Build()

		if err := ensureAPIServerKubeletRBAC(context.Background(), c); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		binding := getAPIServerKubeletBinding(t, c)
		if binding.RoleRef.Name != "some-other-role" {
			t.Fatalf("existing binding was modified: %#v", binding.RoleRef)
		}
	})

	t.Run("tolerates a create race", func(t *testing.T) {
		// Get reports NotFound but Create loses the race and returns AlreadyExists:
		// the helper must swallow it rather than surface an error.
		c := fake.NewClientBuilder().WithScheme(scheme.Scheme).
			WithInterceptorFuncs(interceptor.Funcs{
				Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
					return k8serrors.NewAlreadyExists(
						schema.GroupResource{Group: rbacv1.GroupName, Resource: "clusterrolebindings"},
						obj.GetName(),
					)
				},
			}).Build()

		if err := ensureAPIServerKubeletRBAC(context.Background(), c); err != nil {
			t.Fatalf("AlreadyExists on create must be tolerated, got: %v", err)
		}
	})

	t.Run("propagates unexpected get errors", func(t *testing.T) {
		boom := errors.New("boom")
		c := fake.NewClientBuilder().WithScheme(scheme.Scheme).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
					return boom
				},
			}).Build()

		if err := ensureAPIServerKubeletRBAC(context.Background(), c); !errors.Is(err, boom) {
			t.Fatalf("expected get error to propagate, got: %v", err)
		}
	})
}
