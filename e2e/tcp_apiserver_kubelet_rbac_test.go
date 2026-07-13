// Copyright 2022 Clastix Labs
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	pointer "k8s.io/utils/ptr"

	kamajiv1alpha1 "github.com/clastix/kamaji/api/v1alpha1"
)

var _ = Describe("apiserver->kubelet RBAC binding self-heal", func() {
	// The tenant client below talks to the tenant API server from the test host,
	// so the control plane must be exposed on a routable endpoint. Use NodePort on
	// the KinD node IP (like the worker-join suite) rather than a ClusterIP: a
	// ClusterIP is not reachable from the host, and its admin kubeconfig would
	// point at an address served by a different API server (TLS would not verify).
	var tcp *kamajiv1alpha1.TenantControlPlane

	JustBeforeEach(func() {
		tcp = &kamajiv1alpha1.TenantControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tcp-apiserver-kubelet-rbac",
				Namespace: "default",
			},
			Spec: kamajiv1alpha1.TenantControlPlaneSpec{
				ControlPlane: kamajiv1alpha1.ControlPlane{
					Deployment: kamajiv1alpha1.DeploymentSpec{
						Replicas: pointer.To(int32(1)),
					},
					Service: kamajiv1alpha1.ServiceSpec{
						ServiceType: "NodePort",
					},
				},
				NetworkProfile: kamajiv1alpha1.NetworkProfileSpec{
					Address: GetKindIPAddress(),
					Port:    31444,
				},
				Kubernetes: kamajiv1alpha1.KubernetesSpec{
					Version: "v1.35.0",
					Kubelet: kamajiv1alpha1.KubeletSpec{
						CGroupFS: "cgroupfs",
					},
					AdmissionControllers: kamajiv1alpha1.AdmissionControllers{
						"LimitRanger",
						"ResourceQuota",
					},
				},
				Addons: kamajiv1alpha1.AddonsSpec{},
			},
		}

		Expect(k8sClient.Create(context.Background(), tcp)).NotTo(HaveOccurred())
	})

	JustAfterEach(func() {
		Expect(k8sClient.Delete(context.Background(), tcp)).Should(Succeed())
	})

	// Regression guard: the binding is reconciled *before* the
	// cluster-info checksum gate in KubeadmBootstrap, so an already-converged
	// tenant (whose gate short-circuits) still self-heals it. This mirrors the
	// manual dev verification: delete the binding, watch Kamaji recreate it.
	// If a refactor moved the reconcile below the gate, a converged tenant would
	// never recreate the deleted binding and this test would time out.
	It("recreates the binding on a converged tenant after it is deleted", func() {
		ctx := context.Background()

		By("waiting for the Tenant Control Plane to be Ready (converged)", func() {
			StatusMustEqualTo(tcp, kamajiv1alpha1.VersionReady)
		})

		var tenant kubernetes.Interface

		By("building a client for the tenant", func() {
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: tcp.GetNamespace(), Name: tcp.Status.KubeConfig.Admin.SecretName}, secret)).NotTo(HaveOccurred())

			config, err := clientcmd.RESTConfigFromKubeConfig(secret.Data["admin.conf"])
			Expect(err).ToNot(HaveOccurred())

			tenant, err = kubernetes.NewForConfig(config)
			Expect(err).ToNot(HaveOccurred())
		})

		By("waiting for the binding to be present after the initial bootstrap", func() {
			Eventually(func() error {
				_, err := tenant.RbacV1().ClusterRoleBindings().Get(ctx, kubeadmconstants.KubeletAPIAdminClusterRoleBindingName, metav1.GetOptions{})

				return err
			}, time.Minute, time.Second).Should(Succeed())
		})

		By("deleting the binding on the tenant", func() {
			Expect(tenant.RbacV1().ClusterRoleBindings().Delete(ctx, kubeadmconstants.KubeletAPIAdminClusterRoleBindingName, metav1.DeleteOptions{})).To(Succeed())
		})

		// The TenantControlPlane is requeued periodically; the pre-gate reconcile
		// runs on every pass, so the deleted binding is recreated without any
		// change to the tenant's cluster-info (which would open the checksum gate).
		By("verifying Kamaji recreates it via the pre-gate reconcile", func() {
			Eventually(func() error {
				_, err := tenant.RbacV1().ClusterRoleBindings().Get(ctx, kubeadmconstants.KubeletAPIAdminClusterRoleBindingName, metav1.GetOptions{})

				return err
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})
	})
})
