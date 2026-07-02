// Copyright 2022 Clastix Labs
// SPDX-License-Identifier: Apache-2.0

package resources

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kamajiv1alpha1 "github.com/clastix/kamaji/api/v1alpha1"
	"github.com/clastix/kamaji/internal/kubeadm"
	"github.com/clastix/kamaji/internal/utilities"
)

func GetKubeadmManifestDeps(ctx context.Context, client client.Client, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) (*clientset.Clientset, *kubeadm.Configuration, error) {
	config, err := getStoredKubeadmConfiguration(ctx, client, "", tenantControlPlane)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot retrieve kubeadm configuration: %w", err)
	}

	kubeconfig, err := utilities.GetTenantKubeconfig(ctx, client, tenantControlPlane)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot retrieve kubeconfig configuration: %w", err)
	}

	address, _, err := tenantControlPlane.AssignedControlPlaneAddress()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot retrieve Tenant Control Plane address: %w", err)
	}

	config.Kubeconfig = *kubeconfig
	config.Parameters = kubeadm.Parameters{
		TenantControlPlaneName:         tenantControlPlane.GetName(),
		TenantDNSServiceIPs:            tenantControlPlane.Spec.NetworkProfile.DNSServiceIPs,
		TenantControlPlaneVersion:      tenantControlPlane.Spec.Kubernetes.Version,
		TenantControlPlanePodCIDR:      tenantControlPlane.Spec.NetworkProfile.PodCIDR,
		TenantControlPlaneAddress:      address,
		TenantControlPlaneCertSANs:     tenantControlPlane.Spec.NetworkProfile.CertSANs,
		TenantControlPlanePort:         tenantControlPlane.Spec.NetworkProfile.Port,
		TenantControlPlaneCGroupDriver: tenantControlPlane.Spec.Kubernetes.Kubelet.CGroupFS.String(), //nolint:staticcheck
	}
	// If CoreDNS addon is enabled and with an override, adding these to the kubeadm init configuration
	if coreDNS := tenantControlPlane.Spec.Addons.CoreDNS; coreDNS != nil {
		config.Parameters.CoreDNSOptions = &kubeadm.AddonOptions{}

		if len(coreDNS.ImageRepository) > 0 {
			config.Parameters.CoreDNSOptions.Repository = coreDNS.ImageRepository
		}

		if len(coreDNS.ImageRepository) > 0 {
			config.Parameters.CoreDNSOptions.Tag = coreDNS.ImageTag
		}
	}
	// If the kube-proxy addon is enabled and with overrides, adding it to the kubeadm parameters
	if kubeProxy := tenantControlPlane.Spec.Addons.KubeProxy; kubeProxy != nil {
		config.Parameters.KubeProxyOptions = &kubeadm.AddonOptions{}

		if len(kubeProxy.ImageRepository) > 0 {
			config.Parameters.KubeProxyOptions.Repository = kubeProxy.ImageRepository
		} else {
			config.Parameters.KubeProxyOptions.Repository = "registry.k8s.io"
		}

		if len(kubeProxy.ImageTag) > 0 {
			config.Parameters.KubeProxyOptions.Tag = kubeProxy.ImageTag
		} else {
			config.Parameters.KubeProxyOptions.Tag = tenantControlPlane.Spec.Kubernetes.Version
		}
	}

	tenantClient, err := utilities.GetTenantClientSet(ctx, client, tenantControlPlane)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot generate tenant client: %w", err)
	}

	return tenantClient, config, nil
}

func KubeadmBootstrap(ctx context.Context, r KubeadmPhaseResource, logger logr.Logger, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) (controllerutil.OperationResult, error) {
	var checksum string

	tntClient, err := utilities.GetTenantClient(ctx, r.GetClient(), tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot generate tenant client")

		return controllerutil.OperationResultNone, err
	}

	var clusterInfo corev1.ConfigMap
	if cmErr := tntClient.Get(ctx, types.NamespacedName{Name: bootstrapapi.ConfigMapClusterInfo, Namespace: metav1.NamespacePublic}, &clusterInfo); cmErr != nil {
		if !k8serrors.IsNotFound(cmErr) {
			logger.Error(cmErr, "cannot retrieve cluster-info ConfigMap")
		}
	}

	// Ensure the apiserver->kubelet RBAC binding exists, independent of the
	// cluster-info checksum gate below, so already-provisioned clusters self-heal
	// (the gate would otherwise skip the bootstrap-token phase for them).
	// The kubeadm 1.36 emitter mints the apiserver-kubelet-client cert without the
	// privileged org and authorizes it via this binding instead; without it,
	// apiserver->kubelet calls (logs/exec, nodes/proxy) are Forbidden.
	// Create-if-absent only: the binding content is constant and its RoleRef is
	// immutable, so an Update on drift would wedge the cluster in a 422 loop.
	if err = ensureAPIServerKubeletRBAC(ctx, tntClient); err != nil {
		logger.Error(err, "cannot ensure apiserver->kubelet RBAC binding")

		return controllerutil.OperationResultNone, err
	}

	status, err := r.GetStatus(tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve status")

		return controllerutil.OperationResultNone, err
	}

	if status != nil {
		checksum = utilities.CalculateMapChecksum(clusterInfo.Data)

		if checksum == status.GetChecksum() {
			r.SetKubeadmConfigChecksum(checksum)

			return controllerutil.OperationResultNone, nil
		}
	}

	kubeconfig, err := utilities.GetTenantKubeconfig(ctx, r.GetClient(), tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve kubeconfig configuration")

		return controllerutil.OperationResultNone, err
	}

	config, err := getStoredKubeadmConfiguration(ctx, r.GetClient(), r.GetTmpDirectory(), tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve kubeadm configuration")

		return controllerutil.OperationResultNone, err
	}

	config.Kubeconfig = *kubeconfig

	fun, err := r.GetKubeadmFunction(ctx, tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve kubeadm function")

		return controllerutil.OperationResultNone, err
	}

	client, err := utilities.GetTenantClientSet(ctx, r.GetClient(), tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot generate tenant client")

		return controllerutil.OperationResultNone, err
	}

	if _, err = fun(client, config); err != nil {
		logger.Error(err, "kubeadm function failed")

		return controllerutil.OperationResultNone, err
	}

	if status == nil {
		return controllerutil.OperationResultNone, nil
	}

	r.SetKubeadmConfigChecksum(checksum)

	if checksum == "" {
		return controllerutil.OperationResultCreated, nil
	}

	return controllerutil.OperationResultUpdated, nil
}

// ensureAPIServerKubeletRBAC creates the kubeadm:apiserver-kubelet-client
// ClusterRoleBinding on the tenant if it is absent, granting the apiserver's
// kubelet client access to the kubelet API. It is a no-op when the binding
// already exists: the binding is constant and its RoleRef is immutable, so it
// never issues an update (which would 422 on any drift).
func ensureAPIServerKubeletRBAC(ctx context.Context, tenantClient client.Client) error {
	const bindingName = "kubeadm:apiserver-kubelet-client"

	var binding rbacv1.ClusterRoleBinding
	switch err := tenantClient.Get(ctx, types.NamespacedName{Name: bindingName}, &binding); {
	case err == nil:
		return nil
	case !k8serrors.IsNotFound(err):
		return err
	}

	return client.IgnoreAlreadyExists(tenantClient.Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: bindingName},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "system:kubelet-api-admin",
		},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.UserKind,
			Name: "kube-apiserver-kubelet-client",
		}},
	}))
}

func KubeadmPhaseCreate(ctx context.Context, r KubeadmPhaseResource, logger logr.Logger, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) (controllerutil.OperationResult, error) {
	config, err := getStoredKubeadmConfiguration(ctx, r.GetClient(), r.GetTmpDirectory(), tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve kubeadm configuration")

		return controllerutil.OperationResultNone, err
	}

	kubeconfig, err := utilities.GetTenantKubeconfig(ctx, r.GetClient(), tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve kubeconfig configuration")

		return controllerutil.OperationResultNone, err
	}

	address, _, err := tenantControlPlane.AssignedControlPlaneAddress()
	if err != nil {
		logger.Error(err, "cannot retrieve Tenant Control Plane address")

		return controllerutil.OperationResultNone, err
	}

	config.Kubeconfig = *kubeconfig
	config.Parameters = kubeadm.Parameters{
		TenantControlPlaneName:         tenantControlPlane.GetName(),
		TenantDNSServiceIPs:            tenantControlPlane.Spec.NetworkProfile.DNSServiceIPs,
		TenantControlPlaneVersion:      tenantControlPlane.Spec.Kubernetes.Version,
		TenantControlPlanePodCIDR:      tenantControlPlane.Spec.NetworkProfile.PodCIDR,
		TenantControlPlaneAddress:      address,
		TenantControlPlaneCertSANs:     tenantControlPlane.Spec.NetworkProfile.CertSANs,
		TenantControlPlanePort:         tenantControlPlane.Spec.NetworkProfile.Port,
		TenantControlPlaneCGroupDriver: tenantControlPlane.Spec.Kubernetes.Kubelet.CGroupFS.String(), //nolint:staticcheck
	}

	var checksum string

	status, err := r.GetStatus(tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve status")

		return controllerutil.OperationResultNone, err
	}
	// if the status is nil it means the kubeadm phase is idempotent:
	// we can skip the checksum check to avoid endless reconciliations.
	if status != nil {
		checksum = config.Checksum()

		if checksum == status.GetChecksum() {
			r.SetKubeadmConfigChecksum(checksum)

			return controllerutil.OperationResultNone, nil
		}
	}

	client, err := utilities.GetTenantClientSet(ctx, r.GetClient(), tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot generate tenant client")

		return controllerutil.OperationResultNone, err
	}

	fun, err := r.GetKubeadmFunction(ctx, tenantControlPlane)
	if err != nil {
		logger.Error(err, "cannot retrieve kubeadm function")

		return controllerutil.OperationResultNone, err
	}
	if _, err = fun(client, config); err != nil {
		logger.Error(err, "kubeadm function failed")

		return controllerutil.OperationResultNone, err
	}

	if status == nil {
		return controllerutil.OperationResultNone, nil
	}

	r.SetKubeadmConfigChecksum(checksum)

	if checksum == "" {
		return controllerutil.OperationResultCreated, nil
	}

	return controllerutil.OperationResultUpdated, nil
}
