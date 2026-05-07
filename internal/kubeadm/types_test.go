// Copyright 2022 Clastix Labs
// SPDX-License-Identifier: Apache-2.0

package kubeadm_test

import (
	"testing"

	"github.com/clastix/kamaji/internal/kubeadm"
)

// TestConfigurationChecksum_KubeletPatchesAffectChecksum guards against the
// regression that left worker nodes unable to join after a
// `spec.kubernetes.kubelet.configurationJSONPatches` change: if the kubelet
// patches do not contribute to the kubeadm-phase checksum, the cached
// upload-config-kubelet phase short-circuits and the tenant's kubelet-config
// ConfigMap is never re-uploaded. See clastix/kamaji#XXXX.
func TestConfigurationChecksum_KubeletPatchesAffectChecksum(t *testing.T) {
	base := kubeadm.Configuration{
		Parameters: kubeadm.Parameters{
			TenantControlPlaneName:    "tcp",
			TenantControlPlaneVersion: "v1.34.0",
		},
	}

	withPatches := base
	withPatches.KubeletPatches = []byte(`[{"op":"add","path":"/cgroupDriver","value":"systemd"}]`)

	withDifferentPatches := base
	withDifferentPatches.KubeletPatches = []byte(`[{"op":"remove","path":"/crashLoopBackOff"}]`)

	bareSum := base.Checksum()
	patchedSum := withPatches.Checksum()
	otherPatchedSum := withDifferentPatches.Checksum()

	if bareSum == patchedSum {
		t.Errorf("expected adding kubelet patches to change the checksum, got %q for both", bareSum)
	}
	if patchedSum == otherPatchedSum {
		t.Errorf("expected different kubelet patches to produce different checksums, got %q for both", patchedSum)
	}

	// Idempotence: same configuration must produce the same checksum.
	if got := withPatches.Checksum(); got != patchedSum {
		t.Errorf("checksum is not idempotent: first=%q second=%q", patchedSum, got)
	}
}

// TestConfigurationChecksum_NilAndEmptyPatchesAreEquivalent documents the
// expected behavior for the empty case: a TCP without configurationJSONPatches
// must produce the same checksum as a TCP whose patches were just removed.
// Otherwise removing all patches would also force a re-upload, which is fine
// (the kubelet-config CM should match the spec) but worth pinning explicitly.
func TestConfigurationChecksum_NilAndEmptyPatchesAreEquivalent(t *testing.T) {
	a := kubeadm.Configuration{Parameters: kubeadm.Parameters{TenantControlPlaneName: "tcp"}}
	b := a
	b.KubeletPatches = []byte(nil)

	if a.Checksum() != b.Checksum() {
		t.Errorf("nil and empty KubeletPatches must hash identically, got %q vs %q", a.Checksum(), b.Checksum())
	}
}
