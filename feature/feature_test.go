/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package feature

import (
	"testing"

	"github.com/spf13/pflag"

	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	. "github.com/onsi/gomega"
)

// TestManagedKubernetesDefault locks the spec of the gate in the process-wide set: Alpha, off by default and not
// locked, so an upgrade that does not set EXP_MANAGED_KUBERNETES never starts the managed controllers.
func TestManagedKubernetesDefault(t *testing.T) {
	g := NewWithT(t)

	spec, known := MutableGates.GetAll()[ManagedKubernetes]
	g.Expect(known).To(BeTrue(), "ManagedKubernetes must be registered in MutableGates")
	g.Expect(spec.Default).To(BeFalse())
	g.Expect(spec.PreRelease).To(Equal(featuregate.Alpha))
	g.Expect(spec.LockToDefault).To(BeFalse())

	g.Expect(Gates.Enabled(ManagedKubernetes)).To(BeFalse())
	// The flag help is built from KnownFeatures; this is the line an operator sees in "manager --help".
	g.Expect(Gates.KnownFeatures()).To(ContainElement("ManagedKubernetes=true|false (ALPHA - default=false)"))
}

// TestGatesFollowsMutableGates proves that Gates is a view of MutableGates (a value set through the flag is what every
// caller of Gates sees) and that featuregatetesting, which later packages use to switch the gate in tests, restores the
// default when the test ends.
func TestGatesFollowsMutableGates(t *testing.T) {
	g := NewWithT(t)
	g.Expect(Gates).To(BeIdenticalTo(MutableGates))

	t.Run("enabled for the duration of a subtest", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, Gates, ManagedKubernetes, true)
		NewWithT(t).Expect(Gates.Enabled(ManagedKubernetes)).To(BeTrue())
	})

	g.Expect(Gates.Enabled(ManagedKubernetes)).To(BeFalse(), "featuregatetesting must restore the default")
}

// TestFeatureGatesFlag parses --feature-gates the way cmd/main.go does (AddFlag on a pflag.FlagSet), on a fresh gate set
// per case so that the process-wide set is never mutated or closed by a test.
func TestFeatureGatesFlag(t *testing.T) {
	testCases := []struct {
		name        string
		args        []string
		wantEnabled bool
		wantErr     string
	}{
		{
			name:        "flag absent keeps the default (off)",
			args:        nil,
			wantEnabled: false,
		},
		{
			name:        "manifest default from an unset EXP_MANAGED_KUBERNETES",
			args:        []string{"--feature-gates=ManagedKubernetes=false"},
			wantEnabled: false,
		},
		{
			name:        "EXP_MANAGED_KUBERNETES=true",
			args:        []string{"--feature-gates=ManagedKubernetes=true"},
			wantEnabled: true,
		},
		{
			name:        "AllAlpha turns on the Alpha gate",
			args:        []string{"--feature-gates=AllAlpha=true"},
			wantEnabled: true,
		},
		{
			name:        "an explicit value wins over AllAlpha",
			args:        []string{"--feature-gates=AllAlpha=true,ManagedKubernetes=false"},
			wantEnabled: false,
		},
		{
			name:    "unknown gate is rejected",
			args:    []string{"--feature-gates=NoSuchGate=true"},
			wantErr: "unrecognized feature gate: NoSuchGate",
		},
		{
			// The set is the provider's own (see the package doc): a Cluster API gate name changes nothing here, so it
			// must fail instead of being silently accepted.
			name:    "Cluster API gate name is rejected",
			args:    []string{"--feature-gates=MachinePool=true"},
			wantErr: "unrecognized feature gate: MachinePool",
		},
		{
			name:    "non-boolean value is rejected",
			args:    []string{"--feature-gates=ManagedKubernetes=yes"},
			wantErr: "invalid value of ManagedKubernetes=yes",
		},
		{
			name:    "missing value is rejected",
			args:    []string{"--feature-gates=ManagedKubernetes"},
			wantErr: "missing bool value for ManagedKubernetes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			gate := newFeatureGate()
			fs := pflag.NewFlagSet("manager", pflag.ContinueOnError)
			gate.AddFlag(fs)

			err := fs.Parse(tc.args)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(gate.Enabled(ManagedKubernetes)).To(Equal(tc.wantEnabled))
		})
	}
}

// TestNewFeatureGateIsIndependent proves that test gate sets do not share state with each other or with MutableGates,
// which the flag test above relies on.
func TestNewFeatureGateIsIndependent(t *testing.T) {
	g := NewWithT(t)

	first := newFeatureGate()
	second := newFeatureGate()
	g.Expect(first.Set("ManagedKubernetes=true")).To(Succeed())

	g.Expect(first.Enabled(ManagedKubernetes)).To(BeTrue())
	g.Expect(second.Enabled(ManagedKubernetes)).To(BeFalse())
	g.Expect(Gates.Enabled(ManagedKubernetes)).To(BeFalse())
}
