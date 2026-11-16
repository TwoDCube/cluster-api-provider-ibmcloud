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

package managedtools

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestValidateUpgradeStep(t *testing.T) {
	testCases := []struct {
		name    string
		from    string
		to      string
		wantErr string
	}{
		{name: "one minor", from: "1.35", to: "1.36"},
		{name: "two minors", from: "1.35", to: "1.37", wantErr: `IKS_DISTRIBUTION_VERSION_UPGRADE_TO "1.37" must be the minor after IKS_DISTRIBUTION_VERSION "1.35", that is "1.36"`},
		{name: "same minor", from: "1.36", to: "1.36", wantErr: `that is "1.37"`},
		{name: "downgrade", from: "1.36", to: "1.35", wantErr: `that is "1.37"`},
		{name: "patch in from", from: "1.35.2", to: "1.36", wantErr: `IKS_DISTRIBUTION_VERSION: invalid minor version "1.35.2"`},
		{name: "empty to", from: "1.35", to: "", wantErr: `IKS_DISTRIBUTION_VERSION_UPGRADE_TO: invalid minor version ""`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := ValidateUpgradeStep(tc.from, tc.to)
			if tc.wantErr == "" {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

func TestIBMVersionMinor(t *testing.T) {
	testCases := []struct {
		name    string
		version string
		want    string
		wantErr bool
	}{
		{name: "IKS fix pack", version: "1.36.5_1537", want: "1.36"},
		{name: "ROKS fix pack", version: "4.21.35_1537_openshift", want: "4.21"},
		{name: "minor only", version: "1.37", want: "1.37"},
		{name: "garbage", version: "v1.36", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := IBMVersionMinor(tc.version)
			if tc.wantErr {
				g.Expect(err).To(HaveOccurred())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestKubeletMinor(t *testing.T) {
	testCases := []struct {
		name    string
		version string
		want    string
		wantErr string
	}{
		{name: "IKS kubelet", version: "v1.36.5+IKS", want: "1.36"},
		{name: "ROKS kubelet", version: "v1.34.9", want: "1.34"},
		{name: "not a version", version: "unknown", wantErr: `kubelet version "unknown" does not parse`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := KubeletMinor(tc.version)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestVersionCounts(t *testing.T) {
	g := NewWithT(t)
	g.Expect(VersionCounts(nil)).To(BeEmpty())
	g.Expect(VersionCounts([]string{"v1.36.5+IKS", "v1.36.5+IKS", "v1.36.6+IKS"})).To(Equal(map[string]int32{
		"v1.36.5+IKS": 2,
		"v1.36.6+IKS": 1,
	}))
}
