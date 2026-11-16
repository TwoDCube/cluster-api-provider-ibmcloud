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

package managed

import (
	"strings"
	"testing"

	. "github.com/onsi/gomega"
)

// fixtureAccountID is the (synthetic) account of the scrubbed live captures in kubernetesservice/testdata.
const fixtureAccountID = "a1b2c3d4e5f60718293a4b5c6d7e8f90"

func TestAccountIDFromCRN(t *testing.T) {
	testCases := []struct {
		name        string
		crn         string
		wantAccount string
		wantErr     []string
	}{
		{name: "IKS live capture", crn: "fixture:iks", wantAccount: fixtureAccountID},
		{name: "ROKS live capture", crn: "fixture:roks", wantAccount: fixtureAccountID},
		{
			name:        "uppercase letters and digits are kept as they are",
			crn:         "crn:v1:bluemix:public:containers-kubernetes:us-south:a/ABC123:c0abcdefghijklmnopqr::",
			wantAccount: "ABC123",
		},
		{name: "empty", crn: "", wantErr: []string{"the cluster CRN is empty", "status.clusterCRN"}},
		{name: "not a CRN", crn: "c0abcdefghijklmnopqr", wantErr: []string{`cluster CRN "c0abcdefghijklmnopqr" is not a CRN`, "a/<account-id>"}},
		{
			name:    "nine segments",
			crn:     "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/" + fixtureAccountID + ":c0abcdefghijklmnopqr:",
			wantErr: []string{"is not a CRN"},
		},
		{
			name:    "does not start with crn",
			crn:     "urn:v1:bluemix:public:containers-kubernetes:eu-de:a/" + fixtureAccountID + ":c0abcdefghijklmnopqr::",
			wantErr: []string{"is not a CRN"},
		},
		{
			name:    "organization scope",
			crn:     "crn:v1:bluemix:public:containers-kubernetes:eu-de:o/org1:c0abcdefghijklmnopqr::",
			wantErr: []string{`has the scope "o/org1"`, "a/<account-id>"},
		},
		{
			name:    "account scope without an ID",
			crn:     "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/:c0abcdefghijklmnopqr::",
			wantErr: []string{`has the scope "a/"`},
		},
		{
			name:    "account ID with a slash would break the provider ID",
			crn:     "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/abc/def:c0abcdefghijklmnopqr::",
			wantErr: []string{`has the scope "a/abc/def"`, "letters and digits"},
		},
		{
			name:    "empty scope",
			crn:     "crn:v1:bluemix:public:containers-kubernetes:eu-de::c0abcdefghijklmnopqr::",
			wantErr: []string{`has the scope ""`},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			crn := tc.crn
			if flavor, found := strings.CutPrefix(crn, "fixture:"); found {
				crn = loadCluster(t, flavor+"-getCluster-normal.json").CRN
			}
			account, err := AccountIDFromCRN(crn)
			if len(tc.wantErr) > 0 {
				g.Expect(err).To(HaveOccurred())
				for _, want := range tc.wantErr {
					g.Expect(err.Error()).To(ContainSubstring(want))
				}
				g.Expect(account).To(BeEmpty())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(account).To(Equal(tc.wantAccount))
		})
	}
}
