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
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// The tests of this file pin every value of assumptions.go to its source, so that switching a decision point after a
// live check is a deliberate one-line change that also changes a test.

// TestAssumptionsCiteTheirSource enforces the rule of assumptions.go that every top-level declaration says where its
// value comes from: observed on IBM Cloud, not yet verified against IBM Cloud, or CAPI parity.
func TestAssumptionsCiteTheirSource(t *testing.T) {
	g := NewWithT(t)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "assumptions.go", nil, parser.ParseComments)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(file.Decls).NotTo(BeEmpty())
	g.Expect(uncitedDeclarations(fset, file)).To(BeEmpty(),
		"every declaration in assumptions.go must say where its value comes from, in a form its file comment lists")
}

// TestUncitedDeclarations proves that the check of TestAssumptionsCiteTheirSource finds an uncited declaration of
// every kind, so that the guard cannot pass vacuously.
func TestUncitedDeclarations(t *testing.T) {
	const source = `package managed

import "time"

// Cited is confirmed. Observed on IBM Cloud.
const Cited = 1

// Uncited has no source.
const Uncited = 2

const (
	// GroupedCited is open. Not yet verified against IBM Cloud.
	GroupedCited = 3
	GroupedUncited = 4 // no source either
	TrailingCited  = 5 // Not yet verified against IBM Cloud
)

// UncitedVar has no source.
var UncitedVar = time.Second

// UncitedType has no source.
type UncitedType string

// CitedFunc follows Cluster API. CAPI parity: machine_controller.go:73.
func CitedFunc() {}

// UncitedFunc has no source.
func UncitedFunc() {}

func undocumented() {}
`
	g := NewWithT(t)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", source, parser.ParseComments)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(uncitedDeclarations(fset, file)).To(ConsistOf(
		HavePrefix("Uncited "), HavePrefix("GroupedUncited "), HavePrefix("UncitedVar "),
		HavePrefix("UncitedType "), HavePrefix("UncitedFunc "), HavePrefix("undocumented "),
	))
}

// uncitedDeclarations returns "<name> at <position>" for every top-level declaration of file whose comment cites no
// source. A grouped declaration (const ( ... )) is checked per name: each one needs its own doc or line comment,
// because a group comment would hide which value a later edit changed.
func uncitedDeclarations(fset *token.FileSet, file *ast.File) []string {
	marker := regexp.MustCompile(`[Oo]bserved on IBM Cloud|[Nn]ot yet verified against IBM Cloud|CAPI parity`)
	var uncited []string
	check := func(names []string, pos token.Pos, comments ...*ast.CommentGroup) {
		var text strings.Builder
		for _, c := range comments {
			text.WriteString(c.Text())
		}
		if !marker.MatchString(text.String()) {
			uncited = append(uncited, strings.Join(names, ", ")+" at "+fset.Position(pos).String())
		}
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			check([]string{d.Name.Name}, d.Pos(), d.Doc)
		case *ast.GenDecl:
			if d.Tok == token.IMPORT {
				continue
			}
			// An unparenthesized declaration carries its comment on the GenDecl.
			declDoc := d.Doc
			if d.Lparen.IsValid() {
				declDoc = nil
			}
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.ValueSpec:
					names := make([]string, 0, len(s.Names))
					for _, n := range s.Names {
						names = append(names, n.Name)
					}
					check(names, s.Pos(), declDoc, s.Doc, s.Comment)
				case *ast.TypeSpec:
					check([]string{s.Name.Name}, s.Pos(), declDoc, s.Doc, s.Comment)
				}
			}
		}
	}
	return uncited
}

// TestDrainCadence pins the Cluster API parity values of the drain.
func TestDrainCadence(t *testing.T) {
	g := NewWithT(t)
	g.Expect(DrainRetryInterval).To(Equal(20 * time.Second))
	g.Expect(EvictionBudgetPerReconcile).To(Equal(10 * time.Second))
	g.Expect(DrainBlockedEventInterval).To(Equal(30 * time.Minute))
}

// fixturePath returns the path of a scrubbed live fixture (probes of 2026-11-16) that the kubernetesservice package
// keeps in its testdata: the assumptions are tested against the same captures the service layer decodes.
func fixturePath(name string) string {
	return filepath.Join("..", "..", "services", "kubernetesservice", "testdata", name)
}

// clusterFixture holds the fields of a getCluster capture that the assumptions read.
type clusterFixture struct {
	ID                string `json:"id"`
	CRN               string `json:"crn"`
	State             string `json:"state"`
	CreatedDate       string `json:"createdDate"`
	MasterKubeVersion string `json:"masterKubeVersion"`
	TargetVersion     string `json:"targetVersion"`
	MasterURL         string `json:"masterURL"`
	Lifecycle         struct {
		MasterState  string `json:"masterState"`
		MasterHealth string `json:"masterHealth"`
		MasterStatus string `json:"masterStatus"`
	} `json:"lifecycle"`
	CACertRotationStatus struct {
		Status string `json:"status"`
	} `json:"caCertRotationStatus"`
}

// loadCluster decodes a getCluster capture into the domain type, the way kubernetesservice.GetCluster fills it.
func loadCluster(t *testing.T, name string) kubernetesservice.Cluster {
	t.Helper()
	data, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	var f clusterFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decoding fixture %s: %v", name, err)
	}
	return kubernetesservice.Cluster{
		ID:                f.ID,
		CRN:               f.CRN,
		State:             f.State,
		CreatedDate:       f.CreatedDate,
		MasterKubeVersion: f.MasterKubeVersion,
		TargetVersion:     f.TargetVersion,
		MasterURL:         f.MasterURL,
		MasterState:       f.Lifecycle.MasterState,
		MasterHealth:      f.Lifecycle.MasterHealth,
		MasterStatus:      strings.TrimSpace(f.Lifecycle.MasterStatus),
		CARotationStatus:  f.CACertRotationStatus.Status,
	}
}

func TestParseCreatedDate(t *testing.T) {
	ibmLayout := loadCluster(t, "iks-getCluster-deployed.json").CreatedDate
	testCases := []struct {
		name        string
		createdDate string
		want        time.Time
		wantErr     bool
	}{
		{name: "the layout of the live capture", createdDate: ibmLayout, want: time.Date(2026, 11, 16, 3, 5, 33, 0, time.UTC)},
		{name: "IBM layout with a non-UTC offset", createdDate: "2026-11-16T04:05:33+0100", want: time.Date(2026, 11, 16, 3, 5, 33, 0, time.UTC)},
		{name: "RFC 3339 with Z", createdDate: "2026-11-16T03:05:33Z", want: time.Date(2026, 11, 16, 3, 5, 33, 0, time.UTC)},
		{name: "RFC 3339 with an offset", createdDate: "2026-11-16T05:05:33+02:00", want: time.Date(2026, 11, 16, 3, 5, 33, 0, time.UTC)},
		{name: "empty", createdDate: "", wantErr: true},
		{name: "date only", createdDate: "2026-11-16", wantErr: true},
		{name: "free text", createdDate: "yesterday", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ParseCreatedDate(tc.createdDate)
			if tc.wantErr {
				g.Expect(err).To(MatchError(ContainSubstring("known layouts are RFC 3339")))
				g.Expect(err.Error()).To(ContainSubstring("%q", tc.createdDate))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Equal(tc.want)).To(BeTrue(), "got %s, want %s", got, tc.want)
		})
	}
}

// TestBindingWindows pins the binding decision points: the createdDate layouts, the adoption and grace windows, and
// AdoptByEndpointMatch.
func TestBindingWindows(t *testing.T) {
	g := NewWithT(t)
	g.Expect(CreatedDateLayouts()).To(Equal([]string{time.RFC3339, "2006-01-02T15:04:05-0700"}))
	g.Expect(CreatedDateSkew).To(Equal(2 * time.Minute))
	g.Expect(CreatedDateSlack).To(Equal(10 * time.Minute))
	g.Expect(CreateGracePeriod).To(Equal(5 * time.Minute))
	g.Expect(DeleteLookupGracePeriod).To(Equal(30 * time.Minute))
	g.Expect(AdoptByEndpointMatch).To(BeFalse())

	// A caller that modifies the returned list does not change the next caller's.
	layouts := CreatedDateLayouts()
	layouts[0] = "changed"
	g.Expect(CreatedDateLayouts()[0]).To(Equal(time.RFC3339))
}
