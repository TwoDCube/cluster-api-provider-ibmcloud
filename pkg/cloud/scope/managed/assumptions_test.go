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
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
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
