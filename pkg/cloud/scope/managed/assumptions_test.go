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

	"k8s.io/utils/ptr"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"
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

func TestCreateVersion(t *testing.T) {
	testCases := []struct {
		name         string
		distribution controlplanev1.Distribution
		minor        ibmversion.Minor
		want         string
	}{
		{name: "IKS sends the minor", distribution: controlplanev1.DistributionKubernetes, minor: ibmversion.Minor{Major: 1, Minor: 36}, want: "1.36"},
		{name: "ROKS adds _openshift", distribution: controlplanev1.DistributionOpenShift, minor: ibmversion.Minor{Major: 4, Minor: 21}, want: "4.21_openshift"},
		{name: "unknown distribution sends nothing", distribution: "Other", minor: ibmversion.Minor{Major: 1, Minor: 36}, want: ""},
		{name: "empty distribution sends nothing", distribution: "", minor: ibmversion.Minor{Major: 4, Minor: 21}, want: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := CreateVersion(tc.distribution, tc.minor)
			g.Expect(got).To(Equal(tc.want))
			if got != "" {
				// The value is an IBM minor-only version, the form IBM was observed to resolve to its newest patch.
				parsed, err := ibmversion.Parse(got)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(parsed.MinorVersion()).To(Equal(tc.minor))
				g.Expect(parsed.HasFixPack).To(BeFalse())
			}
		})
	}
}

func TestMinWorkersPerZoneAtCreate(t *testing.T) {
	g := NewWithT(t)
	g.Expect(MinWorkersPerZoneAtCreate(controlplanev1.DistributionKubernetes)).To(BeEquivalentTo(1))
	g.Expect(MinWorkersPerZoneAtCreate(controlplanev1.DistributionOpenShift)).To(BeEquivalentTo(2))
	g.Expect(MinWorkersPerZoneAtCreate("Other")).To(BeEquivalentTo(2), "an unknown distribution gets the stricter value")
}

func TestIsMasterReady(t *testing.T) {
	deployed := loadCluster(t, "iks-getCluster-deployed.json")
	testCases := []struct {
		name    string
		cluster *kubernetesservice.Cluster
		want    bool
	}{
		{name: "iks_deployed_fixture_with_empty_health_is_ready", cluster: &deployed, want: true},
		{name: "iks_deploying_fixture_is_not_ready", cluster: ptr.To(loadCluster(t, "iks-getCluster-deploying.json")), want: false},
		{name: "roks_deployed_fixture_is_ready", cluster: ptr.To(loadCluster(t, "roks-getCluster-deployed.json")), want: true},
		{name: "roks_deploying_fixture_is_not_ready", cluster: ptr.To(loadCluster(t, "roks-getCluster-deploying.json")), want: false},
		{name: "normal cluster is ready", cluster: ptr.To(loadCluster(t, "iks-getCluster-normal.json")), want: true},
		{name: "Ready with surrounding white space", cluster: withMaster(deployed, "deployed", " Ready \n", deployed.MasterURL), want: true},
		{name: "Ready with a suffix", cluster: withMaster(deployed, "deployed", "Ready (master health checks pending)", deployed.MasterURL), want: true},
		{name: "deployed without a master URL", cluster: withMaster(deployed, "deployed", "Ready", ""), want: false},
		{name: "deployed but not Ready", cluster: withMaster(deployed, "deployed", "Deploy in progress.", deployed.MasterURL), want: false},
		{name: "Ready but still deploying", cluster: withMaster(deployed, "deploying", "Ready", deployed.MasterURL), want: false},
		{name: "lower-case ready is not IBM's value", cluster: withMaster(deployed, "deployed", "ready", deployed.MasterURL), want: false},
		{name: "nil", cluster: nil, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(IsMasterReady(tc.cluster)).To(Equal(tc.want))
		})
	}
	// The point of the case: the live deployed capture has no master health yet.
	NewWithT(t).Expect(deployed.MasterHealth).To(BeEmpty())
}

func TestMasterHealth(t *testing.T) {
	deployed := loadCluster(t, "iks-getCluster-deployed.json")
	with := func(health string) *kubernetesservice.Cluster {
		c := deployed
		c.MasterHealth = health
		return &c
	}
	testCases := []struct {
		name    string
		cluster *kubernetesservice.Cluster
		want    bool
	}{
		{name: "empty health right after Ready is not unhealthy", cluster: &deployed, want: false},
		{name: "normal", cluster: with("normal"), want: false},
		{name: "unsupported is end of support, not unhealthy", cluster: with("unsupported"), want: false},
		{name: "error", cluster: with("error"), want: true},
		{name: "unavailable", cluster: with("unavailable"), want: true},
		{name: "nil", cluster: nil, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(IsMasterUnhealthy(tc.cluster)).To(Equal(tc.want))
		})
	}
	NewWithT(t).Expect(UnhealthyMasterHealth()).To(Equal([]string{"error", "unavailable"}))
}

// TestCreateDecisionPoints pins the create decision points: the workers' operating system and the answers that clear
// a create intent.
func TestCreateDecisionPoints(t *testing.T) {
	g := NewWithT(t)
	g.Expect(InitialWorkerPoolName).To(Equal("default"))
	g.Expect(ROKSWorkerOperatingSystem).To(BeEmpty())
	g.Expect(ClearIntentErrorCodes()).To(Equal([]string{"E0007", "A0656"}))
	g.Expect(ClearIntentStatusCodes()).To(Equal([]int{400, 401, 403, 422}))
}

// withMaster returns a copy of c with the given master state, status and URL.
func withMaster(c kubernetesservice.Cluster, state, status, url string) *kubernetesservice.Cluster {
	c.MasterState, c.MasterStatus, c.MasterURL = state, status, url
	return &c
}

// TestDeleteDecisionPoints pins the delete decision point, DeleteReissueAfter.
func TestDeleteDecisionPoints(t *testing.T) {
	g := NewWithT(t)
	g.Expect(DeleteReissueAfter).To(Equal(10 * time.Minute))
	// IBM was observed to report 404 about 5 minutes after the DELETE, so a re-issue never races the normal deletion.
	g.Expect(DeleteReissueAfter).To(BeNumerically(">", 5*time.Minute))
}

func TestAdminKubeconfigAndTrust(t *testing.T) {
	g := NewWithT(t)
	g.Expect(AdminKubeconfigFormat).To(Equal("json"))
	g.Expect(CATrustFor(controlplanev1.DistributionKubernetes)).To(Equal(managedkubeconfig.ClusterCA), "IKS needs the cluster CA: the cluster CA issues the API server certificate")
	g.Expect(CATrustFor(controlplanev1.DistributionOpenShift)).To(Equal(managedkubeconfig.SystemRoots), "ROKS -e serves a public certificate")
	g.Expect(CATrustFor("Other")).To(Equal(managedkubeconfig.ClusterCA), "an unknown distribution gets the narrower trust")
}

func TestKubeconfigPolicy(t *testing.T) {
	g := NewWithT(t)
	policy := KubeconfigPolicy()
	g.Expect(policy.Validate()).To(Succeed())
	g.Expect(policy).To(Equal(managedkubeconfig.Policy{
		RefreshFraction:       0.5,
		SameCertificateRetry:  24 * time.Hour,
		ExpiryWarning:         30 * 24 * time.Hour,
		CARecheckInterval:     12 * time.Hour,
		RotationPollInterval:  time.Minute,
		AuthFailureBackoffMin: time.Minute,
		AuthFailureBackoffMax: 10 * time.Minute,
		MinRequeue:            time.Minute,
		MaxRequeue:            12 * time.Hour,
	}))
	// IBM's admin certificate is valid two years: it is refreshed after about a year, and the expiry warning comes well
	// after that.
	twoYears := 2 * 365 * 24 * time.Hour
	g.Expect(time.Duration(float64(twoYears) * policy.RefreshFraction)).To(BeNumerically(">", policy.ExpiryWarning))
}

func TestManagementServerURL(t *testing.T) {
	iks := loadCluster(t, "iks-getCluster-normal.json")
	roks := loadCluster(t, "roks-getCluster-normal.json")
	withURL := func(masterURL string) *kubernetesservice.Cluster {
		c := iks
		c.MasterURL = masterURL
		return &c
	}
	testCases := []struct {
		name         string
		distribution controlplanev1.Distribution
		cluster      *kubernetesservice.Cluster
		want         string
		wantErr      string
	}{
		{name: "https_host_without_port_gets_443", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.eu-de.containers.cloud.ibm.com"), want: "https://c111.eu-de.containers.cloud.ibm.com:443"},
		{name: "https_host_with_port_keeps_it", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.eu-de.containers.cloud.ibm.com:30123"), want: "https://c111.eu-de.containers.cloud.ibm.com:30123"},
		{name: "http_is_rejected", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("http://c111.eu-de.containers.cloud.ibm.com"), wantErr: `uses scheme "http"`},
		{name: "IKS live capture", distribution: controlplanev1.DistributionKubernetes, cluster: &iks, want: "https://c111.eu-de.containers.cloud.ibm.com:30101"},
		{name: "ROKS live capture", distribution: controlplanev1.DistributionOpenShift, cluster: &roks, want: "https://c113-e.eu-de.containers.cloud.ibm.com:30102"},
		{name: "trailing slash is accepted", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.eu-de.containers.cloud.ibm.com:30101/"), want: "https://c111.eu-de.containers.cloud.ibm.com:30101"},
		{name: "IPv6 host keeps its brackets", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://[fd00::1]"), want: "https://[fd00::1]:443"},
		{name: "empty master URL", distribution: controlplanev1.DistributionKubernetes, cluster: withURL(""), wantErr: "reports no masterURL"},
		{name: "not a URL", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111 .example:1"), wantErr: "is not a URL"},
		{name: "user information", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://admin:secret@c111.example:30101"), wantErr: "carries user information"},
		{name: "path", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.example:30101/api"), wantErr: "carries a path"},
		{name: "query", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.example:30101?x=1"), wantErr: "carries a path, a query"},
		{name: "fragment", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.example:30101#top"), wantErr: "carries a path, a query or a fragment"},
		{name: "no host", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://:30101"), wantErr: "names no host"},
		{name: "port zero", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.example:0"), wantErr: `has port "0"`},
		{name: "port too large", distribution: controlplanev1.DistributionKubernetes, cluster: withURL("https://c111.example:65536"), wantErr: `has port "65536"`},
		{name: "unknown distribution", distribution: "Other", cluster: &iks, wantErr: `unknown distribution "Other"`},
		{name: "nil cluster", distribution: controlplanev1.DistributionKubernetes, cluster: nil, wantErr: "no IBM cluster"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ManagementServerURL(tc.distribution, tc.cluster)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(got).To(BeEmpty())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestIsCARotationInProgress(t *testing.T) {
	testCases := []struct {
		name   string
		status string
		want   bool
	}{
		{name: "no rotation ever ran (live captures)", status: loadCluster(t, "iks-getCluster-normal.json").CARotationStatus, want: false},
		{name: "white space only", status: "  ", want: false},
		{name: "rotation complete", status: "CA certificate rotation complete", want: false},
		{name: "rotation complete with details", status: "CA certificate rotation complete. Update your kubeconfig.", want: false},
		{name: "rotation in progress", status: "CA certificate rotation in progress", want: true},
		{name: "unknown status string is fail-safe", status: "Something IBM has not documented", want: true},
		{name: "a different case is not the complete marker", status: "ca certificate rotation complete", want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(IsCARotationInProgress(tc.status)).To(Equal(tc.want))
		})
	}
}

func TestIsMasterUpdatePending(t *testing.T) {
	normal := loadCluster(t, "iks-getCluster-normal.json")
	roks := loadCluster(t, "roks-getCluster-normal.json")
	with := func(c kubernetesservice.Cluster, state, master, target string) *kubernetesservice.Cluster {
		c.State, c.MasterKubeVersion, c.TargetVersion = state, master, target
		return &c
	}
	testCases := []struct {
		name    string
		cluster *kubernetesservice.Cluster
		want    bool
	}{
		{name: "IKS live capture: target equals master", cluster: &normal, want: false},
		{name: "ROKS live capture: target equals master", cluster: &roks, want: false},
		{name: "(pending) marker", cluster: with(normal, "normal", "1.37.1_1517 (pending)", "1.37.1_1517"), want: true},
		{name: "state updating", cluster: with(normal, "updating", "1.36.5_1537", "1.36.5_1537"), want: true},
		{name: "target on the next minor", cluster: with(normal, "normal", "1.36.5_1537", "1.37.1_1517"), want: true},
		{name: "target a newer fix pack (IBM patch)", cluster: with(normal, "normal", "1.36.5_1537", "1.36.6_1540"), want: true},
		{name: "target a newer build of the same patch", cluster: with(normal, "normal", "1.36.5_1537", "1.36.5_1540"), want: true},
		{name: "ROKS target a newer fix pack", cluster: with(roks, "normal", "4.21.35_1537_openshift", "4.21.40_1540_openshift"), want: true},
		{name: "target older than the master", cluster: with(normal, "normal", "1.36.6_1540", "1.36.5_1537"), want: false},
		{name: "target on an older minor", cluster: with(normal, "normal", "1.37.1_1517", "1.36.6_1540"), want: false},
		{name: "no target", cluster: with(normal, "normal", "1.36.5_1537", ""), want: false},
		{name: "unparseable master version is fail-safe", cluster: with(normal, "normal", "garbage", "1.36.5_1537"), want: true},
		{name: "empty master version is fail-safe", cluster: with(normal, "deploying", "", ""), want: true},
		{name: "unparseable target is fail-safe", cluster: with(normal, "normal", "1.36.5_1537", "v1.36.5"), want: true},
		{name: "minor-only target on the same minor is fail-safe", cluster: with(normal, "normal", "1.36.5_1537", "1.36"), want: true},
		{name: "nil", cluster: nil, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(IsMasterUpdatePending(tc.cluster)).To(Equal(tc.want))
		})
	}
}

func TestReportedKubernetesVersion(t *testing.T) {
	testCases := []struct {
		name    string
		current string
		probe   string
		rolling bool
		want    string
	}{
		{name: "empty_status_takes_the_probe", current: "", probe: "v1.36.5+IKS", rolling: false, want: "v1.36.5+IKS"},
		{name: "empty_status_takes_the_probe_while_rolling", current: "", probe: "v1.36.6+IKS", rolling: true, want: "v1.36.6+IKS"},
		{name: "rolling_new_replica_keeps_the_old_version", current: "v1.36.5+IKS", probe: "v1.36.6+IKS", rolling: true, want: "v1.36.5+IKS"},
		{name: "rolling_old_replica_keeps_the_old_version", current: "v1.36.5+IKS", probe: "v1.36.5+IKS", rolling: true, want: "v1.36.5+IKS"},
		{name: "update_done_takes_the_new_version", current: "v1.36.5+IKS", probe: "v1.36.6+IKS", rolling: false, want: "v1.36.6+IKS"},
		{name: "lower_probe_outside_a_rollout_wins", current: "v1.36.6+IKS", probe: "v1.36.5+IKS", rolling: false, want: "v1.36.5+IKS"},
		{name: "rolling_lower_probe_lowers_the_minimum", current: "v1.36.6+IKS", probe: "v1.36.5+IKS", rolling: true, want: "v1.36.5+IKS"},
		{name: "ROKS reports a Kubernetes version", current: "", probe: "v1.34.9", rolling: false, want: "v1.34.9"},
		{name: "rolling over a minor keeps the old minor", current: "v1.34.9", probe: "v1.35.2", rolling: true, want: "v1.34.9"},
		{name: "empty probe keeps the published version", current: "v1.36.5+IKS", probe: "", rolling: false, want: "v1.36.5+IKS"},
		{name: "rolling with an unparseable probe keeps the published version", current: "v1.36.5+IKS", probe: "1.36.5_1537", rolling: true, want: "v1.36.5+IKS"},
		{name: "rolling with an unparseable published version keeps it", current: "garbage", probe: "v1.36.5+IKS", rolling: true, want: "garbage"},
		{name: "not rolling replaces an unparseable published version", current: "garbage", probe: "v1.36.5+IKS", rolling: false, want: "v1.36.5+IKS"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(ReportedKubernetesVersion(tc.current, tc.probe, tc.rolling)).To(Equal(tc.want))
		})
	}

	t.Run("alternating_probes_while_rolling_never_change_status", func(t *testing.T) {
		g := NewWithT(t)
		published := "v1.36.5+IKS"
		for _, probe := range []string{"v1.36.5+IKS", "v1.36.6+IKS", "v1.36.5+IKS", "v1.36.6+IKS"} {
			published = ReportedKubernetesVersion(published, probe, true)
			g.Expect(published).To(Equal("v1.36.5+IKS"))
		}
		// IBM reports the update done: the next probe is published.
		g.Expect(ReportedKubernetesVersion(published, "v1.36.6+IKS", false)).To(Equal("v1.36.6+IKS"))
	})
}

func TestProvesOwnership(t *testing.T) {
	ibmAdmins := []string{"ibm-admins", "system:authenticated"} // the admin certificate's groups observed on IBM Cloud
	testCases := []struct {
		name   string
		result workloadprobe.Result
		want   bool
	}{
		{
			// The certificate was fetched by another IAM identity than the one CAPIBM acts as now (a key rotation
			// or a clusterctl move target). The username is never compared, so the access review alone decides.
			name:   "other_callers_certificate_with_cluster_admin_proves_ownership",
			result: workloadprobe.Result{Username: "KubeCert#<synthetic-iam-id-of-another-caller>", Groups: ibmAdmins, GitVersion: "v1.36.5+IKS", ClusterAdmin: true},
			want:   true,
		},
		{
			name:   "same_result_without_a_username_proves_ownership",
			result: workloadprobe.Result{Groups: ibmAdmins, GitVersion: "v1.36.5+IKS", ClusterAdmin: true},
			want:   true,
		},
		{
			name:   "denied access review does not prove ownership even with the ibm-admins group",
			result: workloadprobe.Result{Username: "KubeCert#<synthetic-iam-id>", Groups: ibmAdmins, GitVersion: "v1.34.9", ClusterAdmin: false},
			want:   false,
		},
		{
			name:   "empty result does not prove ownership",
			result: workloadprobe.Result{},
			want:   false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(ProvesOwnership(tc.result)).To(Equal(tc.want))
		})
	}
}
