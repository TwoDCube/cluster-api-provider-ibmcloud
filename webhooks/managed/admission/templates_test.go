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
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/cmd/clusterctl/client/yamlprocessor"
	utilyaml "sigs.k8s.io/cluster-api/util/yaml"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// The clusterctl flavor templates of the managed kinds are tested here, next to the validators, because "a template
// that clusterctl renders and the API server admits" needs the CRDs, their CEL rules and the webhooks of this package
// together, which is exactly this package's envtest suite. The templates are rendered with clusterctl's own variable
// processor (yamlprocessor.SimpleProcessor, drone/envsubst), so the test sees what a clusterctl user gets, including
// YAML typing after substitution: a version or a resource group ID that is not quoted in the template renders as a
// number, and a Cloud Object Storage CRN that is not quoted does not parse.

const (
	// managedTemplatesREADMESection is the heading of the templates/README.md section that documents the variables of
	// the managed flavors.
	managedTemplatesREADMESection = "## IBM Cloud managed Kubernetes (IKS and ROKS)"

	// testCOSInstanceCRN is a synthetic Cloud Object Storage instance CRN. Like every real one it ends in "::" (no
	// resource type, no resource), which is not valid YAML unless the template quotes it.
	testCOSInstanceCRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/placeholder-account:placeholder-instance::"

	// testNumericResourceGroupID is a valid resource group ID made of digits only: unquoted, YAML would read it as a
	// number.
	testNumericResourceGroupID = "01234567890123456789012345678901"
)

var (
	// managedTemplatesDir is the repository's templates directory, relative to this package.
	managedTemplatesDir = filepath.Join("..", "..", "..", "templates")

	// readmeVariableRow matches a row of the variables table in templates/README.md: | `NAME` | defaults | flavors |.
	readmeVariableRow = regexp.MustCompile("^\\| `([A-Z][A-Z0-9_]*)` \\| ([^|]*) \\| ([^|]*) \\|")

	// backtickedText matches every `text` of a README table cell.
	backtickedText = regexp.MustCompile("`([^`]+)`")

	// credentialVariable matches variable names that would carry a credential; the managed templates have none,
	// because CAPIBM calls IBM Cloud with its controller's credentials.
	credentialVariable = regexp.MustCompile(`API_?KEY|APIKEY|SECRET|PASSWORD|TOKEN|CREDENTIAL`)
)

// managedTemplateFlavor is what one managed clusterctl flavor must render: the HA flavors in IBM's HA topology of
// 3 zones x 2 workers, the single-zone dev/test flavors in 1 zone x 2 workers.
type managedTemplateFlavor struct {
	// name is the clusterctl flavor; the template is templates/cluster-template-<name>.yaml.
	name string
	// distribution is the control plane's spec.distribution.
	distribution controlplanev1.Distribution
	// versionVariable is the variable that sets spec.distributionVersion, and defaultVersion its default.
	versionVariable string
	defaultVersion  string
	// zones is the number of zones in the IBMCloudManagedCluster and in the initial pool: 3 for IBM's HA topology,
	// 1 for the single-zone dev/test flavors.
	zones int32
	// defaultReplicas is MachinePool.spec.replicas when WORKER_MACHINE_COUNT is not set: 2 workers per zone.
	defaultReplicas int32
	// defaultWorkerFlavor is the pool's spec.flavor when IBMCLOUD_WORKER_FLAVOR is not set.
	defaultWorkerFlavor string
}

// managedTemplateFlavors are the four managed flavors: the HA flavors with 3 zones x 2 workers and the
// single-zone dev/test flavors with 1 zone x 2 workers.
var managedTemplateFlavors = []managedTemplateFlavor{
	{
		name: "managed-iks", distribution: controlplanev1.DistributionKubernetes,
		versionVariable: "IKS_DISTRIBUTION_VERSION", defaultVersion: "1.36",
		zones: 3, defaultReplicas: 6, defaultWorkerFlavor: "cx2.2x4",
	},
	{
		name: "managed-roks", distribution: controlplanev1.DistributionOpenShift,
		versionVariable: "ROKS_DISTRIBUTION_VERSION", defaultVersion: "4.21",
		zones: 3, defaultReplicas: 6, defaultWorkerFlavor: "bx2.4x16",
	},
	{
		name: "managed-iks-single-zone", distribution: controlplanev1.DistributionKubernetes,
		versionVariable: "IKS_DISTRIBUTION_VERSION", defaultVersion: "1.36",
		zones: 1, defaultReplicas: 2, defaultWorkerFlavor: "cx2.2x4",
	},
	{
		name: "managed-roks-single-zone", distribution: controlplanev1.DistributionOpenShift,
		versionVariable: "ROKS_DISTRIBUTION_VERSION", defaultVersion: "4.21",
		zones: 1, defaultReplicas: 2, defaultWorkerFlavor: "bx2.4x16",
	},
}

// templateRendering is one set of variables for a flavor and the object values it must produce.
type templateRendering struct {
	variables map[string]string

	clusterName     string
	resourceGroupID string
	version         string
	replicas        int32
	workersPerZone  int32
	workerFlavor    string
	updatePolicy    infrav1.WorkerUpdatePolicy
	// persistentStorage is the control plane's spec.deletion.persistentStorage: Retain unless the rendering asks for
	// Delete, so that a cluster from an unmodified template never loses its data when it is deleted.
	persistentStorage controlplanev1.PersistentStoragePolicy
}

// newTemplateRendering returns the variables of one rendering of flavor and what they must produce. Only the required
// variables are set unless setOptional is true; then every optional variable is set too, to values that survive only
// as strings when the template quotes them ("1.30" would become the number 1.3, a digits-only resource group ID a
// number), so a lost quote fails the create. Every value is synthetic.
//
// flavor is the flavor under test; clusterName is the clusterctl cluster name argument (CLUSTER_NAME); setOptional
// selects the variable set.
func newTemplateRendering(flavor managedTemplateFlavor, clusterName string, setOptional bool) templateRendering {
	r := templateRendering{
		variables: map[string]string{
			"CLUSTER_NAME":               clusterName,
			"IBMCLOUD_REGION":            "us-south",
			"IBMCLOUD_RESOURCE_GROUP_ID": testResourceGroupID,
			"IBMCLOUD_VPC_NAME":          "test-vpc",
			"COS_INSTANCE_CRN":           testCOSInstanceCRN,
		},
		clusterName:       clusterName,
		resourceGroupID:   testResourceGroupID,
		version:           flavor.defaultVersion,
		replicas:          flavor.defaultReplicas,
		workersPerZone:    2,
		workerFlavor:      flavor.defaultWorkerFlavor,
		updatePolicy:      infrav1.WorkerUpdatePolicyManual,
		persistentStorage: controlplanev1.PersistentStorageRetain,
	}
	// The single-zone flavors read only the first slot; setting all three proves that they ignore the others.
	for i := 1; i <= 3; i++ {
		r.variables[fmt.Sprintf("IBMCLOUD_ZONE_%d", i)] = fmt.Sprintf("us-south-%d", i)
		r.variables[fmt.Sprintf("IBMCLOUD_SUBNET_NAME_%d", i)] = fmt.Sprintf("test-subnet-%d", i)
	}
	if !setOptional {
		return r
	}
	r.resourceGroupID = testNumericResourceGroupID
	r.variables["IBMCLOUD_RESOURCE_GROUP_ID"] = testNumericResourceGroupID
	r.version = map[controlplanev1.Distribution]string{
		controlplanev1.DistributionKubernetes: "1.30",
		controlplanev1.DistributionOpenShift:  "4.20",
	}[flavor.distribution]
	r.variables[flavor.versionVariable] = r.version
	r.workersPerZone = 3
	r.replicas = r.workersPerZone * flavor.zones
	r.variables["WORKER_MACHINE_COUNT"] = strconv.Itoa(int(r.replicas))
	r.workerFlavor = "bx2.8x32"
	r.variables["IBMCLOUD_WORKER_FLAVOR"] = r.workerFlavor
	r.updatePolicy = infrav1.WorkerUpdatePolicyAutomatic
	r.variables["WORKER_UPDATE_POLICY"] = string(r.updatePolicy)
	r.persistentStorage = controlplanev1.PersistentStorageDelete
	r.variables["DELETION_PERSISTENT_STORAGE"] = string(r.persistentStorage)
	return r
}

// renderedManagedTemplate holds the objects of one rendered template as clusterctl prints them (what kubectl apply
// sends) and decoded into their Go types.
type renderedManagedTemplate struct {
	objects        []*unstructured.Unstructured
	cluster        *clusterv1.Cluster
	infraCluster   *infrav1.IBMCloudManagedCluster
	controlPlane   *controlplanev1.IBMCloudManagedControlPlane
	machinePool    *clusterv1.MachinePool
	ibmMachinePool *infrav1.IBMCloudManagedMachinePool
}

// TestManagedTemplates renders the four managed flavor templates with clusterctl's processor, checks the documents of
// each (exactly the five objects of a managed template, exactly one IBMCloudManagedMachinePool and it is the initial
// pool, creation: WithCluster), its topology (HA 3 zones x 2 workers, single-zone 1 zone x 2 workers), creates every
// object through the API server with the CRDs' CEL rules and this package's webhooks (field validation Strict, so an
// unknown field fails too), expects the ibmCloudClusterName warning (no template sets the name), and checks that
// templates/README.md documents every variable with its defaults and flavors.
func TestManagedTemplates(t *testing.T) {
	variableSets := []struct {
		name        string
		setOptional bool
	}{
		{name: "required_variables_only", setOptional: false},
		{name: "every_variable_set", setOptional: true},
	}
	for _, flavor := range managedTemplateFlavors {
		t.Run(flavor.name, func(t *testing.T) {
			raw := readManagedTemplate(t, flavor.name)

			t.Run("header_comment", func(t *testing.T) {
				expectManagedTemplateHeader(NewWithT(t), flavor, raw)
			})
			for _, set := range variableSets {
				t.Run(set.name, func(t *testing.T) {
					g := NewWithT(t)
					want := newTemplateRendering(flavor, "tmpl-"+flavor.name, set.setOptional)

					objects := renderManagedTemplate(g, raw, want.variables)
					expectManagedTemplateDocuments(g, flavor, want, objects)
					rendered := decodeManagedTemplate(g, objects)

					expectManagedTemplateTopology(g, flavor, want, rendered)
					expectManagedTemplateAdmitted(t, want, objects)
				})
			}
		})
	}
	t.Run("every_variable_is_documented_in_templates_README", func(t *testing.T) {
		expectManagedTemplateVariablesDocumented(t)
	})
}

// sharedManagedTemplateDocument is a document that several managed flavor templates carry byte for byte. The
// templates are hand-written files, so no generator keeps the copies equal; TestManagedTemplatesShareDocuments does.
type sharedManagedTemplateDocument struct {
	// name is the subtest name.
	name string
	// kind is the document's top-level kind; a template holds each shared kind in exactly one document.
	kind string
	// flavors are the flavors whose templates carry the document; the first one holds the reference copy.
	flavors []string
}

// sharedManagedTemplateDocuments are the documents the four flavors share: the Cluster in all of them, the
// IBMCloudManagedCluster in the two three-zone (HA) flavors and in the two single-zone flavors, and the control plane
// in the two IKS and in the two ROKS flavors. The two pool documents differ in every flavor (replicas, worker flavor,
// zones), so they are not listed. templates/README.md shows the same table to the people who edit the templates.
var sharedManagedTemplateDocuments = []sharedManagedTemplateDocument{
	{
		name: "Cluster_in_every_flavor", kind: "Cluster",
		flavors: []string{"managed-iks", "managed-roks", "managed-iks-single-zone", "managed-roks-single-zone"},
	},
	{
		name: "IBMCloudManagedCluster_in_the_HA_flavors", kind: "IBMCloudManagedCluster",
		flavors: []string{"managed-iks", "managed-roks"},
	},
	{
		name: "IBMCloudManagedCluster_in_the_single-zone_flavors", kind: "IBMCloudManagedCluster",
		flavors: []string{"managed-iks-single-zone", "managed-roks-single-zone"},
	},
	{
		name: "IBMCloudManagedControlPlane_in_the_IKS_flavors", kind: "IBMCloudManagedControlPlane",
		flavors: []string{"managed-iks", "managed-iks-single-zone"},
	},
	{
		name: "IBMCloudManagedControlPlane_in_the_ROKS_flavors", kind: "IBMCloudManagedControlPlane",
		flavors: []string{"managed-roks", "managed-roks-single-zone"},
	},
}

// TestManagedTemplatesShareDocuments checks that every document of sharedManagedTemplateDocuments is byte-identical,
// comments included, in the templates of the flavors that share it, so that an edit to one copy cannot miss the
// others. A failure names the flavors and the first line that differs, with its line number in both template files.
func TestManagedTemplatesShareDocuments(t *testing.T) {
	for _, shared := range sharedManagedTemplateDocuments {
		t.Run(shared.name, func(t *testing.T) {
			g := NewWithT(t)
			copies := make([]templateDocument, 0, len(shared.flavors))
			for _, flavor := range shared.flavors {
				document, err := findTemplateDocument(flavor, readManagedTemplate(t, flavor), shared.kind)
				g.Expect(err).NotTo(HaveOccurred())
				copies = append(copies, document)
			}
			g.Expect(sharedDocumentMismatch(shared.kind, copies)).To(BeEmpty())
		})
	}
}

// TestFindTemplateDocument checks how the shared-document check cuts a template into documents: the header comment
// is not part of the first document, line numbers count from the top of the file, only an unindented kind line
// selects a document, and a missing or repeated kind is an error.
func TestFindTemplateDocument(t *testing.T) {
	raw := []byte(strings.Join([]string{
		"# clusterctl flavor test: the header comment", // line 1
		"# names the flavor",                           // line 2
		"apiVersion: cluster.x-k8s.io/v1beta2",         // line 3
		"kind: Cluster",                                // line 4
		"spec:",                                        // line 5
		"  infrastructureRef:",                         // line 6
		"    kind: Pool",                               // line 7
		"---",                                          // line 8
		"# a comment of the second document",           // line 9
		"kind: Pool",                                   // line 10
		"---",                                          // line 11
		"kind: Repeated",                               // line 12
		"---",                                          // line 13
		"kind: Repeated",                               // line 14
	}, "\n") + "\n")
	tests := []struct {
		name      string
		kind      string
		wantLines []string
		wantFirst int
		wantErr   string
	}{
		{
			name: "first_document_without_the_header", kind: "Cluster",
			wantLines: []string{"apiVersion: cluster.x-k8s.io/v1beta2", "kind: Cluster", "spec:", "  infrastructureRef:", "    kind: Pool"},
			wantFirst: 3,
		},
		{
			name: "later_document_with_its_own_comment_not_the_indented_reference", kind: "Pool",
			wantLines: []string{"# a comment of the second document", "kind: Pool"},
			wantFirst: 9,
		},
		{
			name: "missing_kind", kind: "Absent",
			wantErr: `templates/cluster-template-test.yaml has no document with the top-level line "kind: Absent"`,
		},
		{
			name: "repeated_kind", kind: "Repeated",
			wantErr: `templates/cluster-template-test.yaml has 2 documents with the top-level line "kind: Repeated"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			document, err := findTemplateDocument("test", raw, tt.kind)
			if tt.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tt.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(document).To(Equal(templateDocument{flavor: "test", lines: tt.wantLines, firstLine: tt.wantFirst}))
		})
	}
}

// TestSharedDocumentMismatch checks the comparison behind TestManagedTemplatesShareDocuments: identical copies pass,
// and a copy with an added, changed or missing line fails with a message that names the flavors that share the
// document and the first differing line, with its line number, in both template files.
func TestSharedDocumentMismatch(t *testing.T) {
	reference := []string{"apiVersion: cluster.x-k8s.io/v1beta2", "kind: Cluster", "spec:", "  infrastructureRef:"}
	paused := []string{"apiVersion: cluster.x-k8s.io/v1beta2", "kind: Cluster", "spec:", "  paused: true", "  infrastructureRef:"}
	trailingSpace := []string{"apiVersion: cluster.x-k8s.io/v1beta2", "kind: Cluster", "spec: ", "  infrastructureRef:"}
	document := func(flavor string, firstLine int, lines []string) templateDocument {
		return templateDocument{flavor: flavor, lines: lines, firstLine: firstLine}
	}
	tests := []struct {
		name   string
		copies []templateDocument
		// want are the parts the message must contain; none means the copies must pass.
		want []string
	}{
		{
			name:   "identical_copies_at_different_lines",
			copies: []templateDocument{document("managed-iks", 6, reference), document("managed-roks", 8, reference), document("managed-iks-single-zone", 9, reference)},
		},
		{
			name:   "a_single_copy",
			copies: []templateDocument{document("managed-iks", 6, reference)},
		},
		{
			name:   "a_line_added_to_one_copy",
			copies: []templateDocument{document("managed-iks", 6, reference), document("managed-roks", 8, paused)},
			want: []string{`the Cluster document must be byte-identical in the templates of managed-iks, managed-roks, ` +
				`which share it, but templates/cluster-template-managed-iks.yaml line 9 is "  infrastructureRef:" where ` +
				`templates/cluster-template-managed-roks.yaml line 11 is "  paused: true"; make the same edit in every ` +
				`one of these templates`},
		},
		{
			name: "the_third_copy_differs",
			copies: []templateDocument{
				document("managed-iks", 6, reference), document("managed-roks", 8, reference), document("managed-iks-single-zone", 9, paused),
			},
			want: []string{
				"templates of managed-iks, managed-roks, managed-iks-single-zone, which share it",
				`templates/cluster-template-managed-iks-single-zone.yaml line 12 is "  paused: true"`,
			},
		},
		{
			name:   "trailing_whitespace_counts",
			copies: []templateDocument{document("managed-iks", 6, reference), document("managed-roks", 8, trailingSpace)},
			want: []string{
				`templates/cluster-template-managed-iks.yaml line 8 is "spec:"`,
				`templates/cluster-template-managed-roks.yaml line 10 is "spec: "`,
			},
		},
		{
			name:   "one_copy_ends_early",
			copies: []templateDocument{document("managed-iks", 6, reference), document("managed-roks", 8, reference[:3])},
			want: []string{
				`templates/cluster-template-managed-iks.yaml line 9 is "  infrastructureRef:"`,
				"templates/cluster-template-managed-roks.yaml line 11 is the end of the document",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			message := sharedDocumentMismatch("Cluster", tt.copies)
			if len(tt.want) == 0 {
				g.Expect(message).To(BeEmpty())
				return
			}
			for _, part := range tt.want {
				g.Expect(message).To(ContainSubstring(part))
			}
		})
	}
}

// templateDocument is one YAML document of a managed flavor template, as lines of text.
type templateDocument struct {
	// flavor is the flavor whose template holds the document.
	flavor string
	// lines are the document's lines without the "---" separators. The first document of a template starts after the
	// header comment, which names the flavor and so differs between flavors.
	lines []string
	// firstLine is the line number in the template file, counted from 1, of lines[0].
	firstLine int
}

// findTemplateDocument returns the one document of a template whose top-level kind is kind. It works on the text, not
// on parsed YAML, because the shared documents must match byte for byte: comments and quotes count.
//
// flavor names the template in the result and in errors; raw is the template file; kind is the kind to find, matched
// as the unindented line "kind: <kind>" so that the kind of a reference inside a spec does not count.
func findTemplateDocument(flavor string, raw []byte, kind string) (templateDocument, error) {
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	kindLine := "kind: " + kind
	// The header comment is the run of comment lines at the top of the file.
	start := 0
	for start < len(lines) && strings.HasPrefix(lines[start], "#") {
		start++
	}
	found := []templateDocument{}
	for start <= len(lines) {
		end := start
		for end < len(lines) && lines[end] != "---" {
			end++
		}
		if slices.Contains(lines[start:end], kindLine) {
			found = append(found, templateDocument{flavor: flavor, lines: lines[start:end], firstLine: start + 1})
		}
		start = end + 1
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return templateDocument{}, fmt.Errorf("templates/%s has no document with the top-level line %q",
			managedTemplateFile(flavor), kindLine)
	default:
		return templateDocument{}, fmt.Errorf("templates/%s has %d documents with the top-level line %q; a template "+
			"holds each shared kind once", managedTemplateFile(flavor), len(found), kindLine)
	}
}

// sharedDocumentMismatch compares every copy of a shared document with the first one and returns "" when all are
// identical. Otherwise it describes the first copy that differs: the flavors that share the document, and the first
// differing line with its line number in both template files.
//
// kind is the document's kind, for the message; copies are the document as each flavor's template holds it.
func sharedDocumentMismatch(kind string, copies []templateDocument) string {
	if len(copies) < 2 {
		return ""
	}
	flavors := make([]string, 0, len(copies))
	for _, c := range copies {
		flavors = append(flavors, c.flavor)
	}
	want := copies[0]
	for _, got := range copies[1:] {
		i := 0
		for i < len(want.lines) && i < len(got.lines) && want.lines[i] == got.lines[i] {
			i++
		}
		if i == len(want.lines) && i == len(got.lines) {
			continue
		}
		return fmt.Sprintf("the %s document must be byte-identical in the templates of %s, which share it, but "+
			"templates/%s line %d is %s where templates/%s line %d is %s; make the same edit in every one of these "+
			"templates", kind, strings.Join(flavors, ", "),
			managedTemplateFile(want.flavor), want.firstLine+i, documentLine(want.lines, i),
			managedTemplateFile(got.flavor), got.firstLine+i, documentLine(got.lines, i))
	}
	return ""
}

// documentLine quotes the line at index i of a document for a mismatch message, or says that the document ends
// before it.
//
// lines are the document's lines; i is the index of the first differing line.
func documentLine(lines []string, i int) string {
	if i >= len(lines) {
		return "the end of the document"
	}
	return strconv.Quote(lines[i])
}

// readManagedTemplate returns the template file of flavor, as clusterctl downloads it from a release.
//
// t is the test; flavor is the clusterctl flavor name.
func readManagedTemplate(t *testing.T, flavor string) []byte {
	t.Helper()
	path := filepath.Join(managedTemplatesDir, managedTemplateFile(flavor))
	raw, err := os.ReadFile(path) //nolint:gosec // a fixed path inside the repository
	if err != nil {
		t.Fatalf("cannot read %s: %v; the managed flavor templates are hand-written files in the repository's "+
			"templates directory, one per flavor", path, err)
	}
	return raw
}

// managedTemplateFile returns the file name of the template of flavor in the templates directory.
//
// flavor is the clusterctl flavor name.
func managedTemplateFile(flavor string) string {
	return "cluster-template-" + flavor + ".yaml"
}

// expectManagedTemplateHeader checks the leading comment block of a template: it names the flavor, the single-zone
// flavors say that they are not HA, the HA flavors name IBM's HA topology, and every flavor tells the user which
// --worker-machine-count to pass, because clusterctl generate cluster sets WORKER_MACHINE_COUNT to 0 without it.
//
// g is the test's gomega; flavor is the flavor whose template raw is.
func expectManagedTemplateHeader(g *WithT, flavor managedTemplateFlavor, raw []byte) {
	header := []string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "#") {
			break
		}
		header = append(header, strings.TrimSpace(strings.TrimPrefix(line, "#")))
	}
	text := strings.Join(header, " ")
	g.Expect(text).To(HavePrefix("clusterctl flavor "+flavor.name+":"),
		"the template must start with a comment block that names its flavor")
	if flavor.zones == 1 {
		g.Expect(text).To(ContainSubstring("NOT HA"), "a single-zone flavor must say in its header that it is not HA")
	} else {
		g.Expect(text).To(ContainSubstring("IBM's HA topology"), "an HA flavor must name IBM's HA topology in its header")
	}
	g.Expect(text).To(ContainSubstring(fmt.Sprintf("--worker-machine-count=%d", flavor.defaultReplicas)),
		"the header must tell clusterctl generate cluster users which --worker-machine-count to pass")
}

// renderManagedTemplate renders raw as clusterctl generate does and splits it into objects. Like clusterctl, it fails
// when a variable without a default has no value.
//
// g is the test's gomega; raw is the template; variables are the variable values (clusterctl reads them from the
// environment and its config file).
func renderManagedTemplate(g *WithT, raw []byte, variables map[string]string) []*unstructured.Unstructured {
	processed, err := yamlprocessor.NewSimpleProcessor().Process(raw, func(name string) (string, error) {
		value, ok := variables[name]
		if !ok {
			return "", fmt.Errorf("variable %s is not set", name)
		}
		return value, nil
	})
	g.Expect(err).NotTo(HaveOccurred(), "clusterctl cannot render the template with the variables %v", variables)
	objects, err := utilyaml.ToUnstructured(processed)
	g.Expect(err).NotTo(HaveOccurred(), "the rendered template is not valid YAML; a value that needs quotes is not "+
		"quoted in the template:\n%s", processed)
	result := make([]*unstructured.Unstructured, 0, len(objects))
	for i := range objects {
		result = append(result, &objects[i])
	}
	return result
}

// expectManagedTemplateDocuments checks the documents of a rendered template: exactly the five objects of a managed
// template (Cluster, IBMCloudManagedCluster, IBMCloudManagedControlPlane, MachinePool, IBMCloudManagedMachinePool) in
// order, the Cluster first (kubectl apply creates the objects in file order, and the ibmCloudClusterName warning finds
// the Cluster of a template control plane only when that Cluster exists already), exactly one
// IBMCloudManagedMachinePool, no namespace (clusterctl --target-namespace sets it), and a MachinePool spec.replicas
// that is a YAML integer.
//
// g is the test's gomega; flavor and want describe the expected rendering; objects are the rendered objects.
func expectManagedTemplateDocuments(g *WithT, flavor managedTemplateFlavor, want templateRendering, objects []*unstructured.Unstructured) {
	kinds := make([]string, 0, len(objects))
	pools := 0
	for _, obj := range objects {
		kinds = append(kinds, obj.GetKind())
		if obj.GetKind() == "IBMCloudManagedMachinePool" {
			pools++
		}
		g.Expect(obj.GetNamespace()).To(BeEmpty(), "%s %s must not set a namespace; clusterctl --target-namespace sets it",
			obj.GetKind(), obj.GetName())
	}
	g.Expect(pools).To(Equal(1), "%s must have exactly one IBMCloudManagedMachinePool, the initial pool", flavor.name)
	g.Expect(kinds).To(Equal([]string{
		"Cluster", "IBMCloudManagedCluster", "IBMCloudManagedControlPlane", "MachinePool", "IBMCloudManagedMachinePool",
	}), "%s must hold the five managed template objects in this order, the Cluster first", flavor.name)

	// A quoted ${WORKER_MACHINE_COUNT} renders as a string, which the MachinePool schema rejects; the fake MachinePool
	// CRD of envtest accepts any spec, so the type is checked here.
	replicas, found, err := unstructured.NestedFieldNoCopy(objects[3].Object, "spec", "replicas")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(found).To(BeTrue(), "the MachinePool must set spec.replicas")
	// The YAML-to-JSON conversion of the clusterctl YAML utilities gives every number as a float64, so the type check
	// is "a number", which a string is not.
	g.Expect(replicas).To(BeNumerically("==", want.replicas), "MachinePool spec.replicas must render as the YAML "+
		"number %d; keep the WORKER_MACHINE_COUNT variable unquoted in the template", want.replicas)

	_, found, err = unstructured.NestedFieldNoCopy(objects[0].Object, "spec", "clusterNetwork")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(found).To(BeFalse(), "the Cluster must not set spec.clusterNetwork: IBM then picks free ranges, and a fixed "+
		"range would conflict with a second cluster made from the same template")
}

// decodeManagedTemplate decodes the five objects that expectManagedTemplateDocuments checked into their Go types,
// strictly: an unknown field or a wrongly typed value fails, which also covers the fields of the CAPI kinds that the
// fake envtest CRDs do not check.
//
// g is the test's gomega; objects are the rendered objects in the order expectManagedTemplateDocuments checked.
func decodeManagedTemplate(g *WithT, objects []*unstructured.Unstructured) *renderedManagedTemplate {
	r := &renderedManagedTemplate{
		objects:        objects,
		cluster:        &clusterv1.Cluster{},
		infraCluster:   &infrav1.IBMCloudManagedCluster{},
		controlPlane:   &controlplanev1.IBMCloudManagedControlPlane{},
		machinePool:    &clusterv1.MachinePool{},
		ibmMachinePool: &infrav1.IBMCloudManagedMachinePool{},
	}
	typed := []any{r.cluster, r.infraCluster, r.controlPlane, r.machinePool, r.ibmMachinePool}
	for i, obj := range objects {
		err := runtime.DefaultUnstructuredConverter.FromUnstructuredWithValidation(obj.Object, typed[i], true)
		g.Expect(err).NotTo(HaveOccurred(), "%s %s does not decode strictly into its Go type (unknown field or wrong "+
			"type)", obj.GetKind(), obj.GetName())
	}
	return r
}

// expectManagedTemplateTopology checks every rendered spec against the expected rendering: the references between
// the objects, the zone lists of the IBMCloudManagedCluster and of the pool (3 zones for the HA flavors, 1 for the
// single-zone flavors, the same zones in both), the replicas as a whole number of workers per zone (2 by default:
// IBM's HA topology of 6 / 3 and the single-zone 2 / 1), the initial pool marker, the control plane without
// ibmCloudClusterName, and the variable values in the right fields.
//
// g is the test's gomega; flavor and want describe the expected rendering; r holds the decoded objects.
func expectManagedTemplateTopology(g *WithT, flavor managedTemplateFlavor, want templateRendering, r *renderedManagedTemplate) {
	name := want.clusterName
	poolName := name + "-default"

	g.Expect(r.cluster.Name).To(Equal(name))
	g.Expect(r.cluster.Spec.InfrastructureRef).To(Equal(clusterv1.ContractVersionedObjectReference{
		APIGroup: infrav1.GroupVersion.Group, Kind: "IBMCloudManagedCluster", Name: name,
	}), "the Cluster must reference the template's IBMCloudManagedCluster")
	g.Expect(r.cluster.Spec.ControlPlaneRef).To(Equal(clusterv1.ContractVersionedObjectReference{
		APIGroup: controlplanev1.GroupVersion.Group, Kind: "IBMCloudManagedControlPlane", Name: controlPlaneNameFor(name),
	}), "the Cluster must reference the template's IBMCloudManagedControlPlane")

	zones := []infrav1.ZoneSubnet{}
	poolZones := []infrav1.WorkerPoolZone{}
	for i := int32(1); i <= flavor.zones; i++ {
		zone := fmt.Sprintf("us-south-%d", i)
		zones = append(zones, infrav1.ZoneSubnet{Name: zone, Subnet: infrav1.ResourceIdentifier{Name: fmt.Sprintf("test-subnet-%d", i)}})
		poolZones = append(poolZones, infrav1.WorkerPoolZone{Name: zone})
	}
	g.Expect(r.infraCluster.Name).To(Equal(name))
	g.Expect(r.infraCluster.Spec).To(Equal(infrav1.IBMCloudManagedClusterSpec{
		Region:        "us-south",
		ResourceGroup: infrav1.ResourceGroupReference{ID: want.resourceGroupID},
		VPC:           infrav1.VPCSource{Type: infrav1.SourceTypeReference, Reference: infrav1.ResourceIdentifier{Name: "test-vpc"}},
		Zones:         zones,
	}), "%s must list %d zone(s) with one subnet each in the IBMCloudManagedCluster", flavor.name, flavor.zones)

	wantControlPlane := controlplanev1.IBMCloudManagedControlPlaneSpec{
		Distribution:        flavor.distribution,
		DistributionVersion: want.version,
		Deletion:            controlplanev1.DeletionSpec{PersistentStorage: want.persistentStorage},
	}
	if flavor.distribution == controlplanev1.DistributionOpenShift {
		wantControlPlane.OpenShift = controlplanev1.OpenShiftSpec{ImageRegistry: controlplanev1.ImageRegistrySpec{
			COS: controlplanev1.COSInstanceReference{InstanceCRN: testCOSInstanceCRN},
		}}
	}
	g.Expect(r.controlPlane.Name).To(Equal(controlPlaneNameFor(name)))
	g.Expect(r.controlPlane.Spec).To(Equal(wantControlPlane), "%s must render this control plane; no template sets "+
		"ibmCloudClusterName", flavor.name)

	g.Expect(r.machinePool.Name).To(Equal(poolName))
	g.Expect(r.machinePool.Spec.ClusterName).To(Equal(name))
	g.Expect(r.machinePool.Spec.Replicas).To(HaveValue(Equal(want.replicas)))
	g.Expect(*r.machinePool.Spec.Replicas%flavor.zones).To(BeZero(),
		"MachinePool spec.replicas must be a multiple of the pool's zone count (IBM sizes pools per zone)")
	g.Expect(*r.machinePool.Spec.Replicas/flavor.zones).To(Equal(want.workersPerZone),
		"%s must run %d worker(s) in each of its %d zone(s)", flavor.name, want.workersPerZone, flavor.zones)
	template := r.machinePool.Spec.Template.Spec
	g.Expect(template.ClusterName).To(Equal(name))
	g.Expect(template.Bootstrap.DataSecretName).To(HaveValue(BeEmpty()),
		"a managed pool has no bootstrap data; dataSecretName \"\" tells CAPI so")
	g.Expect(template.InfrastructureRef).To(Equal(clusterv1.ContractVersionedObjectReference{
		APIGroup: infrav1.GroupVersion.Group, Kind: "IBMCloudManagedMachinePool", Name: poolName,
	}), "the MachinePool must reference the template's IBMCloudManagedMachinePool")

	g.Expect(r.ibmMachinePool.Name).To(Equal(poolName))
	g.Expect(r.ibmMachinePool.Spec.Creation).To(Equal(infrav1.PoolCreationWithCluster),
		"the template's only pool must be the initial pool, creation: WithCluster")
	g.Expect(r.ibmMachinePool.Spec).To(Equal(infrav1.IBMCloudManagedMachinePoolSpec{
		PoolName:      "default",
		Creation:      infrav1.PoolCreationWithCluster,
		Flavor:        want.workerFlavor,
		Zones:         poolZones,
		WorkerUpdates: infrav1.WorkerUpdatesSpec{Policy: want.updatePolicy},
	}), "%s must render this initial pool, in the same zones as the IBMCloudManagedCluster", flavor.name)
}

// expectManagedTemplateAdmitted creates every rendered object in a new namespace, in file order, as kubectl apply of
// clusterctl's output does, through the API server with the CRDs' CEL rules and this package's webhooks, with field
// validation Strict so that an unknown field fails instead of being dropped. The only warning must be the
// ibmCloudClusterName warning, naming the template's Cluster: the control plane carries no Cluster label, so the
// webhook finds the Cluster through its controlPlaneRef.
//
// t is the test (it owns the namespace); want is the expected rendering; objects are the rendered objects.
func expectManagedTemplateAdmitted(t *testing.T, want templateRendering, objects []*unstructured.Unstructured) {
	t.Helper()
	g := NewWithT(t)
	namespace := newTestNamespace(t, "managed-template")
	c, recorder := newWarningClient(g)
	for _, obj := range objects {
		obj = obj.DeepCopy()
		obj.SetNamespace(namespace)
		g.Expect(c.Create(ctx, obj, client.FieldValidation(metav1.FieldValidationStrict))).To(Succeed(),
			"the API server must admit %s %s as the template renders it", obj.GetKind(), obj.GetName())
	}
	g.Expect(recorder.warnings).To(Equal([]string{clusterNameWarning(want.clusterName)}),
		"creating the template must return exactly the ibmCloudClusterName warning, naming the Cluster")
}

// templateVariableUse is how the managed templates use one variable.
type templateVariableUse struct {
	// flavors are the flavors whose template reads the variable, in managedTemplateFlavors order.
	flavors []string
	// required is true when a template reads the variable without a default.
	required bool
	// defaults are the distinct defaults the templates give it.
	defaults []string
}

// documentedTemplateVariable is one row of the variables table in templates/README.md.
type documentedTemplateVariable struct {
	// defaults is the Default cell: "required" or the defaults in backticks.
	defaults string
	// flavors is the Flavors cell: "all" or the flavor names in backticks.
	flavors string
}

// expectManagedTemplateVariablesDocumented checks that the variables table of the managed section of
// templates/README.md documents exactly the variables of the four templates (as clusterctl finds them, comments
// included), each with its defaults ("required" when there is none) and the flavors that read it, and that no
// template has a credential variable, because CAPIBM calls IBM Cloud with its controller's credentials.
//
// t is the test.
func expectManagedTemplateVariablesDocumented(t *testing.T) {
	t.Helper()
	g := NewWithT(t)
	used := map[string]*templateVariableUse{}
	allFlavors := make([]string, 0, len(managedTemplateFlavors))
	for _, flavor := range managedTemplateFlavors {
		allFlavors = append(allFlavors, flavor.name)
		variables, err := yamlprocessor.NewSimpleProcessor().GetVariableMap(readManagedTemplate(t, flavor.name))
		g.Expect(err).NotTo(HaveOccurred(), "clusterctl cannot list the variables of %s", flavor.name)
		for name, value := range variables {
			use := used[name]
			if use == nil {
				use = &templateVariableUse{}
				used[name] = use
			}
			use.flavors = append(use.flavors, flavor.name)
			switch {
			case value == nil:
				use.required = true
			case !slices.Contains(use.defaults, *value):
				use.defaults = append(use.defaults, *value)
			}
		}
	}

	documented := readDocumentedTemplateVariables(t)
	g.Expect(slices.Sorted(maps.Keys(documented))).To(Equal(slices.Sorted(maps.Keys(used))),
		"the variables table of templates/README.md section %q must list exactly the variables of the managed "+
			"templates", managedTemplatesREADMESection)

	for name, use := range used {
		g.Expect(name).NotTo(MatchRegexp(credentialVariable.String()),
			"variable %s looks like a credential; the managed templates use the controller's credentials", name)
		doc, ok := documented[name]
		if !ok {
			continue // reported by the key comparison above
		}
		if doc.flavors == "all" {
			g.Expect(use.flavors).To(Equal(allFlavors), "templates/README.md says every managed flavor reads %s", name)
		} else {
			g.Expect(backticked(doc.flavors)).To(ConsistOf(use.flavors),
				"the Flavors cell of %s in templates/README.md must name the flavors that read it", name)
		}
		if use.required {
			g.Expect(use.defaults).To(BeEmpty(), "%s is required in one template and has a default in another", name)
			g.Expect(doc.defaults).To(Equal("required"), "the Default cell of %s in templates/README.md must say "+
				"required: a template reads it without a default", name)
			continue
		}
		g.Expect(backticked(doc.defaults)).To(ConsistOf(use.defaults),
			"the Default cell of %s in templates/README.md must give the templates' defaults in backticks", name)
	}
}

// readDocumentedTemplateVariables returns the rows of the variables table in the managed section of
// templates/README.md, keyed by variable name. Only rows whose first cell is an upper-case name in backticks count,
// so the flavor table of the same section is skipped.
//
// t is the test.
func readDocumentedTemplateVariables(t *testing.T) map[string]documentedTemplateVariable {
	t.Helper()
	path := filepath.Join(managedTemplatesDir, "README.md")
	data, err := os.ReadFile(path) //nolint:gosec // a fixed path inside the repository
	if err != nil {
		t.Fatalf("cannot read %s: %v", path, err)
	}
	documented := map[string]documentedTemplateVariable{}
	inSection := false
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case line == managedTemplatesREADMESection:
			inSection = true
			continue
		case inSection && strings.HasPrefix(line, "## "):
			inSection = false
		}
		if !inSection {
			continue
		}
		if m := readmeVariableRow.FindStringSubmatch(line); m != nil {
			documented[m[1]] = documentedTemplateVariable{defaults: strings.TrimSpace(m[2]), flavors: strings.TrimSpace(m[3])}
		}
	}
	if len(documented) == 0 {
		t.Fatalf("%s has no variables table under %q", path, managedTemplatesREADMESection)
	}
	return documented
}

// backticked returns every `text` of a README table cell, without the backticks.
//
// cell is the cell's text.
func backticked(cell string) []string {
	matches := backtickedText.FindAllStringSubmatch(cell, -1)
	values := make([]string, 0, len(matches))
	for _, m := range matches {
		values = append(values, m[1])
	}
	return values
}
