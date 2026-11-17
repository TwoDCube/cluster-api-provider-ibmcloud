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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilyaml "sigs.k8s.io/cluster-api/util/yaml"
	"sigs.k8s.io/yaml"

	. "github.com/onsi/gomega"
)

func TestExpandPlaceholders(t *testing.T) {
	testCases := []struct {
		name    string
		text    string
		vars    map[string]string
		want    string
		wantErr string
	}{
		{name: "braces and bare form", text: "image: ${IMAGE}\nreplicas: $N\n", vars: map[string]string{"IMAGE": "busybox", "N": "6"},
			want: "image: busybox\nreplicas: 6\n"},
		{name: "no placeholder", text: "kind: Namespace\n", want: "kind: Namespace\n"},
		{name: "missing names are listed once, sorted", text: "${B} ${A} ${B}", vars: map[string]string{},
			wantErr: "the manifest uses placeholders without a value: [A B]"},
		{name: "a defined empty value is allowed", text: "x: '${EMPTY}'", vars: map[string]string{"EMPTY": ""}, want: "x: ''"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ExpandPlaceholders(tc.text, tc.vars)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(tc.wantErr))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

// probeVars are the values test/e2e/managed_test.go passes to the probe manifests.
var probeVars = map[string]string{
	"PROBE_NAMESPACE":  "capibm-e2e-probe",
	"PROBE_LABEL":      "capibm-e2e/probe",
	"PROBE_IMAGE":      "registry.k8s.io/e2e-test-images/busybox:1.36.1-1",
	"PROBE_REPLICAS":   "6",
	"BLOCKER_REPLICAS": "3",
	// One pinned probe pod: the lane names them pinned-1, pinned-2, ... and pins each to one Node by its name, which
	// is the worker's private IP address on IKS.
	"PINNED_NAME": "pinned-1",
	"NODE_NAME":   "10.243.0.12",
}

// TestProbeManifests renders the real probe manifests of test/e2e/data/managed with the lanes' values and checks the
// properties the lanes rely on, offline: the spread budget allows one disruption, the blocker budget allows none,
// every probe pod carries the probe label, and every pod stays terminating well beyond the 2-second poll. A pinned
// pod must have no controller and no budget and stay on the one Node it is pinned to, so that a drain of that Node
// always evicts it at once, a drain of any other Node never touches it, and nothing puts it back.
func TestProbeManifests(t *testing.T) {
	g := NewWithT(t)
	objects := map[string][]byte{}
	for _, file := range []string{"pdb-probe.yaml", "pdb-blocker.yaml", "pinned-probe.yaml"} {
		raw, err := fs.ReadFile(os.DirFS(filepath.Join("..", "data", "managed")), file)
		g.Expect(err).NotTo(HaveOccurred())
		rendered, err := ExpandPlaceholders(string(raw), probeVars)
		g.Expect(err).NotTo(HaveOccurred(), file)
		documents, err := utilyaml.ToUnstructured([]byte(rendered))
		g.Expect(err).NotTo(HaveOccurred(), file)
		for i := range documents {
			g.Expect(documents[i].GetNamespace()).To(BeElementOf("", probeVars["PROBE_NAMESPACE"]))
			data, err := documents[i].MarshalJSON()
			g.Expect(err).NotTo(HaveOccurred())
			objects[documents[i].GetKind()+"/"+documents[i].GetName()] = data
		}
	}
	g.Expect(objects).To(HaveKey("Namespace/capibm-e2e-probe"))

	spreadBudget := decodeBudget(g, objects["PodDisruptionBudget/spread"])
	g.Expect(spreadBudget.Spec.MaxUnavailable.IntValue()).To(Equal(1))
	blockerBudget := decodeBudget(g, objects["PodDisruptionBudget/blocker"])
	g.Expect(blockerBudget.Spec.MinAvailable.IntValue()).To(Equal(3), "the blocker budget requires every blocker pod")

	for _, name := range []string{"spread", "blocker"} {
		deployment := decodeDeployment(g, objects["Deployment/"+name])
		expectProbePod(g, "Deployment/"+name, deployment.Spec.Template.Labels, &deployment.Spec.Template.Spec)
	}
	blocker := decodeDeployment(g, objects["Deployment/blocker"])
	g.Expect(blocker.Spec.Replicas).To(HaveValue(Equal(int32(3))))
	g.Expect(blocker.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(
		ConsistOf(HaveField("TopologyKey", corev1.LabelHostname)), "one blocker pod per worker")

	pinned := decodePod(g, objects["Pod/pinned-1"])
	expectProbePod(g, "Pod/pinned-1", pinned.Labels, &pinned.Spec)
	g.Expect(pinned.OwnerReferences).To(BeEmpty(), "the pinned pod has no controller, so nothing recreates it")
	// The lane deletes the pinned pods left after a step by this label.
	g.Expect(pinned.Labels).To(HaveKeyWithValue("app.kubernetes.io/name", "pinned"))
	g.Expect(pinned.Spec.NodeName).To(BeEmpty(), "the scheduler binds the pinned pod, so that it checks the Node's room")
	g.Expect(pinned.Spec.NodeSelector).To(BeEmpty(), "the node affinity alone pins the pod")
	g.Expect(pinned.Spec.Affinity).NotTo(BeNil())
	g.Expect(pinned.Spec.Affinity.NodeAffinity).NotTo(BeNil())
	g.Expect(pinned.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(Equal(
		&corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{
			Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{probeVars["NODE_NAME"]},
		}}}}}), "the pinned pod stays on the one Node it is pinned to")
	for _, budget := range []*policyv1.PodDisruptionBudget{spreadBudget, blockerBudget} {
		selector, err := metav1.LabelSelectorAsSelector(budget.Spec.Selector)
		g.Expect(err).NotTo(HaveOccurred(), budget.Name)
		g.Expect(selector.Matches(labels.Set(pinned.Labels))).To(BeFalse(), "PodDisruptionBudget %s selects the "+
			"pinned pod, so a drain of its worker could wait for that budget instead of evicting it", budget.Name)
	}
}

// expectProbePod checks the pod properties every probe workload needs: the probe label, by which the disruption probe
// lists it, and a termination that outlasts the 2-second poll, so that an evicted pod is seen terminating.
//
// Parameters:
//   - g is the test's Gomega.
//   - name names the workload in the failure messages, for example "Deployment/spread".
//   - podLabels are the pod's labels (a Deployment's template labels).
//   - spec is the pod's spec (a Deployment's template spec).
func expectProbePod(g Gomega, name string, podLabels map[string]string, spec *corev1.PodSpec) {
	g.Expect(podLabels).To(HaveKeyWithValue("capibm-e2e/probe", "true"), name)
	g.Expect(spec.TerminationGracePeriodSeconds).To(HaveValue(BeNumerically(">", 20)), name)
	g.Expect(spec.Containers).To(HaveLen(1), name)
	container := spec.Containers[0]
	g.Expect(container.Image).To(Equal(probeVars["PROBE_IMAGE"]), name)
	g.Expect(container.Command).To(ContainElement(ContainSubstring("trap 'sleep 20; exit 0' TERM")), name)
	g.Expect(spec.SecurityContext).NotTo(BeNil(), name)
	g.Expect(spec.SecurityContext.RunAsNonRoot).To(HaveValue(BeTrue()), name)
}

// decodePod unmarshals a rendered Pod.
func decodePod(g Gomega, data []byte) *corev1.Pod {
	g.Expect(data).NotTo(BeEmpty())
	pod := &corev1.Pod{}
	g.Expect(yaml.Unmarshal(data, pod)).To(Succeed())
	return pod
}

// decodeBudget unmarshals a rendered PodDisruptionBudget.
func decodeBudget(g Gomega, data []byte) *policyv1.PodDisruptionBudget {
	g.Expect(data).NotTo(BeEmpty())
	budget := &policyv1.PodDisruptionBudget{}
	g.Expect(yaml.Unmarshal(data, budget)).To(Succeed())
	return budget
}

// decodeDeployment unmarshals a rendered Deployment.
func decodeDeployment(g Gomega, data []byte) *appsv1.Deployment {
	g.Expect(data).NotTo(BeEmpty())
	deployment := &appsv1.Deployment{}
	g.Expect(yaml.Unmarshal(data, deployment)).To(Succeed())
	return deployment
}
