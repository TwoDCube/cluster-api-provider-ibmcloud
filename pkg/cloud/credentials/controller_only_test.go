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

package credentials

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"

	. "github.com/onsi/gomega"
)

func TestControllerOnlyResolve(t *testing.T) {
	// Any client.Object works as a holder: ControllerOnly does not read it. A ConfigMap keeps this package free of the
	// managed API types, which it must not import.
	holder := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "dev"}}
	var typedNil *corev1.ConfigMap

	testCases := []struct {
		name    string
		holder  client.Object
		want    Identity
		wantErr string
	}{
		{
			name:   "any holder resolves to the controller identity",
			holder: holder,
			want:   Identity{Key: ControllerKey(), Description: "the CAPIBM controller identity"},
		},
		{
			name:    "nil holder is a programming error",
			holder:  nil,
			wantErr: "needs the Cluster's InfraCluster as holder, got nil",
		},
		{
			name:    "typed nil holder is a programming error",
			holder:  typedNil,
			wantErr: "needs the Cluster's InfraCluster as holder, got nil",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ControllerOnly{}.Resolve(context.Background(), tc.holder)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(got).To(BeZero())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
			g.Expect(got.Validate()).To(Succeed())
		})
	}
}
