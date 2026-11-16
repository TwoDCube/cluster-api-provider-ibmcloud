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
	"errors"
	"testing"
	"time"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

func TestOutcomeConstructors(t *testing.T) {
	failure := errors.New("IBM Cloud answered HTTP 503")
	testCases := []struct {
		name string
		got  Outcome
		want Outcome
	}{
		{name: "Done", got: Done(), want: Outcome{Kind: OutcomeDone}},
		{
			name: "NotDone keeps the interval and the reason",
			got:  NotDone(IntervalClusterProvisioning, "Creating"),
			want: Outcome{Kind: OutcomeNotDone, Interval: IntervalClusterProvisioning, Reason: "Creating"},
		},
		{name: "Transient keeps the error", got: Transient(failure), want: Outcome{Kind: OutcomeTransient, Err: failure}},
		{
			name: "RateLimited honours a longer Retry-After",
			got:  RateLimited(2 * time.Minute),
			want: Outcome{Kind: OutcomeRateLimited, RetryAfter: 2 * time.Minute},
		},
		{
			name: "RateLimited never waits less than the minimum backoff",
			got:  RateLimited(5 * time.Second),
			want: Outcome{Kind: OutcomeRateLimited, RetryAfter: kubernetesservice.MinRateLimitBackoff},
		},
		{
			name: "RateLimited without Retry-After waits the minimum backoff",
			got:  RateLimited(0),
			want: Outcome{Kind: OutcomeRateLimited, RetryAfter: 30 * time.Second},
		},
		{
			name: "Terminal keeps reason and message",
			got:  Terminal("SubnetNotFound", "subnet x does not exist"),
			want: Outcome{Kind: OutcomeTerminal, Reason: "SubnetNotFound", Message: "subnet x does not exist"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(tc.got).To(Equal(tc.want))
		})
	}
}

func TestTransientWithoutErrorStillFails(t *testing.T) {
	g := NewWithT(t)
	outcome := Transient(nil)
	g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
	g.Expect(outcome.Err).To(MatchError(ContainSubstring("without an error (programming error)")))
}
