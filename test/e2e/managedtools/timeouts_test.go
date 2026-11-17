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
	"time"

	. "github.com/onsi/gomega"
)

func TestLimitTimeout(t *testing.T) {
	testCases := []struct {
		name      string
		intervals []interface{}
		remaining time.Duration
		want      []interface{}
		wantErr   string
	}{
		{name: "timeout shorter than the time left stays as configured", intervals: []interface{}{"60m", "30s"},
			remaining: 2 * time.Hour, want: []interface{}{"60m", "30s"}},
		{name: "timeout equal to the time left stays", intervals: []interface{}{"60m", "30s"},
			remaining: time.Hour, want: []interface{}{"60m", "30s"}},
		{name: "timeout longer than the time left is lowered, polling kept", intervals: []interface{}{"90m", "30s"},
			remaining: 12 * time.Minute, want: []interface{}{12 * time.Minute, "30s"}},
		{name: "time.Duration timeout", intervals: []interface{}{time.Hour, 10 * time.Second},
			remaining: time.Minute, want: []interface{}{time.Minute, 10 * time.Second}},
		{name: "timeout only", intervals: []interface{}{"10m"}, remaining: time.Minute,
			want: []interface{}{time.Minute}},
		{name: "deadline passed", intervals: []interface{}{"10m", "10s"}, remaining: -90 * time.Second,
			wantErr: "the lane's hard timeout passed 1m30s ago; no step may start another wait"},
		{name: "deadline exactly now", intervals: []interface{}{"10m", "10s"}, remaining: 0,
			wantErr: "the lane's hard timeout passed 0s ago"},
		{name: "no interval", intervals: nil, remaining: time.Hour,
			wantErr: "no interval to limit: the e2e config needs <spec>/<key> or default/<key>"},
		{name: "unparsable timeout", intervals: []interface{}{"an hour", "10s"}, remaining: time.Hour,
			wantErr: `the interval's timeout: "an hour" is not a duration such as "60m"`},
		{name: "zero timeout", intervals: []interface{}{"0s", "10s"}, remaining: time.Hour,
			wantErr: "the interval's timeout: 0s is not a positive duration"},
		{name: "unsupported type", intervals: []interface{}{60, "10s"}, remaining: time.Hour,
			wantErr: "the interval's timeout: 60 (int) is neither a duration string"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			original := append([]interface{}(nil), tc.intervals...)

			got, err := LimitTimeout(tc.intervals, tc.remaining)

			g.Expect(tc.intervals).To(Equal(original), "the input must not be modified")
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestValidateWaitWithinNodeTimeout(t *testing.T) {
	const key = "wait-registry-cluster-gone"
	testCases := []struct {
		name        string
		intervals   []interface{}
		reserve     time.Duration
		nodeTimeout time.Duration
		wantErr     []string
	}{
		{name: "wait plus reserve fit with room", intervals: []interface{}{"20m", "30s"}, reserve: 15 * time.Minute,
			nodeTimeout: 45 * time.Minute},
		{name: "wait plus reserve fill the node timeout exactly", intervals: []interface{}{"30m", "30s"},
			reserve: 15 * time.Minute, nodeTimeout: 45 * time.Minute},
		{name: "time.Duration timeout", intervals: []interface{}{30 * time.Minute, 30 * time.Second},
			reserve: 15 * time.Minute, nodeTimeout: 45 * time.Minute},
		{name: "no reserve", intervals: []interface{}{"45m"}, nodeTimeout: 45 * time.Minute},
		{name: "a longer wait leaves too little for the work after it", intervals: []interface{}{"31m", "30s"},
			reserve: 15 * time.Minute, nodeTimeout: 45 * time.Minute,
			wantErr: []string{"interval " + key + " waits up to 31m0s", "has 45m0s in all and keeps 15m0s",
				"lower the interval's timeout in the e2e config to at most 30m0s"}},
		{name: "a wait as long as the node timeout leaves nothing", intervals: []interface{}{"45m", "30s"},
			reserve: time.Minute, nodeTimeout: 45 * time.Minute, wantErr: []string{"to at most 44m0s"}},
		{name: "no interval", reserve: time.Minute, nodeTimeout: time.Hour,
			wantErr: []string{"the e2e config has no " + key + " interval", "default/" + key}},
		{name: "unparsable timeout", intervals: []interface{}{"half an hour"}, reserve: time.Minute,
			nodeTimeout: time.Hour, wantErr: []string{"the timeout of interval " + key, `"half an hour" is not a duration`}},
		{name: "zero timeout", intervals: []interface{}{"0s"}, reserve: time.Minute, nodeTimeout: time.Hour,
			wantErr: []string{"0s is not a positive duration"}},
		{name: "negative reserve", intervals: []interface{}{"10m"}, reserve: -time.Minute, nodeTimeout: time.Hour,
			wantErr: []string{"must not be negative"}},
		{name: "node timeout not longer than the reserve", intervals: []interface{}{"10m"}, reserve: time.Hour,
			nodeTimeout: time.Hour, wantErr: []string{"needs a node timeout (1h0m0s) longer than the time it keeps"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := ValidateWaitWithinNodeTimeout(key, tc.intervals, tc.reserve, tc.nodeTimeout)
			if len(tc.wantErr) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			for _, want := range tc.wantErr {
				g.Expect(err).To(MatchError(ContainSubstring(want)))
			}
		})
	}
}
