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
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

// The cluster IDs, bucket names and CRN of the bucket tests are made up; the bucket names follow IBM's documented form
// roks-<cluster ID>-<random suffix>.
const (
	testCOSInstanceCRN  = "crn:v1:bluemix:public:cloud-object-storage:global:a/0123456789abcdef0123456789abcdef:11111111-2222-3333-4444-555555555555::"
	otherRunClusterID   = "c0otheraaaaaaaaaaaaa"
	runRegistryBucket   = "roks-" + testClusterID + "-a1b2c3"
	otherRegistryBucket = "roks-" + otherRunClusterID + "-d4e5f6"
	sharedBucket        = "team-backups"
)

func TestValidateCOSInstanceCRN(t *testing.T) {
	testCases := []struct {
		name    string
		crn     string
		wantErr bool
	}{
		{name: "instance CRN", crn: testCOSInstanceCRN},
		{name: "empty", crn: "", wantErr: true},
		{name: "instance GUID only", crn: "11111111-2222-3333-4444-555555555555", wantErr: true},
		{name: "CRN of another service", crn: "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/0123:c0abc::", wantErr: true},
		{name: "old CRN version", crn: "crn:v0:bluemix:public:cloud-object-storage:global:a/0123:1111::", wantErr: true},
		{name: "trailing newline", crn: testCOSInstanceCRN + "\n", wantErr: true},
		{name: "space inside", crn: "crn:v1:bluemix:public:cloud-object-storage:global:a/0123: 1111::", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := ValidateCOSInstanceCRN(tc.crn)
			if !tc.wantErr {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(And(ContainSubstring(COSInstanceCRNVariable),
				ContainSubstring("crn:v1:bluemix:public:cloud-object-storage:global:"))))
		})
	}
}

func TestBucketLocation(t *testing.T) {
	testCases := []struct {
		constraint string
		want       string
		wantErr    bool
	}{
		{constraint: "us-south-smart", want: "us-south"},
		{constraint: "eu-de-standard", want: "eu-de"},
		{constraint: "us-standard", want: "us"},
		{constraint: "ams03-cold", want: "ams03"},
		{constraint: "us-south-onerate_active", want: "us-south"},
		{constraint: "", wantErr: true},
		{constraint: "standard", wantErr: true},
		{constraint: "us-", wantErr: true},
		{constraint: "-standard", wantErr: true},
		{constraint: "US-SOUTH-STANDARD", wantErr: true},
		{constraint: "evil.example.com-standard", wantErr: true},
		{constraint: "evil/path-standard", wantErr: true},
		{constraint: "us--standard", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.constraint, func(t *testing.T) {
			g := NewWithT(t)
			location, err := BucketLocation(tc.constraint)
			if tc.wantErr {
				g.Expect(err).To(MatchError(ContainSubstring("<location>-<storage class>")))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(location).To(Equal(tc.want))
		})
	}
}

func TestRegistryBucketNamePrefix(t *testing.T) {
	g := NewWithT(t)
	prefix := RegistryBucketNamePrefix(testClusterID)
	g.Expect(prefix).To(Equal("roks-" + testClusterID + "-"))
	owner, ok := RegistryBucketOwner(prefix+"a1b2c3", []string{testClusterID})
	g.Expect(ok).To(BeTrue(), "a bucket named with the prefix belongs to the cluster")
	g.Expect(owner).To(Equal(testClusterID))
}

func TestRegistryBucketOwner(t *testing.T) {
	testCases := []struct {
		name       string
		bucket     string
		clusterIDs []string
		wantID     string
	}{
		{name: "the run's registry bucket", bucket: runRegistryBucket, clusterIDs: []string{testClusterID}, wantID: testClusterID},
		{name: "second of two run clusters", bucket: otherRegistryBucket, clusterIDs: []string{testClusterID, otherRunClusterID}, wantID: otherRunClusterID},
		{name: "a suffix of another form still matches", bucket: "roks-" + testClusterID + "-registry.backup", clusterIDs: []string{testClusterID}, wantID: testClusterID},
		{name: "registry bucket of a cluster of another run", bucket: otherRegistryBucket, clusterIDs: []string{testClusterID}},
		{name: "no run cluster", bucket: runRegistryBucket},
		{name: "empty ID never matches", bucket: "roks--a1b2c3", clusterIDs: []string{""}},
		{name: "an ID with a hyphen is not an ID", bucket: "roks-c0-run-a1b2c3", clusterIDs: []string{"c0-run"}},
		{name: "a name in place of an ID is not an ID", bucket: "roks-capibm-e2e-abc123-roks-a1b2c3", clusterIDs: []string{"capibm-e2e-abc123-roks"}},
		{name: "prefix without suffix", bucket: "roks-" + testClusterID + "-", clusterIDs: []string{testClusterID}},
		{name: "prefix without hyphen", bucket: "roks-" + testClusterID, clusterIDs: []string{testClusterID}},
		{name: "ID is only the start of the name's ID", bucket: "roks-" + testClusterID + "x-a1b2c3", clusterIDs: []string{testClusterID}},
		{name: "longer ID does not match a shorter one", bucket: runRegistryBucket, clusterIDs: []string{testClusterID + "x"}},
		{name: "ID elsewhere in the name", bucket: "backup-roks-" + testClusterID + "-a1b2c3", clusterIDs: []string{testClusterID}},
		{name: "unrelated bucket", bucket: sharedBucket, clusterIDs: []string{testClusterID}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			id, ok := RegistryBucketOwner(tc.bucket, tc.clusterIDs)
			g.Expect(ok).To(Equal(tc.wantID != ""))
			g.Expect(id).To(Equal(tc.wantID))
		})
	}
}

func TestDiffBuckets(t *testing.T) {
	lane, err := NewLaneScope(DefaultNamePrefix, laneRunID, testVPCID)
	if err != nil {
		t.Fatalf("building the lane scope: %v", err)
	}
	baseline := []Bucket{{Name: sharedBucket}, {Name: "roks-c0olderaaaaaaaaaaaaa-zzzzzz"}}
	after := []Bucket{
		{Name: "zz-new-by-someone"},
		{Name: otherRegistryBucket, LocationConstraint: "us-south-smart"},
		{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"},
		{Name: sharedBucket},
		{Name: "roks-c0olderaaaaaaaaaaaaa-zzzzzz"},
	}
	testCases := []struct {
		name          string
		before, after []Bucket
		clusterIDs    []string
		scope         InventoryScope
		want          BucketDiff
	}{
		{
			name: "account scope: the run's bucket is deleted, every other new bucket is a leftover",
			// The order of after differs from the result's: both lists come back sorted by name.
			before: baseline, after: after, clusterIDs: []string{testClusterID}, scope: AccountScope(),
			want: BucketDiff{
				RunRegistry: []Bucket{{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"}},
				Leftovers:   []Bucket{{Name: otherRegistryBucket, LocationConstraint: "us-south-smart"}, {Name: "zz-new-by-someone"}},
			},
		},
		{
			name:   "lane scope: other runs' buckets are not the lane's, and are reported as not attributed",
			before: baseline, after: after, clusterIDs: []string{testClusterID}, scope: lane,
			want: BucketDiff{
				RunRegistry:   []Bucket{{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"}},
				NotAttributed: []Bucket{{Name: otherRegistryBucket, LocationConstraint: "us-south-smart"}, {Name: "zz-new-by-someone"}},
			},
		},
		{
			name:   "two run clusters",
			before: baseline, after: after, clusterIDs: []string{otherRunClusterID, testClusterID}, scope: lane,
			want: BucketDiff{
				RunRegistry: []Bucket{
					{Name: otherRegistryBucket, LocationConstraint: "us-south-smart"},
					{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"},
				},
				NotAttributed: []Bucket{{Name: "zz-new-by-someone"}},
			},
		},
		{
			name:   "no run cluster ID: nothing is the run's, so the registry-looking bucket is a leftover",
			before: baseline, after: []Bucket{{Name: runRegistryBucket}}, scope: AccountScope(),
			want: BucketDiff{Leftovers: []Bucket{{Name: runRegistryBucket}}},
		},
		{
			name:   "lane scope, no run cluster ID: the run's own registry bucket is reported as not attributed",
			before: baseline, after: []Bucket{{Name: runRegistryBucket}}, scope: lane,
			want: BucketDiff{NotAttributed: []Bucket{{Name: runRegistryBucket}}},
		},
		{
			name:   "a bucket of the baseline is never the run's, whatever its name",
			before: []Bucket{{Name: runRegistryBucket}}, after: []Bucket{{Name: runRegistryBucket}},
			clusterIDs: []string{testClusterID}, scope: AccountScope(),
		},
		{
			name:   "a bucket deleted during the run is no leftover",
			before: baseline, after: nil, clusterIDs: []string{testClusterID}, scope: AccountScope(),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(DiffBuckets(tc.before, tc.after, tc.clusterIDs, tc.scope)).To(Equal(tc.want))
		})
	}
}

// fakeBucketAPI is an in-memory BucketAPI with scripted errors, for the tests of CleanUpRegistryBuckets.
type fakeBucketAPI struct {
	// buckets is the instance's current listing.
	buckets []Bucket
	// listErrs are the errors of the next ListBuckets calls, in order; a nil entry, or none left, lists.
	listErrs []error
	// emptyErrs and deleteErrs map a bucket name to the errors of its next EmptyBucket and DeleteBucket calls.
	emptyErrs, deleteErrs map[string][]error
	// stuck are buckets that a successful or 404 DeleteBucket leaves in the listing.
	stuck map[string]bool
	// calls records every call, for example "list" and "delete roks-...".
	calls []string
}

var _ BucketAPI = &fakeBucketAPI{}

// pop removes and returns the first error of errs[name] (nil when none is left).
func pop(errs map[string][]error, name string) error {
	if len(errs[name]) == 0 {
		return nil
	}
	err := errs[name][0]
	errs[name] = errs[name][1:]
	return err
}

func (f *fakeBucketAPI) ListBuckets(context.Context) ([]Bucket, error) {
	f.calls = append(f.calls, "list")
	if len(f.listErrs) > 0 {
		err := f.listErrs[0]
		f.listErrs = f.listErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return slices.Clone(f.buckets), nil
}

func (f *fakeBucketAPI) EmptyBucket(_ context.Context, bucket Bucket) error {
	f.calls = append(f.calls, "empty "+bucket.Name)
	return pop(f.emptyErrs, bucket.Name)
}

func (f *fakeBucketAPI) DeleteBucket(_ context.Context, bucket Bucket) error {
	f.calls = append(f.calls, "delete "+bucket.Name)
	err := pop(f.deleteErrs, bucket.Name)
	if (err == nil || IsNotFound(err)) && !f.stuck[bucket.Name] {
		f.buckets = slices.DeleteFunc(f.buckets, func(b Bucket) bool { return b.Name == bucket.Name })
	}
	return err
}

// apiError returns an APIError with the status code, as BucketService returns it.
func apiError(operation string, status int) error {
	return &APIError{Operation: operation, StatusCode: status, Message: http.StatusText(status)}
}

func TestCleanUpRegistryBuckets(t *testing.T) {
	lane, err := NewLaneScope(DefaultNamePrefix, laneRunID, testVPCID)
	if err != nil {
		t.Fatalf("building the lane scope: %v", err)
	}
	own := Bucket{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"}
	others := Bucket{Name: otherRegistryBucket, LocationConstraint: "eu-de-smart"}
	stranger := Bucket{Name: "zz-new-by-someone", LocationConstraint: "eu-de-standard"}
	shared := Bucket{Name: sharedBucket, LocationConstraint: "eu-de-standard"}
	ownDeleted := []string{"list", "empty " + own.Name, "delete " + own.Name, "list"}

	testCases := []struct {
		name string
		api  *fakeBucketAPI
		opts RegistryBucketCleanup
		// wantCalls are the expected calls, in order.
		wantCalls []string
		// wantReport is the expected report, without the errors of Failed, which wantErr checks.
		wantReport BucketCleanupReport
		// wantFailed are the names of the buckets in the report's Failed.
		wantFailed []string
		// wantErr are substrings of the error; none means no error.
		wantErr []string
	}{
		{
			name:       "deletes the run's registry bucket and checks that IBM no longer lists it",
			api:        &fakeBucketAPI{buckets: []Bucket{shared, own}},
			opts:       RegistryBucketCleanup{Baseline: []Bucket{shared}, RunClusterIDs: []string{testClusterID}, Attempts: 3},
			wantCalls:  ownDeleted,
			wantReport: BucketCleanupReport{Deleted: []Bucket{own}},
		},
		{
			name: "account scope: fails on every other new bucket, naming it, and never touches it",
			api:  &fakeBucketAPI{buckets: []Bucket{shared, stranger, own, others}},
			opts: RegistryBucketCleanup{Baseline: []Bucket{shared}, RunClusterIDs: []string{testClusterID}, Attempts: 3},
			// The run's bucket is deleted although the others fail the cleanup.
			wantCalls:  ownDeleted,
			wantReport: BucketCleanupReport{Deleted: []Bucket{own}, Leftovers: []Bucket{others, stranger}},
			wantErr:    []string{others.Name, stranger.Name, "left untouched"},
		},
		{
			name:       "lane scope: other new buckets are not the lane's; reported, untouched, without an error",
			api:        &fakeBucketAPI{buckets: []Bucket{shared, stranger, own, others}},
			opts:       RegistryBucketCleanup{Baseline: []Bucket{shared}, RunClusterIDs: []string{testClusterID}, Scope: lane, Attempts: 3},
			wantCalls:  ownDeleted,
			wantReport: BucketCleanupReport{Deleted: []Bucket{own}, NotAttributed: []Bucket{others, stranger}},
		},
		{
			name:       "lane scope, no run cluster ID: the run's own registry bucket is reported, untouched",
			api:        &fakeBucketAPI{buckets: []Bucket{shared, own}},
			opts:       RegistryBucketCleanup{Baseline: []Bucket{shared}, Scope: lane, Attempts: 3},
			wantCalls:  []string{"list"},
			wantReport: BucketCleanupReport{NotAttributed: []Bucket{own}},
		},
		{
			name:      "no run cluster ID: a registry-looking bucket is a leftover and stays",
			api:       &fakeBucketAPI{buckets: []Bucket{shared, own}},
			opts:      RegistryBucketCleanup{Baseline: []Bucket{shared}, Attempts: 3},
			wantCalls: []string{"list"}, wantReport: BucketCleanupReport{Leftovers: []Bucket{own}},
			wantErr: []string{own.Name},
		},
		{
			name: "retries a failed attempt",
			api: &fakeBucketAPI{buckets: []Bucket{own},
				emptyErrs: map[string][]error{own.Name: {apiError("ListObjectsV2", http.StatusServiceUnavailable)}}},
			opts:       RegistryBucketCleanup{RunClusterIDs: []string{testClusterID}, Attempts: 3, Wait: time.Millisecond},
			wantCalls:  []string{"list", "empty " + own.Name, "empty " + own.Name, "delete " + own.Name, "list"},
			wantReport: BucketCleanupReport{Deleted: []Bucket{own}},
		},
		{
			name: "gives up after the attempts and reports the last error",
			api: &fakeBucketAPI{buckets: []Bucket{own}, deleteErrs: map[string][]error{own.Name: {
				apiError("DeleteBucket", http.StatusConflict), apiError("DeleteBucket", http.StatusConflict),
			}}},
			opts: RegistryBucketCleanup{RunClusterIDs: []string{testClusterID}, Attempts: 2},
			wantCalls: []string{"list", "empty " + own.Name, "delete " + own.Name, "empty " + own.Name,
				"delete " + own.Name},
			wantFailed: []string{own.Name},
			wantErr:    []string{own.Name, "gave up after 2 attempts", "deleting it", "HTTP 409"},
		},
		{
			name: "a 404 is not trusted while IBM still lists the bucket",
			api: &fakeBucketAPI{buckets: []Bucket{own}, stuck: map[string]bool{own.Name: true},
				deleteErrs: map[string][]error{own.Name: {apiError("DeleteBucket", http.StatusNotFound)}}},
			opts:       RegistryBucketCleanup{RunClusterIDs: []string{testClusterID}, Attempts: 1},
			wantCalls:  ownDeleted,
			wantFailed: []string{own.Name},
			wantErr:    []string{own.Name, "IBM still lists it", `"eu-de-smart"`},
		},
		{
			name: "a bucket that is already gone counts as deleted once IBM no longer lists it",
			api: &fakeBucketAPI{buckets: []Bucket{own},
				emptyErrs:  map[string][]error{own.Name: {apiError("GetBucketVersioning", http.StatusNotFound)}},
				deleteErrs: map[string][]error{own.Name: {apiError("DeleteBucket", http.StatusNotFound)}}},
			opts:       RegistryBucketCleanup{RunClusterIDs: []string{testClusterID}, Attempts: 1},
			wantCalls:  ownDeleted,
			wantReport: BucketCleanupReport{Deleted: []Bucket{own}},
		},
		{
			name: "a failed check of the deletion is retried",
			api: &fakeBucketAPI{buckets: []Bucket{own},
				listErrs: []error{nil, apiError("ListBuckets", http.StatusInternalServerError)}},
			opts: RegistryBucketCleanup{RunClusterIDs: []string{testClusterID}, Attempts: 2},
			wantCalls: []string{"list", "empty " + own.Name, "delete " + own.Name, "list", "empty " + own.Name,
				"delete " + own.Name, "list"},
			wantReport: BucketCleanupReport{Deleted: []Bucket{own}},
		},
		{
			name:      "a failed listing deletes nothing",
			api:       &fakeBucketAPI{buckets: []Bucket{own}, listErrs: []error{apiError("ListBuckets", http.StatusForbidden)}},
			opts:      RegistryBucketCleanup{RunClusterIDs: []string{testClusterID}, Attempts: 3},
			wantCalls: []string{"list"},
			wantErr:   []string{"listing the buckets", "HTTP 403"},
		},
		{
			name:    "invalid options send no request",
			api:     &fakeBucketAPI{buckets: []Bucket{own}},
			opts:    RegistryBucketCleanup{RunClusterIDs: []string{testClusterID}, Wait: -time.Second},
			wantErr: []string{"Attempts is 0", "must be at least 1", "Wait is -1s"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			report, err := CleanUpRegistryBuckets(context.Background(), tc.api, tc.opts)
			if len(tc.wantErr) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
			} else {
				for _, want := range tc.wantErr {
					g.Expect(err).To(MatchError(ContainSubstring(want)))
				}
			}
			g.Expect(tc.api.calls).To(Equal(tc.wantCalls))
			var failed []string
			for _, f := range report.Failed {
				g.Expect(f.Err).To(HaveOccurred())
				failed = append(failed, f.Bucket.Name)
			}
			g.Expect(failed).To(Equal(tc.wantFailed))
			report.Failed = nil
			g.Expect(report).To(Equal(tc.wantReport))
		})
	}
}

func TestCleanUpRegistryBucketsStopsWhenTheContextEnds(t *testing.T) {
	g := NewWithT(t)
	own := Bucket{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"}
	api := &fakeBucketAPI{buckets: []Bucket{own},
		emptyErrs: map[string][]error{own.Name: {apiError("ListObjectsV2", http.StatusServiceUnavailable)}}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	report, err := CleanUpRegistryBuckets(ctx, api, RegistryBucketCleanup{
		RunClusterIDs: []string{testClusterID}, Attempts: 3, Wait: time.Hour,
	})
	g.Expect(time.Since(start)).To(BeNumerically("<", time.Minute), "the wait between attempts ends with the context")
	g.Expect(err).To(MatchError(And(ContainSubstring("stopped after 1 of 3 attempts"), ContainSubstring("HTTP 503"))))
	g.Expect(err).To(MatchError(context.DeadlineExceeded))
	g.Expect(report.Failed).To(HaveLen(1))
	g.Expect(api.calls).To(Equal([]string{"list", "empty " + own.Name}))
}

func TestBucketCleanupReportString(t *testing.T) {
	g := NewWithT(t)
	g.Expect(BucketCleanupReport{}.String()).To(Equal("no new bucket\n"))
	report := BucketCleanupReport{
		Deleted:   []Bucket{{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"}},
		Failed:    []FailedBucketDeletion{{Bucket: Bucket{Name: otherRegistryBucket, LocationConstraint: "us-south-smart"}, Err: apiError("DeleteBucket", http.StatusConflict)}},
		Leftovers: []Bucket{{Name: "zz-new-by-someone", LocationConstraint: "eu-de-standard"}},
	}
	g.Expect(report.String()).To(Equal(
		"deleted registry bucket " + runRegistryBucket + " (eu-de-smart)\n" +
			"NOT deleted: registry bucket " + otherRegistryBucket + " (us-south-smart): DeleteBucket: IBM Cloud answered HTTP 409: Conflict\n" +
			"leftover bucket zz-new-by-someone (eu-de-standard), left untouched: its name does not start with roks-<ID of a cluster of this run>-\n"))

	// The lane scope's report: the buckets it does not attribute to the run are named, so that none leaks unseen.
	laneReport := BucketCleanupReport{NotAttributed: []Bucket{{Name: otherRegistryBucket, LocationConstraint: "us-south-smart"}}}
	g.Expect(laneReport.String()).To(Equal(
		"new bucket " + otherRegistryBucket + " (us-south-smart), not attributed to this run, left untouched: its name " +
			"does not start with roks-<ID of a cluster of this run>-; if it is the registry bucket of a cluster of this " +
			"run, delete it by hand\n"))
}
