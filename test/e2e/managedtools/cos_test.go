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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestNewBucketServiceRejectsInvalidOptions(t *testing.T) {
	fake := newFakeCOS(t)
	testCases := []struct {
		name    string
		mutate  func(*BucketServiceOptions)
		wantErr []string
	}{
		{name: "nil authenticator", mutate: func(o *BucketServiceOptions) { o.Authenticator = nil },
			wantErr: []string{"Cloud Object Storage client: the authenticator is nil"}},
		{name: "nil HTTP client", mutate: func(o *BucketServiceOptions) { o.HTTPClient = nil },
			wantErr: []string{"Cloud Object Storage client: the HTTP client is nil"}},
		{name: "not a COS instance CRN", mutate: func(o *BucketServiceOptions) { o.InstanceCRN = "11111111-2222" },
			wantErr: []string{`COS_INSTANCE_CRN "11111111-2222" is not the CRN of a Cloud Object Storage instance`}},
		{name: "empty listing location", mutate: func(o *BucketServiceOptions) { o.ListLocation = "" },
			wantErr: []string{`the listing location "" is not a COS location`}},
		{name: "listing location that is a host name", mutate: func(o *BucketServiceOptions) { o.ListLocation = "evil.example.com" },
			wantErr: []string{`the listing location "evil.example.com" is not a COS location`}},
		{
			name: "every invalid field at once",
			mutate: func(o *BucketServiceOptions) {
				*o = BucketServiceOptions{}
			},
			wantErr: []string{"the authenticator is nil", "the HTTP client is nil", "is not the CRN", "is not a COS location"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			opts := fake.options("eu-de")
			tc.mutate(&opts)
			service, err := NewBucketService(opts)
			g.Expect(service).To(BeNil())
			for _, want := range tc.wantErr {
				g.Expect(err).To(MatchError(ContainSubstring(want)))
			}
		})
	}
}

func TestBucketServiceListBuckets(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t, "eu-de", "us-south")
	created := time.Date(2026, 11, 16, 3, 5, 33, 0, time.UTC)
	fake.add(runRegistryBucket, &fakeBucket{constraint: "us-south-smart", created: created})
	fake.add(sharedBucket, &fakeBucket{constraint: "eu-de-standard", created: created})
	fake.add("zz-cross-region", &fakeBucket{constraint: "us-standard", created: created})
	fake.pageSize = 2

	buckets, err := fake.service("eu-de").ListBuckets(context.Background())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(buckets).To(Equal([]Bucket{
		{Name: runRegistryBucket, LocationConstraint: "us-south-smart", Created: created},
		{Name: sharedBucket, LocationConstraint: "eu-de-standard", Created: created},
		{Name: "zz-cross-region", LocationConstraint: "us-standard", Created: created},
	}), "every bucket of the instance, whatever its location, from both pages")

	requests := fake.recorded()
	g.Expect(requests).To(HaveLen(2))
	for _, r := range requests {
		g.Expect(r.Location).To(Equal("eu-de"), "the listings go to the endpoint of the listing location")
		g.Expect(r.Method).To(Equal(http.MethodGet))
		g.Expect(r.Path).To(Equal("/"))
		g.Expect(r.Header.Get("ibm-service-instance-id")).To(Equal(testCOSInstanceCRN))
	}
	second, err := url.ParseQuery(requests[1].Query)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(second.Get("marker")).To(Equal(sharedBucket), "the second page starts after the first page's last bucket")
}

func TestBucketServiceListBucketsError(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t, "eu-de")
	fake.listStatus = http.StatusForbidden
	_, err := fake.service("eu-de").ListBuckets(context.Background())
	var apiErr *APIError
	g.Expect(errors.As(err, &apiErr)).To(BeTrue(), "error %v", err)
	g.Expect(apiErr.Operation).To(Equal("ListBuckets"))
	g.Expect(apiErr.StatusCode).To(Equal(http.StatusForbidden))
	g.Expect(apiErr.Message).To(ContainSubstring("Forbidden"))
	g.Expect(IsNotFound(err)).To(BeFalse())
}

func TestBucketServiceEmptiesAndDeletesAnUnversionedBucket(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t, "eu-de", "us-south")
	fake.add(runRegistryBucket, &fakeBucket{
		constraint: "us-south-smart",
		objects: []string{"docker/registry/v2/blobs/sha256/aa/1", "docker/registry/v2/blobs/sha256/bb/2",
			"docker/registry/v2/repositories/app/_layers/3", "docker/registry/v2/repositories/app/_manifests/4", "z"},
		uploads: []fakeUpload{{Key: "docker/registry/v2/uploads/1", UploadID: "u1"}, {Key: "docker/registry/v2/uploads/2", UploadID: "u2"}},
	})
	fake.pageSize = 2
	service := fake.service("eu-de")
	bucket := Bucket{Name: runRegistryBucket, LocationConstraint: "us-south-smart"}

	g.Expect(service.EmptyBucket(context.Background(), bucket)).To(Succeed())
	left := fake.bucket(runRegistryBucket)
	g.Expect(left.objects).To(BeEmpty(), "every object of every page is deleted")
	g.Expect(left.uploads).To(BeEmpty(), "every incomplete multipart upload is aborted")

	g.Expect(service.DeleteBucket(context.Background(), bucket)).To(Succeed())
	g.Expect(fake.bucket(runRegistryBucket)).To(BeNil())

	var deleteObjects, listObjects int
	for _, r := range fake.recorded() {
		g.Expect(r.Location).To(Equal("us-south"), "%s %s?%s goes to the endpoint of the bucket's location", r.Method, r.Path, r.Query)
		query, err := url.ParseQuery(r.Query)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(query.Has("versions")).To(BeFalse(), "an unversioned bucket is emptied through its objects")
		switch {
		case r.Method == http.MethodPost && query.Has("delete"):
			deleteObjects++
		case r.Method == http.MethodGet && query.Get("list-type") == "2":
			listObjects++
		}
	}
	g.Expect(listObjects).To(Equal(3), "5 objects in pages of 2")
	g.Expect(deleteObjects).To(Equal(3), "one DeleteObjects request per page")

	err := service.DeleteBucket(context.Background(), bucket)
	g.Expect(IsNotFound(err)).To(BeTrue(), "a deleted bucket answers 404: %v", err)
}

func TestBucketServiceEmptiesAVersionedBucketThroughItsVersions(t *testing.T) {
	for _, status := range []string{"Enabled", "Suspended"} {
		t.Run(status, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeCOS(t, "eu-de")
			versions := make([]fakeVersion, 0, 1500)
			for i := range 1500 {
				versions = append(versions, fakeVersion{Key: fmt.Sprintf("key-%04d", i), VersionID: "v1", DeleteMarker: i%3 == 0})
			}
			fake.add(runRegistryBucket, &fakeBucket{constraint: "eu-de-smart", versioning: status, versions: versions})
			fake.pageSize = 1500
			bucket := Bucket{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"}

			service := fake.service("eu-de")
			g.Expect(service.EmptyBucket(context.Background(), bucket)).To(Succeed())
			g.Expect(fake.bucket(runRegistryBucket).versions).To(BeEmpty(), "every version and delete marker is deleted")
			g.Expect(service.DeleteBucket(context.Background(), bucket)).To(Succeed())

			var batches []int
			for _, r := range fake.recorded() {
				g.Expect(r.Query).NotTo(ContainSubstring("list-type=2"), "a versioned bucket is emptied through its versions")
				if r.Method == http.MethodPost {
					batches = append(batches, r.Objects)
				}
			}
			g.Expect(batches).To(Equal([]int{1000, 500}), "a page of 1500 entries is deleted in requests of at most 1000")
		})
	}
}

func TestBucketServiceDeleteBucketThatIsNotEmpty(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t, "eu-de")
	fake.add(runRegistryBucket, &fakeBucket{constraint: "eu-de-smart", objects: []string{"left"}})
	err := fake.service("eu-de").DeleteBucket(context.Background(), Bucket{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"})
	var apiErr *APIError
	g.Expect(errors.As(err, &apiErr)).To(BeTrue(), "error %v", err)
	g.Expect(apiErr.StatusCode).To(Equal(http.StatusConflict))
	g.Expect(apiErr.Message).To(ContainSubstring("BucketNotEmpty"))
	g.Expect(fake.bucket(runRegistryBucket)).NotTo(BeNil())
}

func TestBucketServiceReportsObjectsIBMKept(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t, "eu-de")
	fake.add(runRegistryBucket, &fakeBucket{constraint: "eu-de-smart", objects: []string{"kept", "gone"}})
	fake.keptKeys["kept"] = true
	err := fake.service("eu-de").EmptyBucket(context.Background(), Bucket{Name: runRegistryBucket, LocationConstraint: "eu-de-smart"})
	g.Expect(err).To(MatchError(And(ContainSubstring("IBM kept 1 of 2 objects"), ContainSubstring(`"kept"`),
		ContainSubstring("AccessDenied"))))
	g.Expect(fake.bucket(runRegistryBucket).objects).To(Equal([]string{"kept"}))
}

func TestBucketServiceRefusesOtherBucketsWithoutARequest(t *testing.T) {
	fake := newFakeCOS(t, "eu-de")
	fake.add(sharedBucket, &fakeBucket{constraint: "eu-de-standard", objects: []string{"precious"}})
	testCases := []struct {
		name    string
		bucket  Bucket
		wantErr string
	}{
		{name: "not a registry bucket", bucket: Bucket{Name: sharedBucket, LocationConstraint: "eu-de-standard"},
			wantErr: "empties and deletes only the registry buckets of ROKS clusters"},
		{name: "unknown location", bucket: Bucket{Name: runRegistryBucket, LocationConstraint: ""},
			wantErr: "<location>-<storage class>"},
		{name: "location that is a host name", bucket: Bucket{Name: runRegistryBucket, LocationConstraint: "evil.example.com-standard"},
			wantErr: "<location>-<storage class>"},
	}
	service := fake.service("eu-de")
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(service.EmptyBucket(context.Background(), tc.bucket)).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(service.DeleteBucket(context.Background(), tc.bucket)).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
	NewWithT(t).Expect(fake.recorded()).To(BeEmpty())
	NewWithT(t).Expect(fake.bucket(sharedBucket).objects).To(Equal([]string{"precious"}))
}

// failingAuthenticator is a core.Authenticator whose every Authenticate call fails, as an IAM authenticator does when
// IAM refuses the API key.
type failingAuthenticator struct{}

func (failingAuthenticator) AuthenticationType() string { return "iam" }

func (failingAuthenticator) Authenticate(*http.Request) error {
	return errors.New("IAM refused the API key")
}

func (failingAuthenticator) Validate() error { return nil }

func TestBucketServiceAuthenticatorError(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t, "eu-de")
	opts := fake.options("eu-de")
	opts.Authenticator = failingAuthenticator{}
	service, err := NewBucketService(opts)
	g.Expect(err).NotTo(HaveOccurred())
	_, err = service.ListBuckets(context.Background())
	g.Expect(err).To(MatchError(And(ContainSubstring("ListBuckets"), ContainSubstring("authenticating the request"),
		ContainSubstring("IAM refused the API key"))))
	g.Expect(fake.recorded()).To(BeEmpty(), "an unsigned request is never sent")
}

func TestBucketServiceWithoutAnAnswer(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t)
	opts := fake.options("eu-de")
	// A listener that is closed at once: the request gets no answer.
	opts.EndpointURL = func(string) string { return "http://127.0.0.1:1" }
	service, err := NewBucketService(opts)
	g.Expect(err).NotTo(HaveOccurred())
	_, err = service.ListBuckets(context.Background())
	var apiErr *APIError
	g.Expect(errors.As(err, &apiErr)).To(BeTrue(), "error %v", err)
	g.Expect(apiErr.StatusCode).To(BeZero())
	g.Expect(err).To(MatchError(ContainSubstring("ListBuckets: no answer from IBM Cloud")))
}

func TestCleanUpRegistryBucketsWithBucketService(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeCOS(t, "eu-de", "us-south")
	fake.add(sharedBucket, &fakeBucket{constraint: "eu-de-standard", objects: []string{"precious"}})
	service := fake.service("eu-de")
	baseline, err := service.ListBuckets(context.Background())
	g.Expect(err).NotTo(HaveOccurred())

	// During the run IBM creates the registry bucket of the run's cluster, and someone else a bucket of their own.
	fake.add(runRegistryBucket, &fakeBucket{constraint: "us-south-smart", objects: []string{"docker/registry/v2/blobs/1"},
		uploads: []fakeUpload{{Key: "docker/registry/v2/uploads/1", UploadID: "u1"}}})
	fake.add(otherRegistryBucket, &fakeBucket{constraint: "eu-de-smart", objects: []string{"theirs"}})

	report, err := CleanUpRegistryBuckets(context.Background(), service, RegistryBucketCleanup{
		Baseline: baseline, RunClusterIDs: []string{testClusterID}, Scope: AccountScope(), Attempts: 3,
	})
	g.Expect(err).To(MatchError(ContainSubstring(otherRegistryBucket)), "the other new bucket fails the account scope")
	g.Expect(report.Deleted).To(HaveLen(1))
	g.Expect(report.Deleted[0].Name).To(Equal(runRegistryBucket))
	g.Expect(fake.bucket(runRegistryBucket)).To(BeNil())
	g.Expect(fake.bucket(otherRegistryBucket).objects).To(Equal([]string{"theirs"}))
	g.Expect(fake.bucket(sharedBucket).objects).To(Equal([]string{"precious"}))
	for _, r := range fake.recorded() {
		g.Expect(r.Path).To(Or(Equal("/"), HavePrefix("/"+runRegistryBucket)),
			"no request names a bucket other than the run's registry bucket")
	}
}
