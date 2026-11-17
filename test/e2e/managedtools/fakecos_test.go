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
	"cmp"
	"crypto/md5" //nolint:gosec // G501: IBM's object storage API requires Content-MD5 on DeleteObjects; the fake checks it.
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
)

// fakeCOS is a set of httptest servers, one per COS location, that answer the object storage calls of BucketService
// from one shared in-memory instance and record every request. The bucket listing answers on every server with every
// bucket, as IBM's does; every other request answers only for the buckets of the server's own location and 404
// NoSuchBucket for the others, so that a test sees whether a request went to the endpoint of the bucket's location.
// The servers speak plain HTTP: the client under test takes its endpoints from EndpointURL, whatever their scheme.
type fakeCOS struct {
	t *testing.T
	// servers maps a location to its server.
	servers map[string]*httptest.Server

	mu       sync.Mutex
	requests []cosRequest
	// buckets maps a bucket name to the bucket.
	buckets map[string]*fakeBucket
	// pageSize is the size of every listed page (buckets, objects, versions); 0 means 1000, IBM's default.
	pageSize int
	// listStatus, when not 0, is the status of every bucket listing.
	listStatus int
	// keptKeys are object keys that DeleteObjects reports as not deleted (AccessDenied) and keeps.
	keptKeys map[string]bool
}

// cosRequest is one request a fakeCOS server received.
type cosRequest struct {
	// Location is the location of the server that received it.
	Location string
	Method   string
	Path     string
	Query    string
	Header   http.Header
	// Objects is the number of objects a DeleteObjects request named.
	Objects int
}

// fakeBucket is one bucket of the fake instance.
type fakeBucket struct {
	constraint string
	created    time.Time
	// versioning is "", "Enabled" or "Suspended".
	versioning string
	// objects are the keys of an unversioned bucket.
	objects []string
	// versions are the object versions and delete markers of a bucket with versioning.
	versions []fakeVersion
	// uploads are the incomplete multipart uploads.
	uploads []fakeUpload
}

// fakeVersion is one object version or delete marker.
type fakeVersion struct {
	Key, VersionID string
	DeleteMarker   bool
}

// fakeUpload is one incomplete multipart upload.
type fakeUpload struct {
	Key, UploadID string
}

// newFakeCOS starts one server per location and stops them when the test ends.
//
// Parameters:
//   - t is the test.
//   - locations are the COS locations to serve, for example "eu-de" and "us-south".
func newFakeCOS(t *testing.T, locations ...string) *fakeCOS {
	t.Helper()
	f := &fakeCOS{t: t, servers: map[string]*httptest.Server{}, buckets: map[string]*fakeBucket{},
		keptKeys: map[string]bool{}}
	for _, location := range locations {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.serve(location, w, r)
		}))
		t.Cleanup(server.Close)
		f.servers[location] = server
	}
	return f
}

// endpointURL is the EndpointURL of the clients under test.
//
// Parameters:
//   - location is the location the client asks for.
func (f *fakeCOS) endpointURL(location string) string {
	server, ok := f.servers[location]
	if !ok {
		f.t.Errorf("the client asked for the endpoint of location %q, which the fake does not serve", location)
		return "http://127.0.0.1:1"
	}
	return server.URL
}

// options returns BucketServiceOptions for the fake: the test CRN, listings on listLocation, a bearer-token
// authenticator and a plain HTTP client.
//
// Parameters:
//   - listLocation is the location of the bucket listings.
func (f *fakeCOS) options(listLocation string) BucketServiceOptions {
	auth, err := core.NewBearerTokenAuthenticator(testToken)
	if err != nil {
		f.t.Fatalf("building the test authenticator: %v", err)
	}
	return BucketServiceOptions{InstanceCRN: testCOSInstanceCRN, ListLocation: listLocation, EndpointURL: f.endpointURL,
		Authenticator: auth, HTTPClient: &http.Client{Timeout: 10 * time.Second}}
}

// service returns a BucketService for the fake.
//
// Parameters:
//   - listLocation is the location of the bucket listings.
func (f *fakeCOS) service(listLocation string) *BucketService {
	service, err := NewBucketService(f.options(listLocation))
	if err != nil {
		f.t.Fatalf("building the bucket service: %v", err)
	}
	return service
}

// add puts a bucket into the instance.
//
// Parameters:
//   - name is the bucket name.
//   - bucket is its content; its constraint decides which server answers for it.
func (f *fakeCOS) add(name string, bucket *fakeBucket) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buckets[name] = bucket
}

// bucket returns a copy of the named bucket, or nil when it does not exist.
func (f *fakeCOS) bucket(name string) *fakeBucket {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.buckets[name]
	if !ok {
		return nil
	}
	copied := *b
	copied.objects = slices.Clone(b.objects)
	copied.versions = slices.Clone(b.versions)
	copied.uploads = slices.Clone(b.uploads)
	return &copied
}

// recorded returns a copy of the recorded requests.
func (f *fakeCOS) recorded() []cosRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

// serve routes one request of the server of location. The paths are path-style: "/" lists the buckets,
// "/<bucket>" addresses a bucket and "/<bucket>/<key>" an object.
func (f *fakeCOS) serve(location string, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		f.t.Errorf("reading the request body: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	recorded := cosRequest{Location: location, Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery,
		Header: r.Header.Clone()}
	defer func() { f.requests = append(f.requests, recorded) }()

	if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
		f.t.Errorf("%s %s: Authorization %q, want the bearer token of the caller's authenticator", r.Method, r.URL.Path, got)
		writeCOSError(w, http.StatusForbidden, "AccessDenied")
		return
	}
	if ua := r.Header.Get("User-Agent"); !strings.Contains(ua, DefaultUserAgent) {
		f.t.Errorf("%s %s: User-Agent %q does not contain %q", r.Method, r.URL.Path, ua, DefaultUserAgent)
	}
	if r.URL.Path == "/" {
		f.serveListing(w, r)
		return
	}
	name, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	bucket, ok := f.buckets[name]
	if !ok || !strings.HasPrefix(bucket.constraint, location+"-") {
		writeCOSError(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	query := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && key == "" && query.Has("versioning"):
		writeXML(w, xmlVersioning{Status: bucket.versioning})
	case r.Method == http.MethodGet && key == "" && query.Get("list-type") == "2":
		f.serveObjects(w, bucket, query.Get("continuation-token"))
	case r.Method == http.MethodGet && key == "" && query.Has("versions"):
		f.serveVersions(w, bucket, query.Get("key-marker"), query.Get("version-id-marker"))
	case r.Method == http.MethodGet && key == "" && query.Has("uploads"):
		result := xmlUploads{}
		for _, upload := range bucket.uploads {
			result.Uploads = append(result.Uploads, xmlUpload(upload))
		}
		writeXML(w, result)
	case r.Method == http.MethodPost && key == "" && query.Has("delete"):
		recorded.Objects = f.deleteObjects(w, r, bucket, body)
	case r.Method == http.MethodDelete && key != "" && query.Has("uploadId"):
		uploads := slices.DeleteFunc(slices.Clone(bucket.uploads), func(u fakeUpload) bool {
			return u.Key == key && u.UploadID == query.Get("uploadId")
		})
		if len(uploads) == len(bucket.uploads) {
			writeCOSError(w, http.StatusNotFound, "NoSuchUpload")
			return
		}
		bucket.uploads = uploads
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete && key == "":
		if len(bucket.objects)+len(bucket.versions)+len(bucket.uploads) > 0 {
			writeCOSError(w, http.StatusConflict, "BucketNotEmpty")
			return
		}
		delete(f.buckets, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		f.t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		writeCOSError(w, http.StatusNotImplemented, "NotImplemented")
	}
}

// serveListing answers the extended bucket listing with every bucket of the instance, sorted by name, one page after
// the marker. The caller holds f.mu.
func (f *fakeCOS) serveListing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !r.URL.Query().Has("extended") {
		f.t.Errorf("unexpected request %s /?%s; the client lists buckets only with ?extended", r.Method, r.URL.RawQuery)
		writeCOSError(w, http.StatusNotImplemented, "NotImplemented")
		return
	}
	if got := r.Header.Get("ibm-service-instance-id"); got != testCOSInstanceCRN {
		f.t.Errorf("bucket listing for instance %q, want %q", got, testCOSInstanceCRN)
		writeCOSError(w, http.StatusBadRequest, "InvalidArgument")
		return
	}
	if f.listStatus != 0 {
		writeCOSError(w, f.listStatus, http.StatusText(f.listStatus))
		return
	}
	names := make([]string, 0, len(f.buckets))
	for name := range f.buckets {
		names = append(names, name)
	}
	slices.Sort(names)
	marker := r.URL.Query().Get("marker")
	names = slices.DeleteFunc(names, func(name string) bool { return name <= marker })
	page, truncated := f.page(len(names))
	result := xmlBucketList{IsTruncated: truncated}
	for _, name := range names[:page] {
		bucket := f.buckets[name]
		result.Buckets = append(result.Buckets, xmlBucket{Name: name, LocationConstraint: bucket.constraint,
			CreationDate: bucket.created.UTC().Format(time.RFC3339)})
	}
	writeXML(w, result)
}

// serveObjects answers one page of an unversioned bucket's objects after the continuation token, which is the last key
// of the previous page. The caller holds f.mu.
func (f *fakeCOS) serveObjects(w http.ResponseWriter, bucket *fakeBucket, token string) {
	keys := slices.DeleteFunc(slices.Sorted(slices.Values(bucket.objects)), func(key string) bool {
		return token != "" && key <= token
	})
	page, truncated := f.page(len(keys))
	result := xmlObjectList{IsTruncated: truncated, KeyCount: page}
	for _, key := range keys[:page] {
		result.Contents = append(result.Contents, xmlObject{Key: key})
	}
	if truncated {
		result.NextContinuationToken = keys[page-1]
	}
	writeXML(w, result)
}

// serveVersions answers one page of a bucket's object versions and delete markers after the key and version markers.
// The caller holds f.mu.
func (f *fakeCOS) serveVersions(w http.ResponseWriter, bucket *fakeBucket, keyMarker, versionMarker string) {
	versions := slices.SortedFunc(slices.Values(bucket.versions), func(a, b fakeVersion) int {
		return cmp.Or(cmp.Compare(a.Key, b.Key), cmp.Compare(a.VersionID, b.VersionID))
	})
	versions = slices.DeleteFunc(versions, func(v fakeVersion) bool {
		return keyMarker != "" && cmp.Or(cmp.Compare(v.Key, keyMarker), cmp.Compare(v.VersionID, versionMarker)) <= 0
	})
	page, truncated := f.page(len(versions))
	result := xmlVersionList{IsTruncated: truncated}
	for _, v := range versions[:page] {
		entry := xmlVersion{Key: v.Key, VersionID: v.VersionID}
		if v.DeleteMarker {
			result.DeleteMarkers = append(result.DeleteMarkers, entry)
		} else {
			result.Versions = append(result.Versions, entry)
		}
	}
	if truncated {
		result.NextKeyMarker, result.NextVersionIDMarker = versions[page-1].Key, versions[page-1].VersionID
	}
	writeXML(w, result)
}

// deleteObjects answers a DeleteObjects request and returns the number of objects it named. It checks the Content-MD5
// header that IBM requires. The caller holds f.mu.
func (f *fakeCOS) deleteObjects(w http.ResponseWriter, r *http.Request, bucket *fakeBucket, body []byte) int {
	sum := md5.Sum(body) //nolint:gosec // G401: the checksum IBM's API defines, not a security measure.
	if got, want := r.Header.Get("Content-MD5"), base64.StdEncoding.EncodeToString(sum[:]); got != want {
		f.t.Errorf("DeleteObjects: Content-MD5 %q, want %q", got, want)
		writeCOSError(w, http.StatusBadRequest, "InvalidDigest")
		return 0
	}
	var request xmlDelete
	if err := xml.Unmarshal(body, &request); err != nil {
		f.t.Errorf("DeleteObjects: decoding the body: %v", err)
		writeCOSError(w, http.StatusBadRequest, "MalformedXML")
		return 0
	}
	result := xmlDeleteResult{}
	for _, object := range request.Objects {
		if f.keptKeys[object.Key] {
			result.Errors = append(result.Errors, xmlDeleteError{Key: object.Key, VersionID: object.VersionID,
				Code: "AccessDenied", Message: "Access Denied"})
			continue
		}
		if object.VersionID == "" {
			bucket.objects = slices.DeleteFunc(bucket.objects, func(key string) bool { return key == object.Key })
			continue
		}
		bucket.versions = slices.DeleteFunc(bucket.versions, func(v fakeVersion) bool {
			return v.Key == object.Key && v.VersionID == object.VersionID
		})
	}
	writeXML(w, result)
	return len(request.Objects)
}

// page returns how many of available items fit on one page and whether more are left.
func (f *fakeCOS) page(available int) (int, bool) {
	size := cmp.Or(f.pageSize, 1000)
	if available > size {
		return size, true
	}
	return available, false
}

// The XML documents of the fake, in the shapes of IBM's object storage API: the bucket operations
// (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-compatibility-api-bucket-operations), the
// object operations for DeleteResult
// (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-object-operations) and versioning for
// VersioningConfiguration and ListVersionsResult
// (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-versioning). The SDK matches elements by
// their local names.
type (
	xmlBucketList struct {
		XMLName     xml.Name    `xml:"ListAllMyBucketsResult"`
		IsTruncated bool        `xml:"IsTruncated"`
		Buckets     []xmlBucket `xml:"Buckets>Bucket"`
	}
	xmlBucket struct {
		Name               string `xml:"Name"`
		CreationDate       string `xml:"CreationDate"`
		LocationConstraint string `xml:"LocationConstraint"`
	}
	xmlVersioning struct {
		XMLName xml.Name `xml:"VersioningConfiguration"`
		Status  string   `xml:"Status,omitempty"`
	}
	xmlObjectList struct {
		XMLName               xml.Name    `xml:"ListBucketResult"`
		IsTruncated           bool        `xml:"IsTruncated"`
		KeyCount              int         `xml:"KeyCount"`
		Contents              []xmlObject `xml:"Contents"`
		NextContinuationToken string      `xml:"NextContinuationToken,omitempty"`
	}
	xmlObject struct {
		Key string `xml:"Key"`
	}
	xmlVersionList struct {
		XMLName             xml.Name     `xml:"ListVersionsResult"`
		IsTruncated         bool         `xml:"IsTruncated"`
		NextKeyMarker       string       `xml:"NextKeyMarker,omitempty"`
		NextVersionIDMarker string       `xml:"NextVersionIdMarker,omitempty"`
		Versions            []xmlVersion `xml:"Version"`
		DeleteMarkers       []xmlVersion `xml:"DeleteMarker"`
	}
	xmlVersion struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	}
	xmlUploads struct {
		XMLName     xml.Name    `xml:"ListMultipartUploadsResult"`
		IsTruncated bool        `xml:"IsTruncated"`
		Uploads     []xmlUpload `xml:"Upload"`
	}
	xmlUpload struct {
		Key      string `xml:"Key"`
		UploadID string `xml:"UploadId"`
	}
	xmlDelete struct {
		Objects []xmlVersion `xml:"Object"`
	}
	xmlDeleteResult struct {
		XMLName xml.Name         `xml:"DeleteResult"`
		Errors  []xmlDeleteError `xml:"Error"`
	}
	xmlDeleteError struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId,omitempty"`
		Code      string `xml:"Code"`
		Message   string `xml:"Message"`
	}
	xmlError struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
)

// writeXML writes body as a 200 XML answer; a body that does not encode becomes a 500, which fails the request.
func writeXML(w http.ResponseWriter, body any) {
	data, err := xml.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(append([]byte(xml.Header), data...))
}

// writeCOSError writes an error answer in the form of IBM's object storage API.
func writeCOSError(w http.ResponseWriter, status int, code string) {
	data, _ := xml.Marshal(xmlError{Code: code, Message: http.StatusText(status)})
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write(append([]byte(xml.Header), data...))
}
