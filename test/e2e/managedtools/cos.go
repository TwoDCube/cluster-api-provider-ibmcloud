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
	"slices"
	"strings"
	"sync"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/awserr"
	"github.com/IBM/ibm-cos-sdk-go/aws/client"
	"github.com/IBM/ibm-cos-sdk-go/aws/corehandlers"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials"
	"github.com/IBM/ibm-cos-sdk-go/aws/defaults"
	"github.com/IBM/ibm-cos-sdk-go/aws/request"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
)

// cosService names the Cloud Object Storage client in error messages.
const cosService = "Cloud Object Storage"

// maxDeleteObjects is the most objects one DeleteObjects request may name.
const maxDeleteObjects = 1000

// BucketServiceOptions are the explicit inputs of NewBucketService. Nothing is read from the environment.
type BucketServiceOptions struct {
	// InstanceCRN is the CRN of the Cloud Object Storage instance (COSInstanceCRNVariable); every bucket listing sends
	// it as ibm-service-instance-id. Required; see ValidateCOSInstanceCRN.
	InstanceCRN string
	// ListLocation is the location whose endpoint receives the bucket listings, for example the lanes' region "eu-de".
	// Any location will do: IBM returns every bucket of the instance on a list request, "not just the subset that is
	// located in the region of the target endpoint"
	// (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-sdk-about#sdk-about-extended-list,
	// "Extended bucket listing"). Required.
	ListLocation string
	// EndpointURL returns the base URL of the endpoint of a location. Optional: nil selects IBM's public endpoints,
	// https://s3.<location>.cloud-object-storage.appdomain.cloud, which serve every region, cross-region geography and
	// single site (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-endpoints). The unit tests
	// point it at httptest servers.
	EndpointURL func(location string) string
	// Authenticator signs every request with an IAM token. Required; there is no fallback to the environment.
	Authenticator core.Authenticator
	// HTTPClient sends every request. Required, so that the caller chooses the timeout.
	HTTPClient *http.Client
}

// validate returns one error naming every invalid field of o and the accepted form.
func (o BucketServiceOptions) validate() error {
	errs := []error{ClientOptions{Authenticator: o.Authenticator, HTTPClient: o.HTTPClient}.validate(cosService)}
	if err := ValidateCOSInstanceCRN(o.InstanceCRN); err != nil {
		errs = append(errs, fmt.Errorf("%s client: %w", cosService, err))
	}
	if !cosLocationPattern.MatchString(o.ListLocation) {
		errs = append(errs, fmt.Errorf("%s client: the listing location %q is not a COS location; pass the lanes' "+
			"region, for example eu-de", cosService, o.ListLocation))
	}
	return errors.Join(errs...)
}

// BucketService implements BucketAPI on the object storage API of IBM Cloud Object Storage, with the IBM COS SDK that
// CAPIBM's own COS client wraps (pkg/cloud/services/cos). It keeps one SDK client per location, because every request
// about a bucket goes to the endpoint of the bucket's location. Create it with NewBucketService.
//
// It empties and deletes only buckets whose name starts with "roks-" (the registry buckets IBM creates for ROKS
// clusters) and refuses any other bucket without a request, so that a mistake elsewhere cannot make the e2e tooling
// empty another bucket of a shared instance.
type BucketService struct {
	// instanceCRN identifies the instance in every bucket listing.
	instanceCRN string
	// listLocation is the location whose endpoint receives the bucket listings.
	listLocation string
	// endpointURL returns the base URL of a location's endpoint.
	endpointURL func(location string) string
	// authenticator signs every request.
	authenticator core.Authenticator
	// httpClient sends every request.
	httpClient *http.Client

	// mu guards clients.
	mu sync.Mutex
	// clients holds the SDK client of each location used so far.
	clients map[string]*s3.S3
}

var _ BucketAPI = &BucketService{}

// NewBucketService validates opts and returns the client. It makes no request.
//
// Parameters:
//   - opts are the instance CRN, the listing location, the optional endpoint function, the authenticator and the HTTP
//     client; see BucketServiceOptions.
func NewBucketService(opts BucketServiceOptions) (*BucketService, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	endpointURL := opts.EndpointURL
	if endpointURL == nil {
		endpointURL = publicCOSEndpoint
	}
	return &BucketService{
		instanceCRN:   opts.InstanceCRN,
		listLocation:  opts.ListLocation,
		endpointURL:   endpointURL,
		authenticator: opts.Authenticator,
		httpClient:    opts.HTTPClient,
		clients:       map[string]*s3.S3{},
	}, nil
}

// publicCOSEndpoint returns the URL of IBM's public endpoint of a location.
//
// Parameters:
//   - location is a COS location, for example "us-south", "us" or "ams03".
func publicCOSEndpoint(location string) string {
	return "https://s3." + location + ".cloud-object-storage.appdomain.cloud"
}

// ListBuckets implements BucketLister with the extended bucket listing (GET /?extended), which returns each bucket's
// LocationConstraint, and follows every page.
func (s *BucketService) ListBuckets(ctx context.Context) ([]Bucket, error) {
	svc, err := s.client(s.listLocation)
	if err != nil {
		return nil, err
	}
	var buckets []Bucket
	input := &s3.ListBucketsExtendedInput{IBMServiceInstanceId: aws.String(s.instanceCRN)}
	err = svc.ListBucketsExtendedPagesWithContext(ctx, input, func(page *s3.ListBucketsExtendedOutput, _ bool) bool {
		for _, b := range page.Buckets {
			buckets = append(buckets, Bucket{
				Name:               aws.StringValue(b.Name),
				LocationConstraint: aws.StringValue(b.LocationConstraint),
				Created:            aws.TimeValue(b.CreationDate),
			})
		}
		return true
	})
	if err != nil {
		return nil, newCOSError("ListBuckets", err)
	}
	return buckets, nil
}

// EmptyBucket implements BucketAPI. A bucket with versioning enabled or suspended is emptied through its object
// versions, which include the delete markers; any other bucket through its objects, because deleting an object of a
// versioned bucket without its version ID would only add a delete marker. Then it aborts every incomplete multipart
// upload: an image registry uploads its layers in parts, and IBM refuses to delete a bucket that still holds an
// upload ("How do I delete a non-empty bucket when I do not see any objects in it?",
// https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-faq-bucket#faq-delete-nonempty-bucket).
//
// Parameters:
//   - ctx bounds the requests.
//   - bucket is a ListBuckets result whose name starts with "roks-".
func (s *BucketService) EmptyBucket(ctx context.Context, bucket Bucket) error {
	svc, err := s.bucketClient(bucket)
	if err != nil {
		return err
	}
	versioning, err := svc.GetBucketVersioningWithContext(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket.Name)})
	if err != nil {
		return newCOSError("GetBucketVersioning", err)
	}
	if aws.StringValue(versioning.Status) == "" {
		err = deleteObjects(ctx, svc, bucket.Name)
	} else {
		err = deleteObjectVersions(ctx, svc, bucket.Name)
	}
	if err != nil {
		return err
	}
	return abortMultipartUploads(ctx, svc, bucket.Name)
}

// DeleteBucket implements BucketAPI with DELETE /<bucket> on the endpoint of the bucket's location. IBM answers 409
// BucketNotEmpty for a bucket that is not empty.
//
// Parameters:
//   - ctx bounds the request.
//   - bucket is a ListBuckets result whose name starts with "roks-".
func (s *BucketService) DeleteBucket(ctx context.Context, bucket Bucket) error {
	svc, err := s.bucketClient(bucket)
	if err != nil {
		return err
	}
	if _, err := svc.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket.Name)}); err != nil {
		return newCOSError("DeleteBucket", err)
	}
	return nil
}

// deleteObjects deletes every object of an unversioned bucket, one DeleteObjects request per listed page. The listing
// goes on after each page's key, so deleting the listed keys does not make it skip any.
//
// Parameters:
//   - ctx bounds the requests.
//   - svc is the client of the bucket's location.
//   - bucket is the bucket name.
func deleteObjects(ctx context.Context, svc *s3.S3, bucket string) error {
	var deleteErr error
	input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	err := svc.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		ids := make([]*s3.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			ids = append(ids, &s3.ObjectIdentifier{Key: object.Key})
		}
		deleteErr = deleteObjectBatches(ctx, svc, bucket, ids)
		return deleteErr == nil
	})
	if err != nil {
		return newCOSError("ListObjectsV2", err)
	}
	return deleteErr
}

// deleteObjectVersions deletes every object version and delete marker of a bucket with versioning, one batch per
// listed page.
//
// Parameters:
//   - ctx bounds the requests.
//   - svc is the client of the bucket's location.
//   - bucket is the bucket name.
func deleteObjectVersions(ctx context.Context, svc *s3.S3, bucket string) error {
	var deleteErr error
	input := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)}
	err := svc.ListObjectVersionsPagesWithContext(ctx, input, func(page *s3.ListObjectVersionsOutput, _ bool) bool {
		ids := make([]*s3.ObjectIdentifier, 0, len(page.Versions)+len(page.DeleteMarkers))
		for _, version := range page.Versions {
			ids = append(ids, &s3.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
		}
		for _, marker := range page.DeleteMarkers {
			ids = append(ids, &s3.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
		}
		deleteErr = deleteObjectBatches(ctx, svc, bucket, ids)
		return deleteErr == nil
	})
	if err != nil {
		return newCOSError("ListObjectVersions", err)
	}
	return deleteErr
}

// deleteObjectBatches deletes ids with DeleteObjects requests of at most maxDeleteObjects objects each. The SDK adds
// the Content-MD5 header that IBM requires. An object IBM reports as not deleted fails the batch.
//
// Parameters:
//   - ctx bounds the requests.
//   - svc is the client of the bucket's location.
//   - bucket is the bucket name.
//   - ids are the objects (and versions) to delete.
func deleteObjectBatches(ctx context.Context, svc *s3.S3, bucket string, ids []*s3.ObjectIdentifier) error {
	for batch := range slices.Chunk(ids, maxDeleteObjects) {
		output, err := svc.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3.Delete{Objects: batch, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return newCOSError("DeleteObjects", err)
		}
		if len(output.Errors) > 0 {
			kept := make([]string, 0, len(output.Errors))
			for _, e := range output.Errors {
				kept = append(kept, fmt.Sprintf("%q (version %q): %s: %s", aws.StringValue(e.Key),
					aws.StringValue(e.VersionId), aws.StringValue(e.Code), aws.StringValue(e.Message)))
			}
			return &APIError{Operation: "DeleteObjects", StatusCode: http.StatusOK, Message: fmt.Sprintf(
				"IBM kept %d of %d objects: %s", len(output.Errors), len(batch), strings.Join(kept, "; "))}
		}
	}
	return nil
}

// abortMultipartUploads aborts every incomplete multipart upload of a bucket; an upload IBM no longer knows (404) is
// gone already.
//
// Parameters:
//   - ctx bounds the requests.
//   - svc is the client of the bucket's location.
//   - bucket is the bucket name.
func abortMultipartUploads(ctx context.Context, svc *s3.S3, bucket string) error {
	var abortErr error
	input := &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)}
	err := svc.ListMultipartUploadsPagesWithContext(ctx, input, func(page *s3.ListMultipartUploadsOutput, _ bool) bool {
		for _, upload := range page.Uploads {
			_, err := svc.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      upload.Key,
				UploadId: upload.UploadId,
			})
			if err != nil {
				if apiErr := newCOSError("AbortMultipartUpload", err); !IsNotFound(apiErr) {
					abortErr = apiErr
					return false
				}
			}
		}
		return true
	})
	if err != nil {
		return newCOSError("ListMultipartUploads", err)
	}
	return abortErr
}

// bucketClient returns the client of the bucket's location after checking that the bucket is a ROKS registry bucket.
//
// Parameters:
//   - bucket is a ListBuckets result.
func (s *BucketService) bucketClient(bucket Bucket) (*s3.S3, error) {
	if !strings.HasPrefix(bucket.Name, registryBucketPrefix) {
		return nil, fmt.Errorf("bucket %q: the e2e tooling empties and deletes only the registry buckets of ROKS "+
			"clusters, named %s<cluster ID>-..., never another bucket of the instance", bucket.Name, registryBucketPrefix)
	}
	location, err := BucketLocation(bucket.LocationConstraint)
	if err != nil {
		return nil, fmt.Errorf("bucket %q: %w", bucket.Name, err)
	}
	return s.client(location)
}

// client returns the SDK client of a location, building it on first use.
//
// The client is built from explicitClientConfig rather than from an SDK session, because a session reads AWS_*
// environment variables and the ~/.aws files. Its credentials are the SDK's anonymous credentials, so that neither
// the SDK's IAM signer nor its HMAC signer signs a request; signWithAuthenticator, a Sign handler, adds the bearer
// token of the caller's authenticator instead. Retries are off (MaxRetries 0): CleanUpRegistryBuckets retries whole
// attempts itself. Path-style addressing keeps the bucket name out of the host name, as CAPIBM's own COS client does.
//
// Parameters:
//   - location is a COS location; it must match cosLocationPattern.
func (s *BucketService) client(location string) (*s3.S3, error) {
	if !cosLocationPattern.MatchString(location) {
		return nil, fmt.Errorf("%s client: %q is not a COS location", cosService, location)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if svc, ok := s.clients[location]; ok {
		return svc, nil
	}
	endpoint := s.endpointURL(location)
	handlers := defaults.Handlers()
	// The handler appends AWS_EXECUTION_ENV to the User-Agent, the only environment read left in the default handlers.
	handlers.Build.Remove(corehandlers.AddHostExecEnvUserAgentHander)
	// A session adds the parameter validation to the default handlers; without a session it is added here.
	handlers.Validate.PushBackNamed(corehandlers.ValidateParametersHandler)
	provider := explicitClientConfig{config: client.Config{
		Config: &aws.Config{
			Region:           aws.String(location),
			Endpoint:         aws.String(endpoint),
			HTTPClient:       s.httpClient,
			MaxRetries:       aws.Int(0),
			S3ForcePathStyle: aws.Bool(true),
		},
		Handlers:      handlers,
		Endpoint:      endpoint,
		SigningRegion: location,
		SigningName:   s3.ServiceName,
	}}
	// The credentials are passed to New, not only in the provider's configuration: s3.New reads IBM credentials from
	// the environment to check them for conflicts unless one of its configurations sets credentials.
	svc := s3.New(provider, &aws.Config{Credentials: credentials.AnonymousCredentials})
	svc.Handlers.Build.PushBackNamed(request.NamedHandler{
		Name: "managedtools.UserAgent",
		Fn:   request.MakeAddToUserAgentFreeFormHandler(DefaultUserAgent),
	})
	svc.Handlers.Sign.PushBackNamed(request.NamedHandler{
		Name: "managedtools.SignWithAuthenticator",
		Fn:   s.signWithAuthenticator,
	})
	s.clients[location] = svc
	return svc, nil
}

// signWithAuthenticator is the Sign handler of every request: it adds the Authorization header of the caller's
// authenticator (for an IAM authenticator, "Bearer <IAM token>", fetched or refreshed as needed).
//
// Parameters:
//   - r is the SDK request being signed.
func (s *BucketService) signWithAuthenticator(r *request.Request) {
	if err := s.authenticator.Authenticate(r.HTTPRequest); err != nil {
		r.Error = fmt.Errorf("%s client: authenticating the request: %w", cosService, err)
	}
}

// explicitClientConfig is the client.ConfigProvider the COS clients are built from: it hands the SDK client exactly
// the configuration built in BucketService.client, with the configurations passed to s3.New merged into a copy.
type explicitClientConfig struct {
	// config is the client configuration.
	config client.Config
}

// ClientConfig implements client.ConfigProvider.
//
// Parameters:
//   - serviceName is the SDK's service name; the configuration is for the object storage API only, so it is ignored.
//   - cfgs are merged into a copy of the configuration.
func (p explicitClientConfig) ClientConfig(_ string, cfgs ...*aws.Config) client.Config {
	config := p.config
	config.Config = p.config.Config.Copy(cfgs...)
	config.Handlers = p.config.Handlers.Copy()
	return config
}

// newCOSError turns an SDK error into an APIError: for an answer, its status code and IBM's error code and message;
// for a request that got no answer (transport error, timeout, cancellation), status 0 and the SDK's text. Never the
// request itself, so that no token reaches a log. Any other error (for example one of signWithAuthenticator) is
// returned wrapped, with the operation's name.
//
// Parameters:
//   - operation names the call, for example "DeleteBucket".
//   - err is the SDK's error; it must not be nil.
func newCOSError(operation string, err error) error {
	var failure awserr.RequestFailure
	if errors.As(err, &failure) {
		return &APIError{Operation: operation, StatusCode: failure.StatusCode(),
			Message: failure.Code() + ": " + failure.Message()}
	}
	var sdkErr awserr.Error
	if errors.As(err, &sdkErr) {
		switch sdkErr.Code() {
		case request.ErrCodeRequestError, request.ErrCodeResponseTimeout, request.CanceledErrorCode:
			return &APIError{Operation: operation, Message: err.Error()}
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
