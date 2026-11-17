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
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// This file is the registry bucket check of the ROKS lane.
//
// A ROKS cluster on VPC is created with a Cloud Object Storage instance (openShift.imageRegistry.cos.instanceCRN, which
// the ROKS flavors fill from COS_INSTANCE_CRN), and IBM creates a bucket in that instance to back the cluster's
// internal image registry. Deleting the cluster keeps the bucket and everything in it
// (https://cloud.ibm.com/docs/openshift?topic=openshift-registry#cos_image_registry: "Any data that is stored in the
// object storage bucket remains even if you delete the cluster"); only a cluster DELETE with
// deleteOpenShiftRegistryCOSBucket=true removes it. The IBM SDK that CAPIBM pins cannot send that parameter
// (kubernetesserviceapiv1.RemoveClusterOptions has no field for it), so CAPIBM keeps the bucket, and every ROKS run
// would leave one behind in the CI's instance. The inventory diff (inventory.go) lists no buckets and would not notice.
//
// So the ROKS lane lists the instance's buckets before it creates anything and, once IBM answers 404 for its cluster,
// empties and deletes every new bucket named after that cluster. It touches no other new bucket: the account scope
// fails the lane naming them, and the lane scope only reports them as not attributed to the run. This cleanup becomes
// unnecessary once CAPIBM can pass deleteOpenShiftRegistryCOSBucket on the cluster DELETE: the lane would then only
// check that no bucket is left.

// COSInstanceCRNVariable is the e2e configuration variable with the CRN of the Cloud Object Storage instance that the
// ROKS lane's clusters store their image registry in, and whose buckets the lane checks.
const COSInstanceCRNVariable = "COS_INSTANCE_CRN"

// registryBucketPrefix starts the name of every ROKS registry bucket that the cleanup attributes to a cluster, which it
// takes to be roks-<cluster ID>-<random suffix>. IBM documents that form only for a registry bucket created by hand
// (https://cloud.ibm.com/docs/openshift?topic=openshift-registry#cos-registry-steps, "Configuring the COS bucket");
// that the bucket IBM creates with the cluster has the same form is not documented, so the cleanup deletes only buckets
// of exactly this form and touches no other new bucket (see DiffBuckets for what each scope does with those).
const registryBucketPrefix = "roks-"

// cosLocationPattern accepts the COS locations that IBM's endpoints are named after: regions such as "us-south", the
// cross-region geographies "us", "eu" and "ap", and single sites such as "ams03"
// (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-endpoints). It never accepts a dot, a
// slash or a colon, so a location from IBM's answer cannot turn an endpoint into another host.
var cosLocationPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// cosStorageClassPattern accepts the storage class part of a LocationConstraint, for example "standard", "smart" or
// "onerate_active".
var cosStorageClassPattern = regexp.MustCompile(`^[a-z0-9_]+$`)

// Bucket is one bucket of a Cloud Object Storage instance, as the extended bucket listing returns it.
type Bucket struct {
	// Name is the bucket name. Bucket names are unique across all of Cloud Object Storage, so the name identifies the
	// bucket across listings.
	Name string
	// LocationConstraint is IBM's location and storage class of the bucket, for example "us-south-smart",
	// "us-standard" or "ams03-cold" (see BucketLocation).
	LocationConstraint string
	// Created is the bucket's creation time; zero when IBM sent none.
	Created time.Time
}

// BucketLister lists the buckets of one Cloud Object Storage instance. BucketService implements it.
type BucketLister interface {
	// ListBuckets returns every bucket of the instance, whatever its location.
	ListBuckets(ctx context.Context) ([]Bucket, error)
}

// BucketAPI is what the registry bucket cleanup needs from Cloud Object Storage. BucketService implements it.
type BucketAPI interface {
	BucketLister
	// EmptyBucket deletes every object, object version, delete marker and incomplete multipart upload of the bucket,
	// because IBM deletes only an empty bucket.
	EmptyBucket(ctx context.Context, bucket Bucket) error
	// DeleteBucket deletes the bucket, which must be empty.
	DeleteBucket(ctx context.Context, bucket Bucket) error
}

// ValidateCOSInstanceCRN returns an error unless crn has the form of a Cloud Object Storage instance CRN: the form that
// the IBMCloudManagedControlPlane API accepts for openShift.imageRegistry.cos.instanceCRN ("crn:v1:" and the service
// name cloud-object-storage), without white space.
//
// Parameters:
//   - crn is the value of COSInstanceCRNVariable.
func ValidateCOSInstanceCRN(crn string) error {
	if !strings.HasPrefix(crn, "crn:v1:") || !strings.Contains(crn, ":cloud-object-storage:") ||
		strings.ContainsAny(crn, " \t\r\n") {
		return fmt.Errorf("%s %q is not the CRN of a Cloud Object Storage instance: set the CRN (for example "+
			"crn:v1:bluemix:public:cloud-object-storage:global:a/<account ID>:<instance GUID>::) of the standard COS "+
			"instance that the ROKS clusters store their image registry in", COSInstanceCRNVariable, crn)
	}
	return nil
}

// BucketLocation returns the location part of a bucket's LocationConstraint, which is "<location>-<storage class>"
// (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-classes#classes-locationconstraint):
// "us-south" for "us-south-smart", "us" for "us-standard", "ams03" for "ams03-cold". Every request about one bucket
// goes to the endpoint of its location
// (https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-endpoints), while the bucket listing
// answers on any endpoint.
//
// Parameters:
//   - constraint is the LocationConstraint of a ListBuckets result.
func BucketLocation(constraint string) (string, error) {
	cut := strings.LastIndex(constraint, "-")
	if cut < 0 || !cosLocationPattern.MatchString(constraint[:cut]) ||
		!cosStorageClassPattern.MatchString(constraint[cut+1:]) {
		return "", fmt.Errorf("location constraint %q is not of the form <location>-<storage class> (for example "+
			"us-south-smart or us-standard), so the endpoint of the bucket is unknown", constraint)
	}
	return constraint[:cut], nil
}

// RegistryBucketNamePrefix returns "roks-<cluster ID>-", the start of the name of every registry bucket that the
// cleanup attributes to the cluster (RegistryBucketOwner). The lanes also name it in their messages, so that an
// operator can find a registry bucket that a failed or skipped cleanup left behind.
//
// Parameters:
//   - clusterID is the IBM ID of a ROKS cluster.
func RegistryBucketNamePrefix(clusterID string) string {
	return registryBucketPrefix + clusterID + "-"
}

// RegistryBucketOwner returns the ID, among clusterIDs, of the ROKS cluster that the bucket name belongs to: the name
// starts with the cluster's RegistryBucketNamePrefix and goes on after it. That prefix is the only proof of ownership
// the cleanup accepts. An entry that is not an IBM cluster ID (ValidateClusterID), "" in particular, never matches, so
// a bucket can never be attributed to a missing ID. IBM cluster IDs contain no hyphen, so at most one ID matches.
//
// Parameters:
//   - name is a bucket name.
//   - clusterIDs are the IBM IDs of the run's clusters.
func RegistryBucketOwner(name string, clusterIDs []string) (string, bool) {
	for _, id := range clusterIDs {
		if ValidateClusterID(id) != nil {
			continue
		}
		prefix := RegistryBucketNamePrefix(id)
		if strings.HasPrefix(name, prefix) && len(name) > len(prefix) {
			return id, true
		}
	}
	return "", false
}

// BucketDiff is what a run added to a Cloud Object Storage instance, split by the lane's leftover scope.
type BucketDiff struct {
	// RunRegistry are the new buckets named after one of the run's clusters (RegistryBucketOwner): the registry
	// buckets IBM created for them, which the lane deletes.
	RunRegistry []Bucket
	// Leftovers are the other new buckets in the account scope. The lane fails naming them and never touches them.
	Leftovers []Bucket
	// NotAttributed are the other new buckets in the lane scope. The lane never touches them and does not fail on
	// them, because nothing proves they are the run's, but it reports them: one of them may still be the run's own
	// registry bucket, if IBM named it in another form or the lane never learnt its cluster's ID.
	NotAttributed []Bucket
}

// DiffBuckets returns the buckets of after that are not in before, split into the run's registry buckets and the
// other new buckets, each list sorted by name. A bucket that was already in before is never the run's, whatever its
// name, so it is in no list. Buckets that disappeared during the run are not reported.
//
// The scope decides about the other new buckets, as it does for the inventory diff: the account scope reports every
// one of them as a leftover, whoever created it; the lane scope reports them as not attributed to the run, because
// only the name of a registry bucket says which cluster, and so which run, it belongs to.
//
// Parameters:
//   - before is the listing taken before the lane created anything.
//   - after is the listing taken after the run's clusters were gone.
//   - runClusterIDs are the IBM IDs of the run's clusters.
//   - scope is the lane's leftover scope.
func DiffBuckets(before, after []Bucket, runClusterIDs []string, scope InventoryScope) BucketDiff {
	known := make(map[string]bool, len(before))
	for _, bucket := range before {
		known[bucket.Name] = true
	}
	var diff BucketDiff
	for _, bucket := range after {
		if known[bucket.Name] {
			continue
		}
		if _, ok := RegistryBucketOwner(bucket.Name, runClusterIDs); ok {
			diff.RunRegistry = append(diff.RunRegistry, bucket)
			continue
		}
		if scope.Scope() == LeftoverScopeAccount {
			diff.Leftovers = append(diff.Leftovers, bucket)
		} else {
			diff.NotAttributed = append(diff.NotAttributed, bucket)
		}
	}
	slices.SortFunc(diff.RunRegistry, compareBuckets)
	slices.SortFunc(diff.Leftovers, compareBuckets)
	slices.SortFunc(diff.NotAttributed, compareBuckets)
	return diff
}

// RegistryBucketCleanup configures CleanUpRegistryBuckets.
type RegistryBucketCleanup struct {
	// Baseline is the instance's bucket listing taken before the lane created anything.
	Baseline []Bucket
	// RunClusterIDs are the IBM IDs of the run's clusters; only new buckets named after one of them are deleted. IBM
	// must answer 404 for every one of them before the cleanup runs: a cluster that still exists may still write to its
	// registry bucket.
	RunClusterIDs []string
	// Scope is the lane's leftover scope (see DiffBuckets).
	Scope InventoryScope
	// Attempts is how often the emptying and deletion of one bucket is tried before the cleanup gives up on it; at
	// least 1.
	Attempts int
	// Wait is the pause between two attempts; not negative.
	Wait time.Duration
}

// validate returns one error naming every invalid field of c.
func (c RegistryBucketCleanup) validate() error {
	var errs []error
	if c.Attempts < 1 {
		errs = append(errs, fmt.Errorf("registry bucket cleanup: Attempts is %d; it must be at least 1", c.Attempts))
	}
	if c.Wait < 0 {
		errs = append(errs, fmt.Errorf("registry bucket cleanup: Wait is %s; it must not be negative", c.Wait))
	}
	return errors.Join(errs...)
}

// FailedBucketDeletion is a run registry bucket that the cleanup could not delete.
type FailedBucketDeletion struct {
	// Bucket is the bucket that stays.
	Bucket Bucket
	// Err is the error of the last attempt.
	Err error
}

// BucketCleanupReport is the outcome of CleanUpRegistryBuckets.
type BucketCleanupReport struct {
	// Deleted are the run's registry buckets that are gone: IBM no longer lists them.
	Deleted []Bucket
	// Failed are the run's registry buckets that stay.
	Failed []FailedBucketDeletion
	// Leftovers are the other new buckets in the account scope, untouched; each fails the cleanup.
	Leftovers []Bucket
	// NotAttributed are the other new buckets in the lane scope, untouched; they do not fail the cleanup.
	NotAttributed []Bucket
}

// String renders the report for the suite log, one bucket per line.
func (r BucketCleanupReport) String() string {
	var b strings.Builder
	for _, bucket := range r.Deleted {
		fmt.Fprintf(&b, "deleted registry bucket %s (%s)\n", bucket.Name, bucket.LocationConstraint)
	}
	for _, f := range r.Failed {
		fmt.Fprintf(&b, "NOT deleted: registry bucket %s (%s): %v\n", f.Bucket.Name, f.Bucket.LocationConstraint, f.Err)
	}
	for _, bucket := range r.Leftovers {
		fmt.Fprintf(&b, "leftover bucket %s (%s), left untouched: its name does not start with %s<ID of a cluster of "+
			"this run>-\n", bucket.Name, bucket.LocationConstraint, registryBucketPrefix)
	}
	for _, bucket := range r.NotAttributed {
		fmt.Fprintf(&b, "new bucket %s (%s), not attributed to this run, left untouched: its name does not start with "+
			"%s<ID of a cluster of this run>-; if it is the registry bucket of a cluster of this run, delete it by hand\n",
			bucket.Name, bucket.LocationConstraint, registryBucketPrefix)
	}
	if b.Len() == 0 {
		return "no new bucket\n"
	}
	return b.String()
}

// CleanUpRegistryBuckets lists the instance's buckets, deletes the run's new registry buckets (DiffBuckets) and
// returns an error for each one it could not delete and for every leftover, which it never touches. The new buckets
// that the lane scope does not attribute to the run go into the report only, untouched and without an error. It sends
// no request about a bucket that is not one of the run's registry buckets.
//
// A bucket counts as deleted only once IBM no longer lists it. IBM's answer to the DELETE alone is not enough: a 404
// from the endpoint of the wrong location would look like an earlier deletion while the bucket stays. Each attempt
// therefore empties the bucket (EmptyBucket; a 404 passes), deletes it (DeleteBucket; a 404 passes) and lists the
// buckets again; the next attempt starts opts.Wait later. The cleanup gives up on a bucket after opts.Attempts attempts
// and goes on with the next one.
//
// Parameters:
//   - ctx bounds the requests and the waits.
//   - api is the Cloud Object Storage client, built with the credentials that create the run's clusters.
//   - opts are the baseline, the run's cluster IDs, the scope and the retry bounds.
func CleanUpRegistryBuckets(ctx context.Context, api BucketAPI, opts RegistryBucketCleanup) (BucketCleanupReport, error) {
	if err := opts.validate(); err != nil {
		return BucketCleanupReport{}, err
	}
	after, err := api.ListBuckets(ctx)
	if err != nil {
		return BucketCleanupReport{}, fmt.Errorf("registry bucket cleanup: listing the buckets: %w", err)
	}
	diff := DiffBuckets(opts.Baseline, after, opts.RunClusterIDs, opts.Scope)
	report := BucketCleanupReport{Leftovers: diff.Leftovers, NotAttributed: diff.NotAttributed}
	var errs []error
	for _, bucket := range diff.RunRegistry {
		if err := deleteRegistryBucket(ctx, api, bucket, opts.Attempts, opts.Wait); err != nil {
			report.Failed = append(report.Failed, FailedBucketDeletion{Bucket: bucket, Err: err})
			errs = append(errs, fmt.Errorf("registry bucket %s: %w", bucket.Name, err))
			continue
		}
		report.Deleted = append(report.Deleted, bucket)
	}
	if len(diff.Leftovers) > 0 {
		names := make([]string, 0, len(diff.Leftovers))
		for _, bucket := range diff.Leftovers {
			names = append(names, bucket.Name)
		}
		errs = append(errs, fmt.Errorf("new buckets that are not the registry bucket of a cluster of this run, left "+
			"untouched: %s; delete them by hand once you know whose they are", strings.Join(names, ", ")))
	}
	return report, errors.Join(errs...)
}

// deleteRegistryBucket tries up to attempts times to empty and delete bucket, waiting wait between two attempts, and
// returns nil once IBM no longer lists it.
//
// Parameters:
//   - ctx bounds the requests and the waits.
//   - api is the Cloud Object Storage client.
//   - bucket is one of the run's registry buckets.
//   - attempts is at least 1.
//   - wait is the pause between two attempts.
func deleteRegistryBucket(ctx context.Context, api BucketAPI, bucket Bucket, attempts int, wait time.Duration) error {
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if err := sleepContext(ctx, wait); err != nil {
				return fmt.Errorf("stopped after %d of %d attempts: %w (last attempt: %w)", attempt-1, attempts, err, lastErr)
			}
		}
		if lastErr = tryDeleteBucket(ctx, api, bucket); lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("gave up after %d attempts: %w", attempts, lastErr)
}

// tryDeleteBucket makes one attempt: it empties and deletes the bucket and checks that IBM no longer lists it.
//
// Parameters:
//   - ctx bounds the requests.
//   - api is the Cloud Object Storage client.
//   - bucket is one of the run's registry buckets.
func tryDeleteBucket(ctx context.Context, api BucketAPI, bucket Bucket) error {
	if err := api.EmptyBucket(ctx, bucket); err != nil && !IsNotFound(err) {
		return fmt.Errorf("emptying it: %w", err)
	}
	if err := api.DeleteBucket(ctx, bucket); err != nil && !IsNotFound(err) {
		return fmt.Errorf("deleting it: %w", err)
	}
	listed, err := api.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("checking its deletion: %w", err)
	}
	if slices.ContainsFunc(listed, func(b Bucket) bool { return b.Name == bucket.Name }) {
		return fmt.Errorf("IBM still lists it after its deletion; check that location constraint %q leads to the "+
			"endpoint of its location", bucket.LocationConstraint)
	}
	return nil
}

// sleepContext waits for d, or until ctx ends, whichever comes first; it returns ctx's error in the second case.
//
// Parameters:
//   - ctx bounds the wait.
//   - d is the time to wait; 0 or less returns at once (unless ctx has ended).
func sleepContext(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// compareBuckets orders buckets by name.
//
// Parameters:
//   - a and b are the buckets to order.
func compareBuckets(a, b Bucket) int {
	return cmp.Compare(a.Name, b.Name)
}
