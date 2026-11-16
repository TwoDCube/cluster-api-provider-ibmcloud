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
	"context"
	"errors"
	"fmt"

	"github.com/IBM/go-sdk-core/v5/core"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/version"
)

// This file sets up the ClusterCache of the managed controllers. The pool controller reads the workload cluster
// through it (Nodes, Pods, PodDisruptionBudgets, VolumeAttachments) to drain Nodes before IBM removes workers, and
// watches its connection events. CAPIBM had no ClusterCache before this feature, so it exists only with the
// ManagedKubernetes gate on, and it connects only to Clusters whose control plane is an IBMCloudManagedControlPlane:
// without the filter it would connect to every Cluster on the management cluster that passes --watch-filter,
// including CAPIBM's own VPC and PowerVS clusters and other providers' clusters, read their admin kubeconfig, run a
// Node informer and probe their API servers every 10 seconds. The filter needs Cluster API v1.14.1 or newer; v1.14.0
// ignored Options.ClusterFilter in SetupWithManager (cluster-api pull request 14124).

const (
	// ibmCloudManagedControlPlaneKind is the control plane kind that marks a managed Cluster.
	ibmCloudManagedControlPlaneKind = "IBMCloudManagedControlPlane"

	// clusterCacheConcurrency is the number of Clusters the ClusterCache reconciles at once. One reconcile may block for
	// up to the client timeout (10 s) on an unreachable workload API server; with a single worker, one unreachable
	// cluster would delay the health probes and the connect events of every other managed cluster. Cluster API's own
	// default is 100 for all clusters of a management cluster; CAPIBM only serves its managed clusters.
	clusterCacheConcurrency = 10
)

// ClusterCacheOptions configures SetupClusterCache.
type ClusterCacheOptions struct {
	// WatchFilterValue is the --watch-filter label value; Clusters without it never get a connection.
	WatchFilterValue string
}

// The ClusterCache watches Clusters and reads each managed Cluster's <cluster>-kubeconfig Secret (uncached).
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

// SetupClusterCache creates the ClusterCache of the managed controllers and registers its reconciler with mgr. It
// uses Cluster API's options (core/setup/setup.go of Cluster API v1.14) with two differences: Secrets are read
// uncached with the manager's API reader, because CAPIBM's manager has no label-filtered Secret cache, and only
// managed Clusters are connected (IsIBMCloudManagedCluster).
//
// Parameters:
//   - ctx is the setup context; its logger names the ClusterCache controller.
//   - mgr is the controller manager; the cache's reconciler watches Clusters through it.
//   - opts are the watch filter options.
func SetupClusterCache(ctx context.Context, mgr ctrl.Manager, opts ClusterCacheOptions) (clustercache.ClusterCache, error) {
	if core.IsNil(mgr) {
		return nil, errors.New("managed: SetupClusterCache needs the controller manager, got nil")
	}
	clusterCache, err := clustercache.SetupWithManager(ctx, mgr, clusterCacheOptions(mgr.GetAPIReader(), opts),
		controller.Options{MaxConcurrentReconciles: clusterCacheConcurrency})
	if err != nil {
		return nil, fmt.Errorf("setting up the ClusterCache of the managed controllers: %w", err)
	}
	return clusterCache, nil
}

// clusterCacheOptions returns the ClusterCache options of SetupClusterCache.
//
// Parameters:
//   - secretReader reads the <cluster>-kubeconfig Secrets; the manager's uncached API reader.
//   - opts are the watch filter options.
func clusterCacheOptions(secretReader client.Reader, opts ClusterCacheOptions) clustercache.Options {
	return clustercache.Options{
		SecretClient:     secretReader,
		WatchFilterValue: opts.WatchFilterValue,
		ClusterFilter:    IsIBMCloudManagedCluster,
		Cache: clustercache.CacheOptions{
			// Managed fields are never read; dropping them halves the memory of the Node cache.
			DefaultTransform: ctrlcache.TransformStripManagedFields(),
			// The pool controller finds the Node of an IBM worker by its providerID and touches no other Node.
			Indexes: []clustercache.CacheOptionsIndex{clustercache.NodeProviderIDIndex},
		},
		Client: clustercache.ClientOptions{
			UserAgent: "cluster-api-provider-ibmcloud/" + version.Get().GitVersion,
			Cache: clustercache.ClientCacheOptions{
				// Read live, never cached: Pods and DaemonSets are listed per Node during a drain, VolumeAttachments and
				// PersistentVolumes while waiting for detach, so that no workload object of a workload cluster is held in
				// CAPIBM's memory; ConfigMaps and Secrets may hold workload data CAPIBM never needs.
				DisableFor: []client.Object{
					&corev1.Pod{},
					&appsv1.DaemonSet{},
					&storagev1.VolumeAttachment{},
					&corev1.PersistentVolume{},
					&corev1.ConfigMap{},
					&corev1.Secret{},
				},
			},
		},
	}
}

// IsIBMCloudManagedCluster reports whether cluster's spec.controlPlaneRef names an IBMCloudManagedControlPlane (group
// controlplane.cluster.x-k8s.io). It is the ClusterCache's ClusterFilter. cluster is the Cluster the ClusterCache
// reconciles; nil is not managed.
func IsIBMCloudManagedCluster(cluster *clusterv1.Cluster) bool {
	if cluster == nil {
		return false
	}
	ref := cluster.Spec.ControlPlaneRef
	return ref.APIGroup == controlplanev1.GroupVersion.Group && ref.Kind == ibmCloudManagedControlPlaneKind
}
