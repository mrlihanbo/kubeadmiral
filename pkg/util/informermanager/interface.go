/*
Copyright 2023 The KubeAdmiral Authors.

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

package informermanager

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	fedcorev1a1listers "github.com/kubewharf/kubeadmiral/pkg/client/listers/core/v1alpha1"
)

// EventHandlerGenerator is used by InformerManger and FederatedInformerManager to generate and register
// ResourceEventHandlers for each FTC's source type informer.
type EventHandlerGenerator struct {
	// Predicate is called each time a FTC is reconciled to determine if a new event handler needs to be generated and
	// registered for this EventHandlerGenerator. If Predicate returns true, any previously registered event handler
	// for this EventHandlerGenerator will also be unregistered.
	Predicate func(lastApplied, latest *fedcorev1a1.FederatedTypeConfig) bool
	// Generator is used to generate a ResourceEventHandler for the given FTC. If nil is returned, no event handler will
	// be registered.
	Generator func(ftc *fedcorev1a1.FederatedTypeConfig) cache.ResourceEventHandler
}

// InformerManager provides an interface for controllers that need to dynamically register event handlers and access
// objects based on FederatedTypeConfigs. InformerManager will listen to FTC events and maintain informers for the
// source type of each FTC.
//
// Having multiple FTCs with the same source type is not supported and may cause InformerManager to behave incorrectly.
// Updating FTC source types is also not supported and may also cause InformerManager to behave incorrectly.
type InformerManager interface {
	// Adds an EventHandler used to generate and register ResourceEventHandlers for each FTC's source type informer.
	AddEventHandlerGenerator(generator *EventHandlerGenerator) error
	// Returns a lister for the given GroupResourceVersion if it exists. The lister for each FTC's source type will
	// eventually exist.
	GetResourceLister(gvk schema.GroupVersionKind) (lister cache.GenericLister, informerSynced cache.InformerSynced, exists bool)
	// Returns the known FTC mapping for the given GVK if it exists.
	GetResourceFTC(gvk schema.GroupVersionKind) (ftc *fedcorev1a1.FederatedTypeConfig, exists bool)

	// Returns the FederatedTypeConfig lister used by the InformerManager.
	GetFederatedTypeConfigLister() fedcorev1a1listers.FederatedTypeConfigLister
	// Returns true if the InformerManager's view of FederatedTypeConfigs is synced.
	HasSynced() bool

	// Starts processing FederatedTypeConfig events.
	Start(ctx context.Context)

	// Returns true if the InformerManager is gracefully shutdown.
	IsShutdown() bool
}

// ClusterEventHandler can be registered by controllers to hook into the cluster events received by the
// FederatedInformerManager.
type ClusterEventHandler struct {
	// ClusterEventPredicate is called for each FederatedCluster event and determines if the callback of this
	// ClusterEventHandler should be called for the the given event.
	Predicate ClusterEventPredicate
	// Callback is a function that accepts a FederatedCluster object.
	Callback func(cluster *fedcorev1a1.FederatedCluster)
}

// ClusterEventPredicate determines if a callback should be called for a given cluster event.
type ClusterEventPredicate func(oldCluster, newCluster *fedcorev1a1.FederatedCluster) bool

// FederatedInformerManager provides an interface for controllers that need to dynamically register event handlers and
// access objects in member clusters based on FederatedTypeConfigs. FederatedInformerManager will listen to FTC events
// and maintain informers for each FTC's source type and joined member cluster.
//
// Having multiple FTCs with the same source type is not supported and may cause FederatedInformerManager to behave
// incorrectly. Updating FTC source types is also not supported and may also cause FederatedInformerManager to behave
// incorrectly.
//
// Updating Cluster connection details is also not supported and may cause FederatedInformerManager to behave
// incorrectly.
type FederatedInformerManager interface {
	// Adds an EventHandler used to generate and register ResourceEventHandlers for each FTC's source type informer.
	AddEventHandlerGenerator(generator *EventHandlerGenerator) error
	// Returns a lister for the given GroupResourceVersion and cluster if it exists. The lister for each FTC's source
	// type and cluster will eventually exist.
	GetResourceLister(
		gvk schema.GroupVersionKind,
		cluster string,
	) (lister cache.GenericLister, informerSynced cache.InformerSynced, exists bool)
	// Returns a client for the given cluster if it exists. The client for each cluster will eventually exist.
	GetClusterClient(cluster string) (client dynamic.Interface, exists bool)

	// Returns the FederatedTypeConfig lister used by the FederatedInformerManager.
	GetFederatedTypeConfigLister() fedcorev1a1listers.FederatedTypeConfigLister
	// Returns the FederatedCluster lister used by the FederatedInformerManager.
	GetFederatedClusterLister() fedcorev1a1listers.FederatedClusterLister
	// Returns true if the FederatedInformerManager's view of FederatedTypeConfigs and FederatedClusters is synced.
	HasSynced() bool

	// Adds ClusterEventHandlers that can be used by controllers to hook into the cluster events received by the
	// FederatedInformerManager.
	AddClusterEventHandlers(handlers ...*ClusterEventHandler) error

	// Starts processing FederatedTypeConfig and FederatedCluster events.
	Start(ctx context.Context)

	// Returns true if the InformerManager is gracefully shutdown.
	IsShutdown() bool
}

// ClusterClientGetter is used by the FederatedInformerManager to create clients for joined member clusters.
type ClusterClientGetter struct {
	// ConnectionHash should return a string that uniquely identifies the combination of parameters used to generate the
	// cluster client. A change in the connection hash indicates a need to create a new client for a given member
	// cluster.
	ConnectionHash func(cluster *fedcorev1a1.FederatedCluster) ([]byte, error)
	// ClientGetter returns a dynamic client for the given member cluster.
	ClientGetter func(cluster *fedcorev1a1.FederatedCluster) (dynamic.Interface, error)
}