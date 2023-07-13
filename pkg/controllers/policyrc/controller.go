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

package policyrc

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/client/generic"
	fedcorev1a1informers "github.com/kubewharf/kubeadmiral/pkg/client/informers/externalversions/core/v1alpha1"
	fedcorev1a1listers "github.com/kubewharf/kubeadmiral/pkg/client/listers/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	controllercontext "github.com/kubewharf/kubeadmiral/pkg/controllers/context"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/override"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/util"
	"github.com/kubewharf/kubeadmiral/pkg/stats"
	"github.com/kubewharf/kubeadmiral/pkg/util/fedobjectadapters"
	"github.com/kubewharf/kubeadmiral/pkg/util/worker"
)

const (
	ControllerName = "policyrc-controller"
)

type informerPair struct {
	store      cache.Store
	controller cache.Controller
}

type Controller struct {
	// name of controller: <federatedKind>-policyrc-controller
	name string

	// Informer store and controller for the PropagationPolicy, ClusterPropagationPolicy,
	// OverridePolicy and ClusterOverridePolicy respectively.
	pp, cpp, op, cop informerPair

	client                   generic.Client
	fedObjectInformer        fedcorev1a1informers.FederatedObjectInformer
	clusterFedObjectInformer fedcorev1a1informers.ClusterFederatedObjectInformer
	fedObjectLister          fedcorev1a1listers.FederatedObjectLister
	clusterFedObjectLister   fedcorev1a1listers.ClusterFederatedObjectLister

	ppCounter, opCounter *Counter

	// updates the local counter upon fed object updates
	countWorker worker.ReconcileWorker[common.QualifiedName]
	// pushes values from local counter to apiserver
	persistPpWorker, persistOpWorker worker.ReconcileWorker[common.QualifiedName]

	metrics stats.Metrics
	logger  klog.Logger
}

func NewPolicyRCController(controllerCtx *controllercontext.Context) (*Controller, error) {
	configWithUserAgent := rest.CopyConfig(controllerCtx.RestConfig)
	rest.AddUserAgent(configWithUserAgent, ControllerName)

	c := &Controller{
		name:                     ControllerName,
		metrics:                  controllerCtx.Metrics,
		logger:                   klog.LoggerWithValues(klog.Background(), "controller", ControllerName),
		fedObjectInformer:        controllerCtx.FedInformerFactory.Core().V1alpha1().FederatedObjects(),
		clusterFedObjectInformer: controllerCtx.FedInformerFactory.Core().V1alpha1().ClusterFederatedObjects(),
		fedObjectLister:          controllerCtx.FedInformerFactory.Core().V1alpha1().FederatedObjects().Lister(),
		clusterFedObjectLister:   controllerCtx.FedInformerFactory.Core().V1alpha1().ClusterFederatedObjects().Lister(),
	}

	c.fedObjectInformer.Informer().AddEventHandler(util.NewTriggerOnAllChanges(func(o pkgruntime.Object) {
		c.countWorker.Enqueue(common.NewQualifiedName(o))
	}))

	c.clusterFedObjectInformer.Informer().AddEventHandler(util.NewTriggerOnAllChanges(func(o pkgruntime.Object) {
		c.countWorker.Enqueue(common.NewQualifiedName(o))
	}))

	c.countWorker = worker.NewReconcileWorker[common.QualifiedName](
		"policyrc-controller-count-worker",
		nil,
		c.reconcileCount,
		worker.RateLimiterOptions{},
		1, // currently only one worker is meaningful due to the global mutex
		controllerCtx.Metrics,
	)

	c.persistPpWorker = worker.NewReconcileWorker[common.QualifiedName](
		"policyrc-controller-persist-worker",
		nil,
		func(ctx context.Context, qualifiedName common.QualifiedName) worker.Result {
			return c.reconcilePersist("propagation-policy", qualifiedName, c.pp.store, c.cpp.store, c.ppCounter)
		},
		worker.RateLimiterOptions{},
		controllerCtx.WorkerCount,
		controllerCtx.Metrics,
	)
	c.persistOpWorker = worker.NewReconcileWorker[common.QualifiedName](
		"policyrc-controller-persist-worker",
		nil,
		func(ctx context.Context, qualifiedName common.QualifiedName) worker.Result {
			return c.reconcilePersist("override-policy", qualifiedName, c.op.store, c.cop.store, c.opCounter)
		},
		worker.RateLimiterOptions{},
		controllerCtx.WorkerCount,
		controllerCtx.Metrics,
	)

	var err error

	targetNamespace := controllerCtx.TargetNamespace
	c.client = generic.NewForConfigOrDie(configWithUserAgent)

	persistPpWorkerTrigger := func(o pkgruntime.Object) {
		c.persistPpWorker.Enqueue(common.NewQualifiedName(o))
	}

	c.pp.store, c.pp.controller, err = util.NewGenericInformer(
		configWithUserAgent,
		targetNamespace,
		&fedcorev1a1.PropagationPolicy{},
		0,
		persistPpWorkerTrigger,
		controllerCtx.Metrics,
	)
	if err != nil {
		return nil, err
	}

	c.cpp.store, c.cpp.controller, err = util.NewGenericInformer(
		configWithUserAgent,
		targetNamespace,
		&fedcorev1a1.ClusterPropagationPolicy{},
		0,
		persistPpWorkerTrigger,
		controllerCtx.Metrics,
	)
	if err != nil {
		return nil, err
	}

	persistOpWorkerTrigger := func(o pkgruntime.Object) {
		c.persistOpWorker.Enqueue(common.NewQualifiedName(o))
	}

	c.op.store, c.op.controller, err = util.NewGenericInformer(
		configWithUserAgent,
		targetNamespace,
		&fedcorev1a1.OverridePolicy{},
		0,
		persistOpWorkerTrigger,
		controllerCtx.Metrics,
	)
	if err != nil {
		return nil, err
	}

	c.cop.store, c.cop.controller, err = util.NewGenericInformer(
		configWithUserAgent,
		targetNamespace,
		&fedcorev1a1.ClusterOverridePolicy{},
		0,
		persistOpWorkerTrigger,
		controllerCtx.Metrics,
	)
	if err != nil {
		return nil, err
	}

	c.ppCounter = NewCounter(func(keys []PolicyKey) {
		for _, key := range keys {
			c.persistPpWorker.Enqueue(common.QualifiedName(key))
		}
	})

	c.opCounter = NewCounter(func(keys []PolicyKey) {
		for _, key := range keys {
			c.persistOpWorker.Enqueue(common.QualifiedName(key))
		}
	})

	return c, nil
}

func (c *Controller) Run(ctx context.Context) {
	c.logger.Info("Starting controller")
	defer c.logger.Info("Stopping controller")

	for _, pair := range []informerPair{c.pp, c.cpp, c.op, c.cop} {
		go pair.controller.Run(ctx.Done())
	}

	c.countWorker.Run(ctx)

	// wait for all counts to finish sync before persisting the values
	if !cache.WaitForNamedCacheSync(c.name, ctx.Done(), c.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync for controller: %s", c.name))
	}
	c.persistPpWorker.Run(ctx)
	c.persistOpWorker.Run(ctx)
}

func (c *Controller) HasSynced() bool {
	return c.pp.controller.HasSynced() &&
		c.cpp.controller.HasSynced() &&
		c.op.controller.HasSynced() &&
		c.cop.controller.HasSynced()
}

func (c *Controller) IsControllerReady() bool {
	return c.HasSynced()
}

func (c *Controller) reconcileCount(ctx context.Context, qualifiedName common.QualifiedName) (status worker.Result) {
	logger := c.logger.WithValues("object", qualifiedName.String())

	c.metrics.Rate("policyrc-count-controller.throughput", 1)
	logger.V(3).Info("Policyrc count controller starting to reconcile")
	startTime := time.Now()
	defer func() {
		c.metrics.Duration("policyrc-count-controller.latency", startTime)
		logger.V(3).WithValues("duration", time.Since(startTime), "status", status.String()).
			Info("Policyrc count controller finished reconciling")
	}()

	fedObj, err := fedobjectadapters.GetFromLister(c.fedObjectLister, c.clusterFedObjectLister, qualifiedName.Namespace, qualifiedName.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		utilruntime.HandleError(err)
		return worker.StatusError
	}

	var newPps []PolicyKey
	if fedObj != nil {
		newPolicy, newHasPolicy := fedobjectadapters.MatchedPolicyKey(fedObj, true)
		if newHasPolicy {
			newPps = []PolicyKey{PolicyKey(newPolicy)}
		}
	} else {
		// we still want to remove the count from the cache.
	}
	c.ppCounter.Update(ObjectKey(qualifiedName), newPps)

	var newOps []PolicyKey
	if fedObj != nil {
		if op, exists := fedObj.GetLabels()[override.OverridePolicyNameLabel]; exists {
			newOps = append(newOps, PolicyKey{Namespace: fedObj.GetNamespace(), Name: op})
		}
		if cop, exists := fedObj.GetLabels()[override.ClusterOverridePolicyNameLabel]; exists {
			newOps = append(newOps, PolicyKey{Name: cop})
		}
	} else {
		// we still want to remove the count from the cache.
	}
	c.opCounter.Update(ObjectKey(qualifiedName), newOps)

	return worker.StatusAllOK
}

func (c *Controller) reconcilePersist(
	metricName string,
	qualifiedName common.QualifiedName,
	nsScopeStore, clusterScopeStore cache.Store,
	counter *Counter,
) worker.Result {
	logger := c.logger.WithValues("object", qualifiedName.String())

	c.metrics.Rate(fmt.Sprintf("policyrc-persist-%s-controller.throughput", metricName), 1)
	logger.V(3).Info("Policyrc persist controller starting to reconcile")
	startTime := time.Now()
	defer func() {
		c.metrics.Duration(fmt.Sprintf("policyrc-persist-%s-controller.latency", metricName), startTime)
		logger.V(3).WithValues("duration", time.Since(startTime)).Info("Policyrc persist controller finished reconciling")
	}()

	store := clusterScopeStore
	if qualifiedName.Namespace != "" {
		store = nsScopeStore
	}

	policyAny, exists, err := store.GetByKey(qualifiedName.String())
	if err != nil {
		utilruntime.HandleError(err)
		return worker.StatusError
	}

	if !exists {
		// wait for the object to get created, which would trigger another reconcile
		return worker.StatusAllOK
	}

	policy := policyAny.(fedcorev1a1.GenericRefCountedPolicy)
	policy = policy.DeepCopyObject().(fedcorev1a1.GenericRefCountedPolicy)

	status := policy.GetRefCountedStatus()

	newRefCount := counter.GetPolicyCounts([]PolicyKey{PolicyKey(qualifiedName)})[0]

	hasChange := false
	if newRefCount != status.RefCount {
		status.RefCount = newRefCount
		hasChange = true
	}

	if hasChange {
		err := c.client.UpdateStatus(context.TODO(), policy)
		if err != nil {
			if apierrors.IsConflict(err) {
				return worker.StatusConflict
			}
			utilruntime.HandleError(err)
			return worker.StatusError
		}
	}

	return worker.StatusAllOK
}
