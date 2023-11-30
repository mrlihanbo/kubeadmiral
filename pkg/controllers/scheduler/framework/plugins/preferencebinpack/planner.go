/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This file may have been modified by The KubeAdmiral Authors
("KubeAdmiral Modifications"). All KubeAdmiral Modifications
are Copyright 2023 The KubeAdmiral Authors.
*/

package preferencebinpack

import (
	"k8s.io/klog/v2"
)

// ClusterPreferences regarding number of replicas assigned to a cluster workload object (dep, rs, ..) within
// a federated workload object.
type ClusterPreferences struct {
	// Minimum number of replicas that should be assigned to this cluster workload object. 0 by default.
	MinReplicas int64

	// Maximum number of replicas that should be assigned to this cluster workload object.
	// Unbounded if no value provided (default).
	MaxReplicas *int64
}

type ReplicaSchedulingPreference struct {
	// A mapping between cluster names and preferences regarding a local workload object (dep, rs, .. ) in
	// these clusters.
	// "*" (if provided) applies to all clusters if an explicit mapping is not provided.
	// If omitted, clusters without explicit preferences should not have any replicas scheduled.
	Clusters map[string]ClusterPreferences
}

type namedClusterPreferences struct {
	clusterName string
	ClusterPreferences
}

func Plan(
	rsp *ReplicaSchedulingPreference,
	totalReplicas int64,
	availableClusters []string,
	currentReplicaCount map[string]int64,
	estimatedCapacity map[string]int64,
	oldEstimatedCapacity map[string]int64,
	limitedCapacity map[string]int64,
	avoidDisruption bool,
	keepUnschedulableReplicas bool,
) (map[string]int64, map[string]int64, error) {
	namedPreferences := make([]*namedClusterPreferences, 0, len(availableClusters))
	for _, cluster := range availableClusters {
		namedPreferences = append(namedPreferences, &namedClusterPreferences{
			clusterName:        cluster,
			ClusterPreferences: rsp.Clusters[cluster],
		})
	}

	// If keepUnschedulableReplicas is false,
	// the resultant plan will likely violate the preferences
	// if any cluster has limited capacity.
	// If avoidDisruption is also false, a subsequent reschedule will restore
	// the replica distribution to the state before we moved the unschedulable
	// replicas. This leads to a infinite reschedule loop and is undesirable.
	// Therefore we default to keeping the unschedulable replicas if avoidDisruption
	// is false.
	if !avoidDisruption {
		keepUnschedulableReplicas = true
	}

	desiredPlan, desiredOverflow := getDesiredPlan(
		namedPreferences,
		estimatedCapacity,
		oldEstimatedCapacity,
		limitedCapacity,
		totalReplicas,
		keepUnschedulableReplicas,
	)

	// If we don't desiredPlan to avoid migration, just return the plan computed from preferences
	if !avoidDisruption {
		return desiredPlan, desiredOverflow, nil
	}

	// Try to avoid instance migration between clusters

	var currentTotalOkReplicas int64
	// currentPlan should only contain clusters in availableClusters
	currentPlan := make(map[string]int64, len(namedPreferences))
	for _, preference := range namedPreferences {
		replicas := currentReplicaCount[preference.clusterName]
		if capacity, exists := limitedCapacity[preference.clusterName]; exists && capacity < replicas {
			replicas = capacity
		}
		if capacity, exists := estimatedCapacity[preference.clusterName]; exists && capacity < replicas {
			replicas = capacity
		}
		currentPlan[preference.clusterName] = replicas

		currentTotalOkReplicas += replicas
	}

	var desiredTotalReplicas int64
	for _, replicas := range desiredPlan {
		desiredTotalReplicas += replicas
	}

	klog.V(4).Infof("[===binpack] for currentTotalOkReplicas: %v, desiredTotalReplicas: %v", desiredTotalReplicas, totalReplicas)

	klog.V(4).Infof("[===binpack] for estimatedCapacity: %v", estimatedCapacity)

	klog.V(4).Infof("[===binpack] for currentPlan: %v", currentPlan)

	klog.V(4).Infof("[===binpack] desiredPlan: %v, desiredOverflow: %v", desiredPlan, desiredOverflow)

	switch {
	case currentTotalOkReplicas == desiredTotalReplicas:
		return currentPlan, desiredOverflow, nil
	case currentTotalOkReplicas > desiredTotalReplicas:
		plan, err := scaleDown(
			currentPlan, desiredPlan,
			currentTotalOkReplicas-desiredTotalReplicas,
			availableClusters,
		)
		if err != nil {
			return nil, nil, err
		}
		return plan, desiredOverflow, nil
	default:
		plan, err := scaleUp(
			rsp,
			currentPlan, desiredPlan,
			desiredTotalReplicas-currentTotalOkReplicas,
			availableClusters,
		)
		if err != nil {
			return nil, nil, err
		}
		return plan, desiredOverflow, nil
	}
}

func getDesiredPlan(
	preferences []*namedClusterPreferences,
	estimatedCapacity map[string]int64,
	oldEstimatedCapacity map[string]int64,
	limitedCapacity map[string]int64,
	totalReplicas int64,
	keepUnschedulableReplicas bool,
) (map[string]int64, map[string]int64) {
	remainingReplicas := totalReplicas
	plan := make(map[string]int64, len(preferences))
	overflow := make(map[string]int64, len(preferences))

	// Assign each cluster the minimum number of replicas it requested.
	for _, preference := range preferences {
		min := minInt64(preference.MinReplicas, remainingReplicas)
		if capacity, hasCapacity := limitedCapacity[preference.clusterName]; hasCapacity && capacity < min {
			min = capacity
		}
		if capacity, hasCapacity := estimatedCapacity[preference.clusterName]; hasCapacity && capacity < min {
			overflow[preference.clusterName] = min - capacity
			min = capacity
		}
		remainingReplicas -= min
		plan[preference.clusterName] = min
	}

	modified := true
	// It is possible single pass of the loop is not enough to distribute all replicas among clusters due
	// to weight, max and rounding corner cases. In such case we iterate until either
	// there is no replicas or no cluster gets any more replicas. Every loop either distributes all remainingReplicas
	// or maxes out at least one cluster.
	for modified && remainingReplicas > 0 {
		modified = false

		newPreferences := make([]*namedClusterPreferences, 0, len(preferences))
		distributeInThisLoop := remainingReplicas
		for _, preference := range preferences {
			start := plan[preference.clusterName]
			extra := minInt64(distributeInThisLoop, remainingReplicas)

			// In total there should be the amount that was there at start plus whatever is due
			// in this iteration
			total := start + extra

			// Check if we don't overflow the cluster, and if yes don't consider this cluster
			// in any of the following iterations.
			full := false
			if preference.MaxReplicas != nil && total > *preference.MaxReplicas {
				total = *preference.MaxReplicas
				full = true
			}
			if capacity, hasCapacity := limitedCapacity[preference.clusterName]; hasCapacity && total > capacity {
				total = capacity
				full = true
			}

			if capacity, hasCapacity := oldEstimatedCapacity[preference.clusterName]; hasCapacity && total > capacity {
				overflow[preference.clusterName] += total - capacity
				total = capacity
				full = true
			}

			if capacity, hasCapacity := estimatedCapacity[preference.clusterName]; hasCapacity && total > capacity {
				overflow[preference.clusterName] += total - capacity
				total = capacity
				full = true
			}
			if !full {
				newPreferences = append(newPreferences, preference)
			}

			// Only total-start replicas were actually taken.
			remainingReplicas -= total - start
			plan[preference.clusterName] = total

			// Something extra got scheduled on this cluster.
			if total > start {
				modified = true
			}
		}
		preferences = newPreferences
	}

	// If we desiredPlan to keep the unschedulable replicas in their original
	// clusters, we return the overflow (which contains these
	// unschedulable replicas) as is.
	if keepUnschedulableReplicas {
		return plan, overflow
	}

	// Otherwise, trim overflow at the level
	// of replicas that the algorithm failed to place anywhere.
	newOverflow := make(map[string]int64)
	for key, value := range overflow {
		value = minInt64(value, remainingReplicas)
		if value > 0 {
			newOverflow[key] = value
		}
	}
	return plan, newOverflow
}

func scaleUp(
	rsp *ReplicaSchedulingPreference,
	currentReplicaCount, desiredReplicaCount map[string]int64,
	scaleUpCount int64,
	availableClusters []string,
) (map[string]int64, error) {
	namedPreferences := make([]*namedClusterPreferences, 0, len(availableClusters))
	for _, cluster := range availableClusters {
		current := currentReplicaCount[cluster]
		desired := desiredReplicaCount[cluster]
		if desired > current {
			klog.V(4).Infof("[===scaleUp]cluster: %+v", cluster)
			pref := &namedClusterPreferences{
				clusterName: cluster,
			}
			pref.MinReplicas = rsp.Clusters[cluster].MinReplicas
			if rsp.Clusters[cluster].MaxReplicas != nil {
				// note that max is always positive because MaxReplicas >= desired > current
				max := *rsp.Clusters[cluster].MaxReplicas - current
				pref.MaxReplicas = &max
			}
			namedPreferences = append(namedPreferences, pref)
		}
	}

	// no estimatedCapacity and hence no overflow
	replicasToScaleUp, _ := getDesiredPlan(namedPreferences, nil, nil, nil, scaleUpCount, true)
	for cluster, count := range replicasToScaleUp {
		currentReplicaCount[cluster] += count
	}

	klog.V(4).Infof("[===scaleUp]plan: %+v, scaleUp: %v",
		currentReplicaCount, scaleUpCount)

	return currentReplicaCount, nil
}

func scaleDown(
	currentReplicaCount, desiredReplicaCount map[string]int64,
	scaleDownCount int64,
	availableClusters []string,
) (map[string]int64, error) {
	namedPreferences := make([]*namedClusterPreferences, 0, len(availableClusters))
	for _, cluster := range availableClusters {
		current := currentReplicaCount[cluster]
		desired := desiredReplicaCount[cluster]
		if desired < current {
			klog.V(4).Infof("[===scaleDown]cluster: %+v", cluster)
			namedPreferences = append([]*namedClusterPreferences{{
				clusterName: cluster,
				ClusterPreferences: ClusterPreferences{
					MaxReplicas: &current,
				},
			}}, namedPreferences...)
		}
	}

	// no estimatedCapacity and hence no overflow
	replicasToScaleDown, _ := getDesiredPlan(namedPreferences, nil, nil, nil, scaleDownCount, true)
	for cluster, count := range replicasToScaleDown {
		currentReplicaCount[cluster] -= count
	}

	klog.V(4).Infof("[===scaleDown]plan: %+v, scaleDownCount: %v",
		currentReplicaCount, scaleDownCount)

	return currentReplicaCount, nil
}

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
