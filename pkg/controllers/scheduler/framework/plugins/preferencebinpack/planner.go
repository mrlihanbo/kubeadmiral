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
	limitedCapacity map[string]int64,
	avoidDisruption bool,
	keepUnschedulableReplicas bool,
	scheduledPods map[string]int64,
) (map[string]int64, map[string]int64, error) {
	namedPreferences := make([]*namedClusterPreferences, 0, len(availableClusters))
	for _, cluster := range availableClusters {
		namedPreferences = append(namedPreferences, &namedClusterPreferences{
			clusterName:        cluster,
			ClusterPreferences: rsp.Clusters[cluster],
		})
	}

	if !avoidDisruption {
		keepUnschedulableReplicas = true
	}

	desiredPlan, desiredOverflow := getDesiredPlan(
		namedPreferences,
		estimatedCapacity,
		limitedCapacity,
		totalReplicas,
	)

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

		if scheduled, exist := scheduledPods[preference.clusterName]; !exist {
			replicas = 0
		} else if scheduled < replicas {
			replicas = scheduled
		}

		currentTotalOkReplicas += replicas
	}

	var desiredTotalReplicas int64
	for _, replicas := range desiredPlan {
		desiredTotalReplicas += replicas
	}

	klog.V(4).Infof(
		"[===binpack] for currentTotalOkReplicas: %v, desiredTotalReplicas: %v", currentTotalOkReplicas, desiredTotalReplicas)

	klog.V(4).Infof(
		"[===binpack] for estimatedCapacity: %v, scheduledPods: %v", estimatedCapacity, scheduledPods)

	klog.V(4).Infof("[===binpack] for currentPlan: %v", currentPlan)

	klog.V(4).Infof("[===binpack] desiredPlan: %v, desiredOverflow: %v", desiredPlan, desiredOverflow)

	if currentTotalOkReplicas == desiredTotalReplicas && !keepUnschedulableReplicas {
		desiredOverflow = nil
	}

	// If we don't desiredPlan to avoid migration, just return the plan computed from preferences
	if !avoidDisruption {
		return desiredPlan, desiredOverflow, nil
	}

	// Try to avoid instance migration between clusters
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
	limitedCapacity map[string]int64,
	totalReplicas int64,
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
	for modified && remainingReplicas > 0 {
		modified = false

		newPreferences := make([]*namedClusterPreferences, 0, len(preferences))
		for _, preference := range preferences {
			start := plan[preference.clusterName]
			extra := remainingReplicas

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

	return plan, overflow
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
	replicasToScaleUp, _ := getDesiredPlan(namedPreferences, nil, nil, scaleUpCount)
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
	replicasToScaleDown, _ := getDesiredPlan(namedPreferences, nil, nil, scaleDownCount)
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
