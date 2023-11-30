package preferencebinpack

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"
)

func Test_getDesiredPlan(t *testing.T) {
	type args struct {
		preferences               []*namedClusterPreferences
		estimatedCapacity         map[string]int64
		limitedCapacity           map[string]int64
		totalReplicas             int64
		keepUnschedulableReplicas bool
	}
	tests := []struct {
		name            string
		args            args
		desiredPlan     map[string]int64
		desiredOverflow map[string]int64
	}{
		{
			name: "All pods scheduled to cluster A",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity:         nil,
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: false,
			},
			desiredPlan: map[string]int64{
				"A": 10,
				"B": 0,
				"C": 0,
			},
			desiredOverflow: map[string]int64{},
		},
		{
			name: "Scheduled to cluster with estimated capacity",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity: map[string]int64{
					"A": 5,
				},
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: false,
			},
			desiredPlan: map[string]int64{
				"A": 5,
				"B": 5,
				"C": 0,
			},
			desiredOverflow: map[string]int64{"A": 5},
		},
		{
			name: "test",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity: map[string]int64{
					"A": 5,
				},
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: true,
			},
			desiredPlan: map[string]int64{
				"A": 5,
				"B": 5,
				"C": 0,
			},
			desiredOverflow: map[string]int64{
				"A": 5,
			},
		},
		{
			name: "test",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity: map[string]int64{
					"A": 5,
					"B": 3,
				},
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: true,
			},
			desiredPlan: map[string]int64{
				"A": 5,
				"B": 3,
				"C": 2,
			},
			desiredOverflow: map[string]int64{
				"A": 5,
				"B": 2,
			},
		},
		{
			name: "test",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity: map[string]int64{
					"A": 5,
					"B": 3,
				},
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: false,
			},
			desiredPlan: map[string]int64{
				"A": 5,
				"B": 3,
				"C": 2,
			},
			desiredOverflow: map[string]int64{"A": 5, "B": 2},
		},
		{
			name: "test",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity: map[string]int64{
					"A": 5,
				},
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: false,
			},
			desiredPlan: map[string]int64{
				"A": 5,
				"B": 0,
				"C": 0,
			},
			desiredOverflow: map[string]int64{},
		},
		{
			name: "test",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity: map[string]int64{
					"A": 5,
					"B": 3,
					"C": 1,
				},
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: false,
			},
			desiredPlan: map[string]int64{
				"A": 5,
				"B": 3,
				"C": 1,
			},
			desiredOverflow: map[string]int64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
		},
		{
			name: "test",
			args: args{
				preferences: []*namedClusterPreferences{
					{
						clusterName:        "A",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "B",
						ClusterPreferences: ClusterPreferences{},
					},
					{
						clusterName:        "C",
						ClusterPreferences: ClusterPreferences{},
					},
				},
				estimatedCapacity: map[string]int64{
					"A": 5,
					"B": 3,
					"C": 1,
				},
				limitedCapacity:           nil,
				totalReplicas:             10,
				keepUnschedulableReplicas: true,
			},
			desiredPlan: map[string]int64{
				"A": 5,
				"B": 3,
				"C": 1,
			},
			desiredOverflow: map[string]int64{
				"A": 5,
				"B": 2,
				"C": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getDesiredPlan(tt.args.preferences, tt.args.estimatedCapacity, tt.args.limitedCapacity, tt.args.totalReplicas)
			if !reflect.DeepEqual(got, tt.desiredPlan) {
				t.Errorf("getDesiredPlan() got = %v, desiredPlan %v", got, tt.desiredPlan)
			}
			if !reflect.DeepEqual(got1, tt.desiredOverflow) {
				t.Errorf("getDesiredPlan() got1 = %v, desiredPlan %v", got1, tt.desiredOverflow)
			}
		})
	}
}

type testCase struct {
	rsp             map[string]ClusterPreferences
	replicas        int64
	clusters        []string
	existing        map[string]int64
	capacity        map[string]int64
	limitedCapacity map[string]int64
	scheduledPods   map[string]int64
}

type expectedResult struct {
	plan     map[string]int64
	overflow map[string]int64
}

func estimateCapacity(currentReplicas, actualCapacity map[string]int64) map[string]int64 {
	estimatedCapacity := make(map[string]int64, len(actualCapacity))
	for cluster, c := range actualCapacity {
		if currentReplicas[cluster] > c {
			estimatedCapacity[cluster] = c
		}
	}

	return estimatedCapacity
}

func doCheck(
	t *testing.T,
	tc *testCase,
	avoidDisruption bool,
	keepUnschedulableReplicas bool,
	expected *expectedResult,
) {
	t.Helper()
	assert := assert.New(t)

	existing := tc.existing
	var plan, overflow, lastPlan, lastOverflow map[string]int64
	var err error

	converged := false
	const maxConvergenceSteps = 3
	for i := 0; i < maxConvergenceSteps; i++ {
		estimatedCapacity := estimateCapacity(existing, tc.capacity)
		plan, overflow, err = Plan(
			&ReplicaSchedulingPreference{
				Clusters: tc.rsp,
			},
			tc.replicas, tc.clusters,
			existing, estimatedCapacity, tc.limitedCapacity,
			avoidDisruption, keepUnschedulableReplicas, tc.scheduledPods,
		)

		assert.Nil(err)
		t.Logf("Step %v: avoidDisruption=%v keepUnschedulableReplicas=%v pref=%+v existing=%v estimatedCapacity=%v plan=%v overflow=%v\n",
			i, avoidDisruption, keepUnschedulableReplicas, tc.rsp, existing, estimatedCapacity, plan, overflow)

		// nil and empty map should be treated as equal
		planConverged := len(plan) == 0 && len(lastPlan) == 0 || reflect.DeepEqual(plan, lastPlan)
		overflowConverged := len(overflow) == 0 && len(lastOverflow) == 0 || reflect.DeepEqual(overflow, lastOverflow)
		converged = planConverged && overflowConverged
		if converged {
			// Break out of the loop if converged
			break
		}

		// Not converged yet, do another round
		existing = make(map[string]int64, len(plan))
		for cluster, replicas := range plan {
			existing[cluster] += replicas
		}
		for cluster, replicas := range overflow {
			existing[cluster] += replicas
		}

		lastPlan, lastOverflow = plan, overflow
	}

	if !converged {
		t.Errorf("did not converge after %v steps", maxConvergenceSteps)
	}

	if len(plan) != 0 || len(expected.plan) != 0 {
		assert.Equal(expected.plan, plan)
	}
	if len(overflow) != 0 || len(expected.overflow) != 0 {
		assert.Equal(expected.overflow, overflow)
	}
}

func doCheckWithoutExisting(
	t *testing.T,
	tc *testCase,
	expected *expectedResult,
) {
	// The replica distribution should be the same regardless of
	// avoidDisruption and keepUnschedulableReplicas.

	t.Helper()
	doCheck(t, tc, false, false, expected)
	doCheck(t, tc, false, true, expected)
	doCheck(t, tc, true, false, expected)
	doCheck(t, tc, true, true, expected)
}

func TestWithoutExisting(t *testing.T) {
	doCheckWithoutExisting(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 50,
			clusters: []string{"A", "B"},
		},
		&expectedResult{plan: map[string]int64{"A": 50, "B": 0}},
	)

	doCheckWithoutExisting(t,
		&testCase{
			rsp: map[string]ClusterPreferences{
				"B": {MinReplicas: 10},
			},
			replicas: 50,
			clusters: []string{"A", "B"},
		},
		&expectedResult{plan: map[string]int64{"A": 40, "B": 10}},
	)

	doCheckWithoutExisting(t,
		&testCase{
			rsp: map[string]ClusterPreferences{
				"A": {MaxReplicas: pointer.Int64(10)},
				"C": {MaxReplicas: pointer.Int64(21)},
				"D": {MaxReplicas: pointer.Int64(10)},
			},
			replicas: 50,
			clusters: []string{"A", "B", "C", "D"},
		},
		&expectedResult{plan: map[string]int64{"A": 10, "B": 40, "C": 0, "D": 0}},
	)

	doCheckWithoutExisting(t,
		&testCase{
			rsp: map[string]ClusterPreferences{
				"A": {MaxReplicas: pointer.Int64(10)},
				"C": {MaxReplicas: pointer.Int64(21)},
				"D": {MinReplicas: 10},
			},
			replicas: 50,
			clusters: []string{"A", "B", "C", "D", "E"},
		},
		&expectedResult{plan: map[string]int64{"A": 10, "B": 30, "C": 0, "D": 10, "E": 0}},
	)

	doCheckWithoutExisting(t,
		&testCase{
			rsp: map[string]ClusterPreferences{
				"A": {MaxReplicas: pointer.Int64(10)},
				"C": {MinReplicas: 10},
			},
			replicas:        50,
			clusters:        []string{"A", "B", "C", "D", "E"},
			limitedCapacity: map[string]int64{"B": 0},
		},
		&expectedResult{plan: map[string]int64{"A": 10, "B": 0, "C": 40, "D": 0, "E": 0}},
	)
}

func doCheckWithExisting(
	t *testing.T,
	tc *testCase,
	expected [2]*expectedResult,
) {
	// With existing, avoidDisruption should affect the distribution

	t.Helper()
	doCheck(t, tc, false, false, expected[0])
	doCheck(t, tc, false, true, expected[0])
	doCheck(t, tc, true, false, expected[1])
	doCheck(t, tc, true, true, expected[1])
}

func TestWithExisting(t *testing.T) {
	doCheckWithExisting(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 15,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 15, "B": 15, "C": 0},
		},
		[2]*expectedResult{
			{plan: map[string]int64{"A": 15, "B": 0, "C": 0}},
			{plan: map[string]int64{"A": 15, "B": 0, "C": 0}},
		},
	)

	doCheckWithExisting(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 18,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 10, "B": 2, "C": 7},
		},
		[2]*expectedResult{
			{plan: map[string]int64{"A": 18, "B": 0, "C": 0}},
			{plan: map[string]int64{"A": 10, "B": 1, "C": 7}},
		},
	)

	doCheckWithExisting(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 18,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 10, "B": 1, "C": 7},
		},
		[2]*expectedResult{
			{plan: map[string]int64{"A": 18, "B": 0, "C": 0}},
			{plan: map[string]int64{"A": 10, "B": 1, "C": 7}},
		},
	)

	doCheckWithExisting(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 18,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 10, "B": 1, "C": 1},
		},
		[2]*expectedResult{
			{plan: map[string]int64{"A": 18, "B": 0, "C": 0}},
			{plan: map[string]int64{"A": 16, "B": 1, "C": 1}},
		},
	)

	doCheckWithExisting(t,
		&testCase{
			rsp: map[string]ClusterPreferences{
				"A": {MaxReplicas: pointer.Int64(5)},
			},
			replicas: 18,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 10, "B": 1, "C": 1},
		},
		[2]*expectedResult{
			{plan: map[string]int64{"A": 5, "B": 13, "C": 0}},
			{plan: map[string]int64{"A": 10, "B": 7, "C": 1}},
		},
	)
}

func doCheckWithExistingAndCapacity(
	t *testing.T,
	tc *testCase,
	expected [4]*expectedResult,
) {
	// With existing, both avoidDisruption should affect the distribution

	t.Helper()
	//doCheck(t, tc, false, false, expected[0])
	//doCheck(t, tc, false, true, expected[1])
	doCheck(t, tc, true, false, expected[2])
	doCheck(t, tc, true, true, expected[3])
}

func TestWithExistingAndCapacity(t *testing.T) {
	doCheckWithExistingAndCapacity(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 60,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 60, "B": 0, "C": 0},
			capacity: map[string]int64{"A": 30},
		},
		[4]*expectedResult{
			{
				plan:     map[string]int64{"A": 30, "B": 30, "C": 0},
				overflow: map[string]int64{"A": 30},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 30, "C": 0},
				overflow: map[string]int64{"A": 30},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 30, "C": 0},
				overflow: map[string]int64{},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 30, "C": 0},
				overflow: map[string]int64{"A": 30},
			},
		},
	)

	doCheckWithExistingAndCapacity(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 60,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 60, "B": 30, "C": 0},
			capacity: map[string]int64{"A": 30, "B": 10},
		},
		[4]*expectedResult{
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{"A": 30, "B": 20},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{"A": 30, "B": 20},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{"A": 30, "B": 20},
			},
		},
	)

	doCheckWithExistingAndCapacity(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 60,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 60, "B": 30, "C": 20},
			capacity: map[string]int64{"A": 30, "B": 10},
		},
		[4]*expectedResult{
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{"A": 30, "B": 20},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{"A": 30, "B": 20},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{"A": 30, "B": 20},
			},
		},
	)

	doCheckWithExistingAndCapacity(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 60,
			clusters: []string{"C", "B", "A"},
			existing: map[string]int64{"A": 60, "B": 30, "C": 20},
			capacity: map[string]int64{"A": 30, "B": 10},
		},
		[4]*expectedResult{
			{
				plan:     map[string]int64{"A": 0, "B": 0, "C": 60},
				overflow: map[string]int64{},
			},
			{
				plan:     map[string]int64{"A": 0, "B": 0, "C": 60},
				overflow: map[string]int64{},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 20},
				overflow: map[string]int64{},
			},
		},
	)

	doCheckWithExistingAndCapacity(t,
		&testCase{
			rsp:      map[string]ClusterPreferences{},
			replicas: 70,
			clusters: []string{"A", "B", "C"},
			existing: map[string]int64{"A": 60, "B": 30, "C": 20},
			capacity: map[string]int64{"A": 30, "B": 10},
		},
		[4]*expectedResult{
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 30},
				overflow: map[string]int64{"A": 40, "B": 30},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 30},
				overflow: map[string]int64{"A": 40, "B": 30},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 30},
				overflow: map[string]int64{},
			},
			{
				plan:     map[string]int64{"A": 30, "B": 10, "C": 30},
				overflow: map[string]int64{"A": 40, "B": 30},
			},
		},
	)
}
