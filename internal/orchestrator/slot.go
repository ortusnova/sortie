package orchestrator

import "strings"

// GlobalAvailableSlots returns the number of dispatch slots available under
// the global concurrency limit. The result is never negative.
func GlobalAvailableSlots(maxConcurrentAgents, runningCount int) int {
	return max(maxConcurrentAgents-runningCount, 0)
}

// AgentsRunning counts the running entries that hold an agent slot: all
// but those whose remote job waits in the matcher's queue for a worker.
// Such a job takes no worker's run slot yet, so it takes no agent slot.
func AgentsRunning(running map[string]*RunningEntry) int {
	n := 0
	for _, e := range running {
		if !e.WorkerQueued {
			n++
		}
	}
	return n
}

// StateAvailableSlots returns the number of dispatch slots available for the
// given issue state. If a per-state limit exists in maxConcurrentByState, it
// returns max(limit - stateRunningCount, 0). If no per-state limit exists,
// it returns globalAvailable as a fallback. The state parameter is normalized
// to lowercase before lookup.
func StateAvailableSlots(state string, maxConcurrentByState map[string]int, stateRunningCount, globalAvailable int) int {
	normalized := strings.ToLower(state)
	if limit, ok := maxConcurrentByState[normalized]; ok {
		return max(limit-stateRunningCount, 0)
	}
	return globalAvailable
}

// HasAvailableSlots reports whether dispatch is permitted for an issue in
// the given state, considering both global and per-state limits.
func HasAvailableSlots(s *State, issueState string) bool {
	globalAvail := GlobalAvailableSlots(s.MaxConcurrentAgents, AgentsRunning(s.Running))
	if globalAvail == 0 {
		return false
	}
	stateRunning := RunningCountByState(s.Running, issueState)
	stateAvail := StateAvailableSlots(issueState, s.MaxConcurrentByState, stateRunning, globalAvail)
	return stateAvail > 0
}
