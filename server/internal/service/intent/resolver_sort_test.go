package intent

import "testing"

func TestCov_SortCandidates_OrdersByScoreDesc(t *testing.T) {
	candidates := []CandidateSummary{
		{AgentID: "a1", Capability: "low", Score: 0.2},
		{AgentID: "a2", Capability: "high", Score: 0.9},
		{AgentID: "a3", Capability: "mid", Score: 0.5},
		{AgentID: "a4", Capability: "mid-low", Score: 0.3},
	}
	sortCandidates(candidates)
	if candidates[0].AgentID != "a2" || candidates[1].AgentID != "a3" ||
		candidates[2].AgentID != "a4" || candidates[3].AgentID != "a1" {
		t.Fatalf("candidates not sorted descending: %+v", candidates)
	}
}

func TestCov_SortCandidates_AlreadySortedAndEmpty(t *testing.T) {
	sorted := []CandidateSummary{
		{AgentID: "a1", Score: 0.9},
		{AgentID: "a2", Score: 0.1},
	}
	sortCandidates(sorted)
	if sorted[0].AgentID != "a1" {
		t.Fatalf("already-sorted slice changed: %+v", sorted)
	}

	sortCandidates(nil)
	sortCandidates([]CandidateSummary{{AgentID: "solo", Score: 1.0}})
}
