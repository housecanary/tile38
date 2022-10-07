package tests

import "testing"

func subTestSimilarity(t *testing.T, mc *mockServer) {
	runStep(t, mc, "SCORE_ADJUSTED", scripts_SCORE_ADJUSTED_test)
}

func scripts_SCORE_ADJUSTED_test(mc *mockServer) error {
	script_score_adjusted := `
		local function process(iterator)
			result[#result + 1] = iterator.id
			return false  -- early stop, after the first object
		end

		local algorithm = {algorithm="classic"}
		local scores = {1: 99, 2: 88, 3: 77}
		local distances = {1: 100, 2: 50, 3: 200}
		local ages = {1: 100, 2: 200, 3: 300}

		adjusted_scores = tile38.adjusted_similarity_scores(
			algorithm, scores, distances, ages)

		return adjusted_scores
	`

	return mc.DoBatch([][]interface{}{
		{"EVAL", script_score_adjusted, 0}, {"[1 [poly9]]"}, // early stop, cursor = 1
	})
}
