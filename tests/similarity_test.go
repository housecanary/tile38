package tests

import "testing"

func subTestSimilarity(t *testing.T, mc *mockServer) {
	runStep(t, mc, "SCORE_ADJUSTED", scripts_SCORE_ADJUSTED_test)
}

func scripts_SCORE_ADJUSTED_test(mc *mockServer) error {
	script_score_adjusted := `
		local algorithm = {algorithm="classic"}
		local scores = {[1]=99, [2]=88, [3]=77}
		local distances = {[1]=100, [2]=50, [3]=200}
		local ages = {[1]=100, [2]=200, [3]=300}

		local adjusted_scores = tile38.adjusted_similarity_scores(
			algorithm, scores, distances, ages)

		return adjusted_scores
	`

	script_score_adjusted_params := `
		local algorithm = {algorithm="classic",distMaxPenalty=10,ageMaxPenalty=20}
		local scores = {[1]=99, [2]=88, [3]=77}
		local distances = {[1]=100, [2]=50, [3]=200}
		local ages = {[1]=100, [2]=200, [3]=300}

		local adjusted_scores = tile38.adjusted_similarity_scores(
			algorithm, scores, distances, ages)

		return adjusted_scores
	`

	return mc.DoBatch([][]interface{}{
		{"EVAL", script_score_adjusted, 0}, {"[[1 98] [2 80] [3 58]]"},
		{"EVAL", script_score_adjusted_params, 0}, {"[[1 96] [2 80] [3 53]]"},
	})
}
