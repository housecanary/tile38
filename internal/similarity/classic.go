package similarity

import (
	lua "github.com/yuin/gopher-lua"
)

func adjustedSimilarityScoresClassic(
	algorithmParams *lua.LTable,
	scores, distances, ages []float64,
) ([]float64, error) {
	meanDist, stDevDist, minDist, _ := meanStdMinMax(distances)
	meanAge, stDevAge, minAge, _ := meanStdMinMax(ages)

	var err error
	var distMaxPenalty, ageMaxPenalty float64

	if distMaxPenalty, err = getParameterNumber(algorithmParams, "distMaxPenalty", 10.); err != nil {
		return nil, err
	}

	if ageMaxPenalty, err = getParameterNumber(algorithmParams, "ageMaxPenalty", 10.); err != nil {
		return nil, err
	}

	res := make([]float64, len(scores))

	for i := range scores {
		score := adjustedSimilarityScoreClassic(
			meanDist, stDevDist, minDist,
			meanAge, stDevAge, minAge,
			distances[i], ages[i], scores[i],
			distMaxPenalty, ageMaxPenalty,
		)

		res[i] = score
	}

	return res, nil
}

func adjustedSimilarityScoreClassic(
	meanDist float64, stDevDist float64, minDist float64,
	meanAge float64, stDevAge float64, minAge float64,
	distMiles float64, ageOfTX float64, simScore float64,
	distMaxPenalty float64, ageMaxPenalty float64,
) float64 {
	cdfDist := cdf(distMiles, minDist, meanDist, stDevDist)
	cdtAge := cdf(ageOfTX, minAge, meanAge, stDevAge)

	adj := simScore - cdfDist*distMaxPenalty - cdtAge*ageMaxPenalty

	if adj < 0 {
		adj = 0
	}
	if adj > 100 {
		adj = 100
	}
	return adj
}
