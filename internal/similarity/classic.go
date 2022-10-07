package similarity

import (
	"math"

	lua "github.com/yuin/gopher-lua"
)

func adjustedSimilarityClassic(
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

func cdf(x float64, minx float64, mu float64, sigma float64) float64 {
	if sigma <= 0.0 {
		return 0
	}

	return 0.5*math.Erfc(-(x-mu)/(sigma*math.Sqrt2)) - 0.5*math.Erfc(-(minx-mu)/(sigma*math.Sqrt2))
}

func meanStdMinMax(data []float64) (mean, std, min, max float64) {
	if len(data) == 0 {
		return math.NaN(), math.NaN(), math.NaN(), math.NaN()
	}

	min = data[0]
	max = data[0]

	var n = len(data)
	var sum float64
	for i := 0; i < n; i++ {
		sum += data[i]

		if data[i] < min {
			min = data[i]
		}

		if data[i] > max {
			max = data[i]
		}
	}

	mean = sum / float64(n)
	for i := 0; i < n; i++ {
		diff := data[i] - mean
		std += diff * diff
	}

	std = math.Sqrt(std / float64(n))

	return mean, std, min, max
}
