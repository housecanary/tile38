package similarity

import (
	"fmt"
	"math"

	lua "github.com/yuin/gopher-lua"
)

func AdjustedSimilarityScores(algorithm string, algorithmParams *lua.LTable, scores, distances, ages []float64) ([]float64, error) {
	switch algorithm {
	case "classic":
		return adjustedSimilarityScoresClassic(algorithmParams, scores, distances, ages)
	default:
		return nil, fmt.Errorf("similarity %v algorithm not implemented", algorithm)
	}
}

func getParameterNumber[T float64](algorithmParams *lua.LTable, name string, defaultValue T) (T, error) {
	value := algorithmParams.RawGetString(name)

	if value == lua.LNil {
		return defaultValue, nil
	}

	switch converted := value.(type) {
	case lua.LNumber:
		return T(converted), nil
	default:
		return 0, fmt.Errorf("parameter parsing not implemented for %v", value.Type())

	}
}

func getParameterString[T string](algorithmParams *lua.LTable, name string, defaultValue T) (T, error) {
	value := algorithmParams.RawGetString(name)

	if value == lua.LNil {
		return defaultValue, nil
	}

	switch converted := value.(type) {
	case lua.LString:
		return T(converted), nil
	default:
		return "", fmt.Errorf("parameter parsing not implemented for %v", value.Type())
	}
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
