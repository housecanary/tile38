package similarity

import (
	"fmt"

	lua "github.com/yuin/gopher-lua"
)

func AdjustedSimilarity(algorithm string, algorithmParams *lua.LTable, scores, distances, ages []float64) ([]float64, error) {
	switch algorithm {
	case "classic":
		return adjustedSimilarityClassic(algorithmParams, scores, distances, ages)
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
