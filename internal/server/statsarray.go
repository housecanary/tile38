package server

import (
	"container/heap"
	"math"
	"sort"
)

type statsArray struct {
	xs      []float64
	summary *summary
}

type summary struct {
	mean              float64
	standardDeviation float64
	min               float64
	max               float64
}

func (a *statsArray) Copy() *statsArray {
	result := statsArray{
		xs: make([]float64, len(a.xs)),
	}

	copy(result.xs, a.xs)

	if a.summary != nil {
		result.summary = &summary{
			a.summary.mean,
			a.summary.standardDeviation,
			a.summary.min,
			a.summary.max,
		}
	}

	return &result
}

func (a *statsArray) Append(x float64) {
	a.xs = append(a.xs, x)
}

func (a *statsArray) Mean() float64 {
	return a.summarize().mean
}

func (a *statsArray) Min() float64 {
	return a.summarize().min
}

func (a *statsArray) Max() float64 {
	return a.summarize().max
}

func (a *statsArray) StandardDeviation() float64 {
	return a.summarize().standardDeviation
}

func (a *statsArray) summarize() *summary {
	if a.summary != nil {
		return a.summary
	}

	var s summary
	if len(a.xs) > 0 {
		m2 := float64(0)
		min := a.xs[0]
		max := a.xs[0]

		// Note: Welford's algorithm is used to minimize floating point error
		for i, x := range a.xs {
			n := float64(i + 1)
			delta := x - s.mean
			s.mean += delta / n
			delta2 := x - s.mean
			m2 += delta * delta2

			if x < min {
				min = x
			} else if x > max {
				max = x
			}
		}
		s.standardDeviation = math.Sqrt(m2 / float64(len(a.xs)))
		s.min = min
		s.max = max
	}
	a.summary = &s
	return a.summary
}

func (a *statsArray) CDF() {
	μ, σ := a.Mean(), a.StandardDeviation()
	for i, x := range a.xs {
		a.xs[i] = 0.5 * (1 + math.Erf((x-μ)/(σ*math.Sqrt2)))
	}
}

func (a *statsArray) CDFOf(x float64) float64 {
	μ, σ := a.Mean(), a.StandardDeviation()
	return 0.5 * (1 + math.Erf((x-μ)/(σ*math.Sqrt2)))
}

func (a *statsArray) MinIndexes(n int) []int {
	return a.minMaxIndexes(n, true)
}

func (a *statsArray) MaxIndexes(n int) []int {
	return a.minMaxIndexes(n, false)
}

func (a *statsArray) minMaxIndexes(n int, min bool) []int {
	if n > len(a.xs) {
		n = len(a.xs)
	}
	indexAry := make([]int, n)
	for i := 0; i < n; i++ {
		indexAry[i] = i
	}

	var s sort.Interface = sortStatsArrayIndexes{
		xs:      a.xs,
		indexes: indexAry,
	}

	if min {
		s = sort.Reverse(s)
	}
	heap.Init(fixedSizeHeap{s})

	for i := n; i < len(a.xs); i++ {
		x := a.xs[i]
		bubble := a.xs[indexAry[0]]
		if (min && x < bubble) || (!min && x > bubble) {
			indexAry[0] = i
			heap.Fix(fixedSizeHeap{s}, 0)
		}
	}
	sort.Sort(sort.Reverse(s))
	return indexAry
}

func (a *statsArray) Clamp(min, max float64) {
	for i, x := range a.xs {
		if x < min {
			a.xs[i] = min
		}
		if x > max {
			a.xs[i] = max
		}
	}
}

func (a *statsArray) MultArray(b *statsArray) {
	a.applyArray(b, func(x, y float64) float64 {
		return x * y
	})
}

func (a *statsArray) MultScalar(b float64) {
	a.applyScalar(b, func(x, y float64) float64 {
		return x * y
	})
}

func (a *statsArray) DivArray(b *statsArray) {
	a.applyArray(b, func(x, y float64) float64 {
		return x / y
	})
}

func (a *statsArray) DivScalar(b float64) {
	a.applyScalar(b, func(x, y float64) float64 {
		return x / y
	})
}

func (a *statsArray) AddArray(b *statsArray) {
	a.applyArray(b, func(x, y float64) float64 {
		return x + y
	})
}

func (a *statsArray) AddScalar(b float64) {
	a.applyScalar(b, func(x, y float64) float64 {
		return x + y
	})
}

func (a *statsArray) SubArray(b *statsArray) {
	a.applyArray(b, func(x, y float64) float64 {
		return x - y
	})
}

func (a *statsArray) SubScalar(b float64) {
	a.applyScalar(b, func(x, y float64) float64 {
		return x - y
	})
}

func (a *statsArray) applyScalar(b float64, f func(x, y float64) float64) {
	n := len(a.xs)
	for i := 0; i < n; i++ {
		a.xs[i] = f(a.xs[i], b)
	}
	a.xs = a.xs[0:n]
}

func (a *statsArray) applyArray(b *statsArray, f func(x, y float64) float64) {
	n := len(a.xs)
	if len(b.xs) < n {
		n = len(b.xs)
	}
	for i := 0; i < n; i++ {
		a.xs[i] = f(a.xs[i], b.xs[i])
	}
	a.xs = a.xs[0:n]
}

type sortStatsArrayIndexes struct {
	xs      []float64
	indexes []int
}

func (s sortStatsArrayIndexes) Len() int           { return len(s.indexes) }
func (s sortStatsArrayIndexes) Less(i, j int) bool { return s.xs[s.indexes[i]] < s.xs[s.indexes[j]] }
func (s sortStatsArrayIndexes) Swap(i, j int) {
	s.indexes[i], s.indexes[j] = s.indexes[j], s.indexes[i]
}

type fixedSizeHeap struct {
	sort.Interface
}

func (fixedSizeHeap) Push(x any) {}
func (fixedSizeHeap) Pop() any   { return nil }
