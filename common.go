package topk

import "math"

var (
	totalNumberOfSets float64
)

func init() {
	// this can be optionally set at start
	totalNumberOfSets = 1.0
}

func pruningPowerUb(freq, k int) float64 {
	return math.Log((float64(min(k, freq)) + 0.5) * (totalNumberOfSets - float64(k) - float64(freq) + float64(min(k, freq)) + 0.5) /
		((float64(max(0, k-freq)) + 0.5) * (float64(max(freq-k, 0)) + 0.5)))
}

func inverseSetFrequency(freq int) float64 {
	return math.Log(totalNumberOfSets / float64(freq))
}

func nextDistinctList(tokens, gids []int64, currListIndex int) (listIndex, numSkipped int) {
	if currListIndex == len(tokens)-1 {
		return len(tokens), 0
	}
	for i := currListIndex + 1; i < len(tokens); i++ {
		if i < len(tokens)-1 && gids[i+1] == gids[i] {
			numSkipped++
			continue
		}
		listIndex = i
		break
	}
	return
}

func overlap(setTokens, queryTokens []int64) int {
	var i, j int
	var overlap int
	for i < len(queryTokens) && j < len(setTokens) {
		switch d := queryTokens[i] - setTokens[j]; {
		case d == 0:
			overlap++
			i++
			j++
		case d < 0:
			i++
		case d > 0:
			j++
		}
	}
	return overlap
}

func overlapAndUpdateCounts(setTokens, queryTokens []int64, counts []int) int {
	var i, j int
	var overlap int
	for i < len(queryTokens) && j < len(setTokens) {
		switch d := queryTokens[i] - setTokens[j]; {
		case d == 0:
			counts[i]--
			overlap++
			i++
			j++
		case d < 0:
			i++
		case d > 0:
			j++
		}
	}
	return overlap
}
