package topk

import (
	"database/sql"
	"time"
)

// The baseline MergeList algorithm without distinct posting list optimization.
func searchMergeList(db *sql.DB, listTable, setTable string, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool) ([]searchResult, experimentResult) {
	var expResult experimentResult

	start := time.Now()
	tokens, _, _ := tb.process(query)
	expResult.PreprocDuration = int(time.Now().Sub(start) / time.Millisecond)
	ac := newActionCollecter(len(tokens))
	start = time.Now()

	ac.start()
	counter := make(map[int64]int)
	for _, token := range tokens {
		entries := InvertedList(db, listTable, token)
		expResult.NumListRead++
		expResult.MaxListSizeRead = max(expResult.MaxListSizeRead, len(entries))
		ac.addReadList(len(entries))
		for _, entry := range entries {
			if ignoreSelf && entry.ID == query.ID {
				continue
			}
			if _, seen := counter[entry.ID]; seen {
				counter[entry.ID]++
			} else {
				counter[entry.ID] = 1
			}
		}
		expResult.MaxCounterSize = max(expResult.MaxCounterSize, len(counter))
	}
	h := &searchResultHeap{}
	for id, overlap := range counter {
		pushCandidate(h, k, id, overlap)
	}
	results := orderedResults(h)
	ac.done()

	expResult.Duration = int(time.Now().Sub(start) / time.Millisecond)
	expResult.Actions = ac.collect()
	expResult.Results = writeResultString(results)
	expResult.QueryID = query.ID
	expResult.QuerySize = len(query.RawTokens)
	expResult.NumResult = len(results)
	expResult.QueryNumToken = len(tokens)
	return results, expResult
}

// The baseline MergeList-D algorithm with distinct posting list optimization.
func searchMergeDistinctList(db *sql.DB, listTable, setTable string, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool) ([]searchResult, experimentResult) {
	var expResult experimentResult

	start := time.Now()
	tokens, _, gids := tb.process(query)
	expResult.PreprocDuration = int(time.Now().Sub(start) / time.Millisecond)
	ac := newActionCollecter(len(tokens))
	start = time.Now()

	ac.start()
	counter := make(map[int64]int)
	var numSkipped int
	querySize := len(tokens)

	for i := 0; i < querySize; i, numSkipped = nextDistinctList(tokens, gids, i) {
		token := tokens[i]
		skippedOverlap := numSkipped

		entries := InvertedList(db, listTable, token)
		expResult.NumListRead++
		expResult.MaxListSizeRead = max(expResult.MaxListSizeRead, len(entries))
		ac.addReadList(len(entries))
		for _, entry := range entries {
			if ignoreSelf && entry.ID == query.ID {
				continue
			}
			if _, seen := counter[entry.ID]; seen {
				counter[entry.ID] += skippedOverlap + 1
			} else {
				counter[entry.ID] = skippedOverlap + 1
			}
		}
		expResult.MaxCounterSize = max(expResult.MaxCounterSize, len(counter))
	}
	h := &searchResultHeap{}
	for id, overlap := range counter {
		pushCandidate(h, k, id, overlap)
	}
	results := orderedResults(h)
	ac.done()

	expResult.Duration = int(time.Now().Sub(start) / time.Millisecond)
	expResult.Actions = ac.collect()
	expResult.Results = writeResultString(results)
	expResult.QueryID = query.ID
	expResult.QuerySize = len(query.RawTokens)
	expResult.NumResult = len(results)
	expResult.QueryNumToken = len(tokens)
	return results, expResult
}
