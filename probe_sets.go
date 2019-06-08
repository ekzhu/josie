package topk

import (
	"database/sql"
	"time"
)

// the base line algorithm that combines prefix filter and position filter
func searchProbeSetSuffix(db *sql.DB, listTable, setTable string, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool) ([]searchResult, experimentResult) {
	var expResult experimentResult
	ac := newActionCollecter(len(query.RawTokens))

	start := time.Now()
	tokens, _, _ := tb.process(query)
	expResult.PreprocDuration = int(time.Now().Sub(start) / time.Millisecond)
	start = time.Now()

	var ignores map[int64]bool
	if ignoreSelf {
		ignores = map[int64]bool{query.ID: true}
	} else {
		ignores = make(map[int64]bool)
	}
	h := &searchResultHeap{}
	ac.start()
	for i, token := range tokens {
		if kthOverlap(h, k) >= len(tokens)-i {
			break
		}
		entries := InvertedList(db, listTable, token)
		expResult.MaxListSizeRead = max(expResult.MaxListSizeRead, len(entries))
		expResult.NumListRead++
		ac.addReadList(len(entries))
		for _, entry := range entries {
			if _, yes := ignores[entry.ID]; yes {
				continue
			}
			ignores[entry.ID] = true
			if kthOverlap(h, k) >= min(len(tokens)-i, entry.Size-entry.MatchPosition) {
				continue
			}
			s := setTokensSuffix(db, setTable, entry.ID, entry.MatchPosition)
			expResult.NumSetRead++
			expResult.MaxSetSizeRead = max(expResult.MaxSetSizeRead, len(s))
			o := overlap(s, tokens[i:])
			pushCandidate(h, k, entry.ID, o)
			ac.addReadSet(len(s), o)
		}
	}
	results := orderedResults(h)
	ac.done()

	expResult.Duration = int(time.Now().Sub(start) / time.Millisecond)
	expResult.Results = writeResultString(results)
	expResult.QueryID = query.ID
	expResult.QuerySize = len(query.RawTokens)
	expResult.NumResult = len(results)
	expResult.IgnoreSize = len(ignores)
	expResult.QueryNumToken = len(tokens)
	expResult.Actions = ac.collect()
	return results, expResult
}

// optimized using distinct lists
func searchProbeSetOptimized(db *sql.DB, listTable, setTable string, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool) ([]searchResult, experimentResult) {
	var expResult experimentResult
	ac := newActionCollecter(len(query.RawTokens))

	start := time.Now()
	tokens, _, gids := tb.process(query)
	expResult.PreprocDuration = int(time.Now().Sub(start) / time.Millisecond)
	start = time.Now()

	var ignores map[int64]bool
	if ignoreSelf {
		ignores = map[int64]bool{query.ID: true}
	} else {
		ignores = make(map[int64]bool)
	}
	h := &searchResultHeap{}
	ac.start()
	var numSkipped int
	querySize := len(tokens)

	for i := 0; i < querySize; i, numSkipped = nextDistinctList(tokens, gids, i) {
		token := tokens[i]
		skippedOverlap := numSkipped

		if kthOverlap(h, k) >= len(tokens)-i+skippedOverlap {
			break
		}
		entries := InvertedList(db, listTable, token)
		expResult.MaxListSizeRead = max(expResult.MaxListSizeRead, len(entries))
		expResult.NumListRead++
		ac.addReadList(len(entries))
		for _, entry := range entries {
			if _, yes := ignores[entry.ID]; yes {
				continue
			}
			ignores[entry.ID] = true
			if kthOverlap(h, k) >= min(len(tokens)-i+skippedOverlap, entry.Size-entry.MatchPosition+skippedOverlap) {
				continue
			}
			s := setTokensSuffix(db, setTable, entry.ID, entry.MatchPosition)
			expResult.NumSetRead++
			expResult.MaxSetSizeRead = max(expResult.MaxSetSizeRead, len(s))
			o := overlap(s, tokens[i:])
			o += skippedOverlap
			pushCandidate(h, k, entry.ID, o)
			ac.addReadSet(len(s), o)
		}
	}
	results := orderedResults(h)
	ac.done()

	expResult.Duration = int(time.Now().Sub(start) / time.Millisecond)
	expResult.Results = writeResultString(results)
	expResult.QueryID = query.ID
	expResult.QuerySize = len(query.RawTokens)
	expResult.NumResult = len(results)
	expResult.IgnoreSize = len(ignores)
	expResult.QueryNumToken = len(tokens)
	expResult.Actions = ac.collect()
	return results, expResult
}
