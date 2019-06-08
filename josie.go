package topk

import (
	"database/sql"
	"sort"
	"time"
)

// This is the JOSIE algorithm presented in the SIGMOD paper.
func searchMergeProbeCostModelGreedy(
	db *sql.DB,
	listTable,
	setTable string,
	tb tokenTable,
	query rawTokenSet,
	k int,
	ignoreSelf bool,
) ([]searchResult, experimentResult) {
	var expResult experimentResult

	start := time.Now()
	tokens, freqs, gids := tb.process(query)
	readListCosts := make([]float64, len(freqs))
	for i := 0; i < len(freqs); i++ {
		if i == 0 {
			readListCosts[i] = readListCost(freqs[i] + 1)
		} else {
			readListCosts[i] = readListCosts[i-1] + readListCost(freqs[i]+1)
		}
	}
	expResult.PreprocDuration = int(time.Now().Sub(start) / time.Millisecond)
	start = time.Now()

	querySize := len(tokens)
	counter := make(map[int64]*candidateEntry)
	var ignores map[int64]bool
	if ignoreSelf {
		ignores = map[int64]bool{query.ID: true}
	} else {
		ignores = make(map[int64]bool)
	}
	h := &searchResultHeap{}
	var numSkipped int

	currBatchLists := batchSize

	for i := 0; i < querySize; i, numSkipped = nextDistinctList(tokens, gids, i) {
		token := tokens[i]
		skippedOverlap := numSkipped
		maxOverlapUnseenCandidate := upperboundOverlapUknownCandidate(querySize,
			i, skippedOverlap)

		// Early terminates once the threshold index has reached and
		// there is no remaining sets in the counter
		if kthOverlap(h, k) >= maxOverlapUnseenCandidate && len(counter) == 0 {
			break
		}

		// Read the list
		entries := InvertedList(db, listTable, token)
		expResult.NumListRead++
		expResult.MaxListSizeRead = max(expResult.MaxListSizeRead, len(entries))

		// Merge this list and compute counter entries
		// Skip sets that has been computed for exact overlap previously
		for _, entry := range entries {
			if _, skip := ignores[entry.ID]; skip {
				continue
			}
			// Process seen candidates
			if ce, seen := counter[entry.ID]; seen {
				ce.update(entry.MatchPosition, skippedOverlap)
				continue
			}
			// No need to process unseen candidate if we have reached this point
			if kthOverlap(h, k) >= maxOverlapUnseenCandidate {
				continue
			}
			// Process new candidate
			counter[entry.ID] = newCandidateEntry(entry.ID, entry.Size,
				entry.MatchPosition, i, skippedOverlap)
		}

		// Terminates as we are at the last list, no need to read set
		if i == querySize-1 {
			break
		}

		// Continue reading the next list when there is no candidates
		if len(counter) == 0 ||
			// Do not start reading sets until we have seen at least k
			// candidates
			(len(counter) < k && h.Len() < k) ||
			// Continue reading the next list when we are still in the
			// current batch
			currBatchLists > 0 {
			currBatchLists--
			continue
		}
		// Reset counter
		currBatchLists = batchSize

		// Find the end index of the next batch of posting lists
		nextBatchEndIndex := nextBatchDistinctLists(tokens, gids, i, batchSize)
		// Compute the cost of reading the next batch of posting lists
		mergeListsCost := readListCosts[nextBatchEndIndex] - readListCosts[i]
		// Process candidates to estimate benefit of reading the next batch of
		// posting lists and obtain qualified candidates
		mergeListsBenefit, numWithBenefit, candidates := processCandidatesInit(
			querySize, i, nextBatchEndIndex, kthOverlap(h, k), batchSize,
			counter, ignores)
		// Record the counter size
		expResult.MaxCounterSize = max(expResult.MaxCounterSize, len(counter))
		// Continue reading posting lists if no qualified candidate found
		// or no candidates can bring positive benefit.
		if numWithBenefit == 0 || len(candidates) == 0 {
			continue
		}
		// Sort the candidates by estimated overlaps
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].estimatedOverlap >
				candidates[j].estimatedOverlap
		})
		// Keep track of the estimation budget
		prevKthOverlap := kthOverlap(h, k)
		var numCandidateExpensive int
		var fastEstimate bool
		var fastEstimateKthOverlap int // the kth overlap used for fast est.
		// Greedily determine the next best candidate until the qualified
		// candidates exhausted or when reading the next batch of lists yield
		// better net benefit
		for _, candidate := range candidates {
			// Skip ones that has already been eliminated.
			if candidate == nil {
				continue
			}
			// The current kth overlap before reading the current candidate
			kth := kthOverlap(h, k)
			// Stop when the current candidate is no longer expected
			// to bring positive benefit.
			if candidate.estimatedOverlap <= kth {
				break
			}
			// Always read candidate when we have not had running top-k yet
			if h.Len() >= k {
				// Increase number of candidate that has used expensive benefit
				// estimation
				numCandidateExpensive++
				// Switch to fast estimate if estimation budget has reached
				if !fastEstimate &&
					numCandidateExpensive*len(candidates) >
						expensiveEstimationBudget {
					fastEstimate = true
					fastEstimateKthOverlap = prevKthOverlap
				}
				if !fastEstimate {
					// Estimate the benefit of reading the next batch of lists
					// (expensive)
					mergeListsBenefit = processCandidatesUpdate(kth, candidates,
						counter, ignores)
				}
				// Estimate the benefit of reading this set
				// (expensive if fastEstimate is false)
				probeSetBenefit := readSetBenefit(querySize,
					kth, kthOverlapAfterPush(h, k, candidate.estimatedOverlap),
					candidates, readListCosts, fastEstimate)
				probeSetCost := candidate.estimatedCost
				// Stop looking at candidates if the current best one is no
				// better than reading the next batch of posting lists
				// The next best one either has lower benefit, which is
				// monotonic w.r.t. the overlap, or higher cost.
				// So if the current best one is not
				// better the next best one will be even worse.
				if probeSetBenefit-probeSetCost <
					mergeListsBenefit-mergeListsCost {
					break
				}
			}
			// Now read this candidate
			// Decrease merge list benefit if we are using fast estimate
			if fastEstimate ||
				(numCandidateExpensive+1)*len(candidates) >
					expensiveEstimationBudget {
				mergeListsBenefit -= readListsBenenfitForCandidate(candidate,
					fastEstimateKthOverlap)
			}
			// Mark this candidate as read.
			candidate.read = true
			// Ingore this candidate in future encounters
			ignores[candidate.id] = true
			// Remove this candidate from counter
			delete(counter, candidate.id)
			// We are done if this candidate can be pruned, this can happen
			// sometimes when using fast estimate.
			if candidate.maximumOverlap <= kth {
				continue
			}
			// Compute the total overlap
			var totalOverlap int
			if candidate.suffixLength() > 0 {
				s := setTokensSuffix(db, setTable, candidate.id,
					candidate.latestMatchPosition+1)
				expResult.NumSetRead++
				expResult.MaxSetSizeRead = max(expResult.MaxSetSizeRead, len(s))
				suffixOverlap := overlap(s, tokens[i+1:])
				totalOverlap = suffixOverlap + candidate.partialOverlap
			} else {
				totalOverlap = candidate.partialOverlap
			}
			// Save the current kth overlap as the previous kth overlap
			prevKthOverlap = kth
			// Push the candidate to the heap
			pushCandidate(h, k, candidate.id, totalOverlap)
		}
	}

	// Handle the remaining sets in the counter that has the full overlaps
	// computed through merging all lists
	for _, ce := range counter {
		pushCandidate(h, k, ce.id, ce.partialOverlap)
	}
	results := orderedResults(h)

	expResult.Duration = int(time.Now().Sub(start) / time.Millisecond)
	expResult.Results = writeResultString(results)
	expResult.QueryID = query.ID
	expResult.QuerySize = len(query.RawTokens)
	expResult.NumResult = len(results)
	expResult.IgnoreSize = len(ignores)
	expResult.QueryNumToken = len(tokens)
	return results, expResult
}
