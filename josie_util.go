package joise

// CandidateEntry keeps track of unread candidate set
// All positions are 0-starting indexes
type candidateEntry struct {
	id                      int64
	size                    int
	firstMatchPosition      int     // the first match position of the candidate set
	latestMatchPosition     int     // the last match position of the candidate set
	queryFirstMatchPosition int     // the first match position of the query set with the candidate
	partialOverlap          int     // the number of overlapping token seen so far
	maximumOverlap          int     // The upperbound overlap
	estimatedOverlap        int     // the estimated overlap
	estimatedCost           float64 // the I/O time cost of reading this set
	estimatedNextUpperbound int     // the estimated next upperbound
	estimatedNextTruncation int     // the estimated next truncation
	read                    bool    // flag to indicate this candidate has already been read.
}

// Create a new entry when first see it
// This means the queryCurrentPosition and candidateCurrentPosition have a matching token
func newCandidateEntry(id int64, size, candidateCurrentPosition, queryCurrentPosition, skippedOverlap int) *candidateEntry {
	ce := &candidateEntry{
		id:                      id,
		size:                    size,
		firstMatchPosition:      candidateCurrentPosition,
		latestMatchPosition:     candidateCurrentPosition,
		queryFirstMatchPosition: queryCurrentPosition,
		partialOverlap:          skippedOverlap + 1, // overlapping token skipped + the token at the current position

	}
	return ce
}

// Update when a new overlapping token is found between
// queryCurrentPosition and candidateCurrentPosition
func (ce *candidateEntry) update(candidateCurrentPosition, skippedOverlap int) {
	ce.latestMatchPosition = candidateCurrentPosition
	ce.partialOverlap = ce.partialOverlap + skippedOverlap + 1 // skipped + this position
}

// Calculate the upperbound overlap, this assumes update has been called if
// the queryCurrentPosition has a matching token
func (ce *candidateEntry) upperboundOverlap(querySize, queryCurrentPosition int) int {
	ce.maximumOverlap = ce.partialOverlap + min(querySize-queryCurrentPosition-1, ce.size-ce.latestMatchPosition-1)
	return ce.maximumOverlap
}

// Estimate the total overlap, this assumes update has been called if
// the queryCurrentPosition has a matching token
func (ce *candidateEntry) estOverlap(querySize, queryCurrentPosition int) int {
	ce.estimatedOverlap = int(float64(ce.partialOverlap) / float64(queryCurrentPosition+1-ce.queryFirstMatchPosition) * float64(querySize-ce.queryFirstMatchPosition))
	ce.estimatedOverlap = min(ce.estimatedOverlap, ce.upperboundOverlap(querySize, queryCurrentPosition))
	return ce.estimatedOverlap
}

// Estimate the I/O time cost of reading this candidate set
func (ce *candidateEntry) estCost() float64 {
	ce.estimatedCost = readSetCost(ce.suffixLength())
	return ce.estimatedCost
}

// Estimate the number tokens truncated from the suffix after reading the posting lists
// from queryCurrentPosition+1 to queryNextPosition
func (ce *candidateEntry) estTruncation(querySize, queryCurrentPosition, queryNextPosition int) int {
	ce.estimatedNextTruncation = int(float64(queryNextPosition-queryCurrentPosition) / float64(querySize-ce.queryFirstMatchPosition) * float64(ce.size-ce.firstMatchPosition))
	return ce.estimatedNextTruncation
}

// Estimate the next overlap upper bound after reading the posting lists
// from queryCurrentPosition+1 to queryNextPosition
func (ce *candidateEntry) estNextOverlapUpperbound(querySize, queryCurrentPosition,
	queryNextPosition int) int {
	queryJumpLength := queryNextPosition - queryCurrentPosition
	queryPrefixLength := queryCurrentPosition + 1 - ce.queryFirstMatchPosition
	additionalOverlap := int(float64(ce.partialOverlap) / float64(queryPrefixLength) * float64(queryJumpLength))
	// Estimate the next latest matching position for candidate
	nextLatestMatchingPosition := int(float64(queryJumpLength)/float64(querySize-ce.queryFirstMatchPosition)*float64(ce.size-ce.firstMatchPosition)) + ce.latestMatchPosition
	// Compute the upper bound of overlap for this candidate
	ce.estimatedNextUpperbound = ce.partialOverlap + additionalOverlap + min(querySize-queryNextPosition-1, ce.size-nextLatestMatchingPosition-1)
	return ce.estimatedNextUpperbound
}

func (ce *candidateEntry) suffixLength() int {
	return ce.size - ce.latestMatchPosition - 1
}

func (ce *candidateEntry) checkMinSampleSize(queryCurrentPosition, batchSize int) bool {
	return (queryCurrentPosition - ce.queryFirstMatchPosition + 1) > batchSize
}

// Sorting wrappber for counter entry
type byEstimatedOverlap []*candidateEntry

func (l byEstimatedOverlap) Less(i, j int) bool {
	if l[i].estimatedOverlap == l[j].estimatedOverlap {
		return l[i].estimatedCost < l[j].estimatedCost
	}
	return l[i].estimatedOverlap > l[j].estimatedOverlap
}
func (l byEstimatedOverlap) Len() int      { return len(l) }
func (l byEstimatedOverlap) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

// Sort by maximum overlap in increasing order
type byMaximumOverlap []*candidateEntry

func (l byMaximumOverlap) Less(i, j int) bool {
	return l[i].maximumOverlap < l[j].maximumOverlap
}
func (l byMaximumOverlap) Len() int      { return len(l) }
func (l byMaximumOverlap) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

// Sort by future maximum overlap in increasing order
type byFutureMaxOverlap []*candidateEntry

func (l byFutureMaxOverlap) Less(i, j int) bool {
	return l[i].estimatedNextUpperbound < l[j].estimatedNextUpperbound
}
func (l byFutureMaxOverlap) Len() int      { return len(l) }
func (l byFutureMaxOverlap) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

// Compute the upperbound overlap of an unseen candidate at the current posting list position
func upperboundOverlapUknownCandidate(querySize, queryCurrentPosition, prefixOverlap int) int {
	return querySize - queryCurrentPosition + prefixOverlap
}

// Find the end index of the next batch of distinct lists
func nextBatchDistinctLists(tokens, gids []int64, currIndex, batchSize int) (endIndex int) {
	n := 0
	next, _ := nextDistinctList(tokens, gids, currIndex)
	for next < len(tokens) {
		currIndex = next
		n++
		if n == batchSize {
			break
		}
		next, _ = nextDistinctList(tokens, gids, currIndex)
	}
	return currIndex
}

// Prefix length is the number of posting lists we have to read
func prefixLength(querySize, kthOverlap int) int {
	if kthOverlap == 0 {
		return querySize
	}
	return querySize - kthOverlap + 1
}

func readListsBenenfitForCandidate(ce *candidateEntry, kthOverlap int) float64 {
	if kthOverlap >= ce.estimatedNextUpperbound {
		return ce.estimatedCost
	}
	return ce.estimatedCost -
		readSetCost(ce.suffixLength()-ce.estimatedNextTruncation)
}

// Process unread candidates from the counter to obtain the sorted list of
// qualified candidates, and compute the benefit of reading the next batch
// of lists.
func processCandidatesInit(querySize, queryCurrentPosition, nextBatchEndIndex,
	kthOverlap, minSampleSize int, candidates map[int64]*candidateEntry,
	ignores map[int64]bool,
) (readListsBenefit float64,
	numWithBenefit int,
	qualified []*candidateEntry) {
	qualified = make([]*candidateEntry, 0, len(candidates))
	for _, ce := range candidates {
		// Compute upper bound overlap
		ce.upperboundOverlap(querySize, queryCurrentPosition)
		// Disqualify candidates and remove it for future reads
		if kthOverlap >= ce.maximumOverlap {
			delete(candidates, ce.id)
			ignores[ce.id] = true
			continue
		}
		// Candidate does not qualify if the estimation std err is too high
		if !ce.checkMinSampleSize(queryCurrentPosition, minSampleSize) {
			continue
		}
		// Compute estimation
		ce.estCost()
		ce.estOverlap(querySize, queryCurrentPosition)
		ce.estTruncation(querySize, queryCurrentPosition, nextBatchEndIndex)
		ce.estNextOverlapUpperbound(querySize, queryCurrentPosition,
			nextBatchEndIndex)
		// Compute read list benefit
		readListsBenefit += readListsBenenfitForCandidate(ce, kthOverlap)
		// Add qualified candidate good for reading
		qualified = append(qualified, ce)
		if ce.estimatedOverlap > kthOverlap {
			numWithBenefit++
		}
	}
	return
}

// Process the unread candidates and calculate the benefit of reading the next
// batch of posting lists.
func processCandidatesUpdate(kthOverlap int, candidates []*candidateEntry,
	counter map[int64]*candidateEntry,
	ignores map[int64]bool) (readListsBenefit float64) {
	for j, ce := range candidates {
		if ce == nil || ce.read {
			continue
		}
		if ce.maximumOverlap <= kthOverlap {
			// Setting the entry to nil marking it eliminated to the caller.
			candidates[j] = nil
			delete(counter, ce.id)
			ignores[ce.id] = true
		}
		// Compute read list benefit for qualified candidate.
		readListsBenefit += readListsBenenfitForCandidate(ce, kthOverlap)
	}
	return
}

// Compute the benefit of reading a candidate set that produces a new
// kth overlap.
func readSetBenefit(querySize, kthOverlap, kthOverlapAfterPush int,
	candidates []*candidateEntry,
	readListCosts []float64,
	fast bool,
) float64 {
	var b float64
	if kthOverlapAfterPush <= kthOverlap {
		return b
	}
	p0 := prefixLength(querySize, kthOverlap)
	p1 := prefixLength(querySize, kthOverlapAfterPush)
	b += readListCosts[p0-1] - readListCosts[p1-1]
	if fast {
		return b
	}
	for _, ce := range candidates {
		if ce == nil || ce.read {
			continue
		}
		if ce.maximumOverlap <= kthOverlapAfterPush {
			// Add benefit from eliminating the candidate.
			b += ce.estimatedCost
		}
	}
	return b
}
