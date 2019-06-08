package topk

import "container/heap"

type searchResult struct {
	ID      int64
	Overlap int
}

type searchResultHeap []searchResult

func (h searchResultHeap) Len() int           { return len(h) }
func (h searchResultHeap) Less(i, j int) bool { return h[i].Overlap < h[j].Overlap }
func (h searchResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *searchResultHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(searchResult))
}

func (h *searchResultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func kthOverlap(h *searchResultHeap, k int) int {
	if h.Len() < k {
		return 0
	}
	return (*h)[0].Overlap
}

func pushCandidate(h *searchResultHeap, k int, id int64, overlap int) bool {
	if h.Len() == k {
		if (*h)[0].Overlap >= overlap {
			return false
		}
		heap.Pop(h)
	}
	heap.Push(h, searchResult{id, overlap})
	return true
}

func orderedResults(h *searchResultHeap) []searchResult {
	r := make([]searchResult, h.Len())
	for i := len(r) - 1; i >= 0; i-- {
		r[i] = heap.Pop(h).(searchResult)
	}
	return r
}

// Without actually performing the push, check what is the kth overlap
// after pushing a candidate with the overlap value.
func kthOverlapAfterPush(h *searchResultHeap, k int, overlap int) int {
	if h.Len() < k-1 {
		return 0
	}
	// First check if the overlap can make into top-k
	kth := (*h)[0].Overlap
	if overlap <= kth {
		return kth
	}
	// Now the overlap is better than kth
	// If our k is 1, then the new overlap is decided
	if k == 1 {
		return overlap
	}
	var jth int
	// Otherwise look at (k-1)-th
	if k == 2 {
		jth = (*h)[1].Overlap
	} else {
		jth = min((*h)[1].Overlap, (*h)[2].Overlap)
	}
	return min(jth, overlap)
}

func copyHeap(h *searchResultHeap) *searchResultHeap {
	h2 := searchResultHeap(make([]searchResult, len(*h)))
	copy(h2, *h)
	return &h2
}
