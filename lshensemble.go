package topk

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ekzhu/lshensemble"
	"github.com/lib/pq"
)

var (
	// MinhashSeed is the random seed used for creating minhash
	MinhashSeed int64 = 41
	// MinhashSize is the number of hash functions used in minhash
	MinhashSize        = 128
	lshMaxK            = 8
	lshNumPartition    = 32
	lshRetrievalFactor = 2
	thresholds         []float64
)

func init() {
	thresholds = make([]float64, 0)
	for i := 1.0; i > 0.1; i -= 0.05 {
		thresholds = append(thresholds, i)
	}
	for i := 0.1; i > 0.01; i -= 0.005 {
		thresholds = append(thresholds, i)
	}
}

func createLSHEnsemble(db *sql.DB, minhashTable string) *lshensemble.LshEnsemble {
	log.Println("Creating LSH Ensemble index...")
	var totalNumDomains int
	if err := db.QueryRow(fmt.Sprintf(`
		SELECT count(id) FROM %s;`, pq.QuoteIdentifier(minhashTable))).Scan(&totalNumDomains); err != nil {
		panic(err)
	}
	records := make(chan *lshensemble.DomainRecord)
	go func() {
		defer close(records)
		rows, err := db.Query(fmt.Sprintf(`
		SELECT id, size, signature FROM %s ORDER BY size;`, pq.QuoteIdentifier(minhashTable)))
		if err != nil {
			panic(err)
		}
		var count int
		for rows.Next() {
			var id int64
			var size int
			var bin []byte
			if err := rows.Scan(&id, &size, &bin); err != nil {
				panic(err)
			}
			signature, err := lshensemble.BytesToSig(bin)
			if err != nil {
				panic(err)
			}
			records <- &lshensemble.DomainRecord{id, size, signature}
			count++
			if count%1000 == 0 {
				fmt.Printf("\r%d sets inserted", count)
			}
		}
		fmt.Println()
		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()
	lsh, err := lshensemble.BootstrapLshEnsemblePlus(lshNumPartition, MinhashSize, lshMaxK, totalNumDomains, records)
	if err != nil {
		panic(err)
	}
	log.Println("Finished creating LSH Ensemble index.")
	return lsh
}

func searchLSHEnsemble(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool, groundTruth []searchResult) ([]searchResult, experimentResult) {
	var expResult experimentResult
	ac := newActionCollecter(len(query.RawTokens))

	start := time.Now()
	tokens, querySig := tb.processAndMinhashSignature(query)
	expResult.PreprocDuration = int(time.Now().Sub(start) / time.Millisecond)
	start = time.Now()

	// First find candidates using LSH Ensemble by decreasing thresholds
	candidates := make(map[int64]bool)
	for _, threshold := range thresholds {
		IDs, _ := lsh.QueryTimed(querySig, len(tokens), threshold)
		for _, ID := range IDs {
			if ignoreSelf && ID == query.ID {
				continue
			}
			candidates[ID.(int64)] = true
		}
		if len(candidates) >= lshRetrievalFactor*k {
			break
		}
	}
	expResult.LSHDuration = int(time.Now().Sub(start) / time.Millisecond)

	// Compute the exact set overlaps of all candidates and find out the top-k
	ac.start()
	h := &searchResultHeap{}
	for ID := range candidates {
		s := SetTokens(db, setTable, ID)
		expResult.NumSetRead++
		o := overlap(s, tokens)
		pushCandidate(h, k, ID, o)
		ac.addReadSet(len(s), o)
	}
	results := orderedResults(h)
	ac.done()

	expResult.Duration = int(time.Now().Sub(start) / time.Millisecond)
	expResult.LSHPrecision = precision(results, groundTruth)
	expResult.Results = writeResultString(results)
	expResult.QueryID = query.ID
	expResult.QuerySize = len(query.RawTokens)
	expResult.NumResult = len(results)
	expResult.QueryNumToken = len(tokens)
	expResult.Actions = ac.collect()
	return results, expResult
}

func searchLSHEnsemblePrecision90(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool, groundTruth []searchResult) ([]searchResult, experimentResult) {
	return searchLSHEnsemblePrecision(db, setTable, lsh, tb, query, k, ignoreSelf, groundTruth, 0.9)
}

func searchLSHEnsemblePrecision80(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool, groundTruth []searchResult) ([]searchResult, experimentResult) {
	return searchLSHEnsemblePrecision(db, setTable, lsh, tb, query, k, ignoreSelf, groundTruth, 0.8)
}

func searchLSHEnsemblePrecision70(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool, groundTruth []searchResult) ([]searchResult, experimentResult) {
	return searchLSHEnsemblePrecision(db, setTable, lsh, tb, query, k, ignoreSelf, groundTruth, 0.7)
}

func searchLSHEnsemblePrecision60(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool, groundTruth []searchResult) ([]searchResult, experimentResult) {
	return searchLSHEnsemblePrecision(db, setTable, lsh, tb, query, k, ignoreSelf, groundTruth, 0.6)
}

func searchLSHEnsemblePrecision(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, query rawTokenSet, k int, ignoreSelf bool, groundTruth []searchResult, minPrecision float64) ([]searchResult, experimentResult) {
	var expResult experimentResult
	ac := newActionCollecter(len(query.RawTokens))

	start := time.Now()
	tokens, querySig := tb.processAndMinhashSignature(query)
	expResult.PreprocDuration = int(time.Now().Sub(start) / time.Millisecond)
	start = time.Now()

	ac.start()
	var ignores map[int64]bool
	if ignoreSelf {
		ignores = map[int64]bool{query.ID: true}
	} else {
		ignores = make(map[int64]bool)
	}
	h := &searchResultHeap{}
	for _, threshold := range thresholds {
		// Get candidate sets from LSH
		IDs, t := lsh.QueryTimed(querySig, len(tokens), threshold)
		expResult.LSHDuration += int(t / time.Millisecond)
		for _, id := range IDs {
			ID := id.(int64)
			if _, yes := ignores[ID]; yes {
				continue
			}
			ignores[ID] = true
			// Compute the exact overlap
			s := SetTokens(db, setTable, ID)
			expResult.NumSetRead++
			expResult.MaxSetSizeRead = max(expResult.MaxSetSizeRead, len(s))
			o := overlap(s, tokens)
			pushCandidate(h, k, ID, o)
			ac.addReadSet(len(s), o)
		}
		p := precision(orderedResults(copyHeap(h)), groundTruth)
		if p >= minPrecision {
			break
		}
	}
	results := orderedResults(h)
	ac.done()

	expResult.Duration = int(time.Now().Sub(start) / time.Millisecond)
	expResult.LSHPrecision = precision(results, groundTruth)
	expResult.Results = writeResultString(results)
	expResult.QueryID = query.ID
	expResult.QuerySize = len(query.RawTokens)
	expResult.NumResult = len(results)
	expResult.IgnoreSize = len(ignores)
	expResult.QueryNumToken = len(tokens)
	expResult.Actions = ac.collect()
	return results, expResult
}

func precision(results, groundTruth []searchResult) float64 {
	if len(results) == 0 {
		return 0.0
	}
	var correct int
	var i, j int
	for i < len(results) && j < len(groundTruth) {
		o1 := results[i].Overlap
		o2 := groundTruth[j].Overlap
		if o1 == o2 {
			correct++
			i++
			j++
			continue
		}
		if o1 < o2 {
			j++
		} else {
			i++
		}
	}
	return float64(correct) / float64(len(results))
}
