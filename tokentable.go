package joise

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"sort"

	"github.com/ekzhu/lshensemble"
	"github.com/lib/pq"
)

// tokenMapEntry is used to map hash value to token
type tokenMapEntry struct {
	Token   int32
	GroupID int32
}

// tokenTable maps a hash value of a raw token into token, frequencies, and group id
type tokenTable interface {
	process(set rawTokenSet) (tokens []int64, counts []int, gids []int64)
	processAndMinhashSignature(set rawTokenSet) (tokens []int64, sig []uint64)
}

type tokenTableMem struct {
	tokenMap    map[uint64]tokenMapEntry
	frequencies []int32 // maps duplicate group ID which is the index to the frequency
	ignoreSelf  bool    // whether to ignore potential matching of query set to itself in the index
	// this is only to be true when running experiment using 100% of sets and you know the
	// query sets must be in the index
}

type tokenTableDisk struct {
	listTable  string
	db         *sql.DB
	ignoreSelf bool // whether to ignore potential matching of query set to itself in the index
	// this is only to be true when running experiment using 100% of sets and you know the
	// query sets must be in the index
}

func createTokenTableMem(db *sql.DB, pgTableLists string, ignoreSelf bool) tokenTable {
	var table tokenTableMem
	table.ignoreSelf = ignoreSelf
	// First find out how many entries do we have, and initialize the map with capacity
	log.Println("Initializing token map...")
	var count int
	err := db.QueryRow(fmt.Sprintf(`
		SELECT count(*) FROM %s;`, pq.QuoteIdentifier(pgTableLists))).Scan(&count)
	if err != nil {
		panic(err)
	}
	table.tokenMap = make(map[uint64]tokenMapEntry, count)
	log.Printf("Initalized token map, %d entries", count)
	// Then find out what is the maximum duplicate group id, so we can initialize the
	// frequencies array
	log.Println("Initializing frequency table...")
	var maxGid int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT max(duplicate_group_id) FROM %s;`, pq.QuoteIdentifier(pgTableLists))).Scan(&maxGid)
	if err != nil {
		panic(err)
	}
	table.frequencies = make([]int32, maxGid+1)
	log.Printf("Initialized frequency table, %d entries", len(table.frequencies))

	// Load all tokens and duplicate group ids
	log.Println("Filling token table entries...")
	rows, err := db.Query(fmt.Sprintf(`
		SELECT raw_token, token, frequency, duplicate_group_id FROM %s;`,
		pq.QuoteIdentifier(pgTableLists)))
	if err != nil {
		panic(err)
	}
	count = 0
	for rows.Next() {
		var entry tokenMapEntry
		var rawToken []byte
		var frequency int32
		if err := rows.Scan(&rawToken, &entry.Token, &frequency, &entry.GroupID); err != nil {
			panic(err)
		}
		// Hash
		h := fnv.New64a()
		h.Write(rawToken)
		hashValue := h.Sum64()
		// Assign the frequency to frequencies table
		table.frequencies[entry.GroupID] = frequency
		// Assign the token entry to map
		// NOTE: no collision has been observed for open data and webtable datasets
		table.tokenMap[hashValue] = entry
		count++
		if count%1000 == 0 {
			fmt.Printf("\r%d read", count)
		}
	}
	fmt.Println()
	if err := rows.Err(); err != nil {
		panic(err)
	}
	log.Printf("Finished creating token map and frequency table")
	return table
}

type byTokenOrder struct {
	tokens []int64
	counts []int
	gids   []int64
}

func (b byTokenOrder) Swap(i, j int) {
	b.tokens[i], b.tokens[j] = b.tokens[j], b.tokens[i]
	b.counts[i], b.counts[j] = b.counts[j], b.counts[i]
	b.gids[i], b.gids[j] = b.gids[j], b.gids[i]
}
func (b byTokenOrder) Less(i, j int) bool { return b.tokens[i] < b.tokens[j] }
func (b byTokenOrder) Len() int           { return len(b.tokens) }

type byTokenOrderSingular []int64

func (b byTokenOrderSingular) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byTokenOrderSingular) Less(i, j int) bool { return b[i] < b[j] }
func (b byTokenOrderSingular) Len() int           { return len(b) }

// Takes the raw tokens and returns the matching tokens in the database
func (tb tokenTableMem) process(set rawTokenSet) (tokens []int64, counts []int, gids []int64) {
	tokens = make([]int64, 0)
	counts = make([]int, 0)
	gids = make([]int64, 0)
	h := fnv.New64a()
	for _, rawToken := range set.RawTokens {
		h.Reset()
		h.Write(rawToken)
		hashValue := h.Sum64()
		if entry, exists := tb.tokenMap[hashValue]; exists {
			frequency := tb.frequencies[entry.GroupID]
			// NOTE: since all tokens are originally from the database, if frequency is 1
			// it means the token only exists in the query set
			if tb.ignoreSelf && frequency < 2 {
				continue
			}
			tokens = append(tokens, int64(entry.Token))
			counts = append(counts, int(frequency-1))
			gids = append(gids, int64(entry.GroupID))
		}
	}
	b := byTokenOrder{tokens, counts, gids}
	sort.Sort(b)
	return
}

func (tb tokenTableMem) processAndMinhashSignature(set rawTokenSet) (tokens []int64, sig []uint64) {
	tokens = make([]int64, 0)
	mh := lshensemble.NewMinhash(MinhashSeed, MinhashSize)
	h := fnv.New64a()
	for _, rawToken := range set.RawTokens {
		h.Reset()
		h.Write(rawToken)
		hashValue := h.Sum64()
		if entry, exists := tb.tokenMap[hashValue]; exists {
			frequency := tb.frequencies[entry.GroupID]
			// NOTE: since all tokens are originally from the database, if frequency is 1
			// it means the token only exists in the query set
			if tb.ignoreSelf && frequency < 2 {
				continue
			}
			tokens = append(tokens, int64(entry.Token))
			mh.Push(rawToken)
		}
	}
	sort.Sort(byTokenOrderSingular(tokens))
	return tokens, mh.Signature()
}

func createTokenTableDisk(db *sql.DB, listTable string, ignoreSelf bool) tokenTable {
	return tokenTableDisk{
		listTable:  listTable,
		db:         db,
		ignoreSelf: ignoreSelf,
	}
}

func (tb tokenTableDisk) process(set rawTokenSet) (tokens []int64, counts []int, gids []int64) {
	var q string
	if tb.ignoreSelf {
		q = fmt.Sprintf(`
		SELECT token, frequency-1 AS count, duplicate_group_id FROM %s
		WHERE token = ANY($1) AND frequency > 1
		ORDER BY token ASC;`, pq.QuoteIdentifier(tb.listTable))
	} else {
		q = fmt.Sprintf(`
		SELECT token, frequency-1 AS count, duplicate_group_id FROM %s
		WHERE token = ANY($1)
		ORDER BY token ASC;`, pq.QuoteIdentifier(tb.listTable))
	}
	rows, err := tb.db.Query(q, pq.Array(set.Tokens))
	if err != nil {
		panic(err)
	}
	tokens = make([]int64, 0)
	counts = make([]int, 0)
	gids = make([]int64, 0)
	for rows.Next() {
		var token int64
		var count int64
		var gid int64
		if err := rows.Scan(&token, &count, &gid); err != nil {
			panic(err)
		}
		tokens = append(tokens, token)
		counts = append(counts, int(count))
		gids = append(gids, gid)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return
}

func (tb tokenTableDisk) processAndMinhashSignature(set rawTokenSet) (tokens []int64, sig []uint64) {
	tokens = make([]int64, 0)
	mh := lshensemble.NewMinhash(MinhashSeed, MinhashSize)
	var q string
	if tb.ignoreSelf {
		q = fmt.Sprintf(`
		SELECT token, raw_token FROM %s
		WHERE token = ANY($1) AND frequency > 1
		ORDER BY token ASC;`, pq.QuoteIdentifier(tb.listTable))
	} else {
		q = fmt.Sprintf(`
		SELECT token, raw_token FROM %s
		WHERE token = ANY($1)
		ORDER BY token ASC;`, pq.QuoteIdentifier(tb.listTable))
	}
	rows, err := tb.db.Query(q, pq.Array(set.Tokens))
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var rawToken []byte
		var token int64
		if err := rows.Scan(&token, &rawToken); err != nil {
			panic(err)
		}
		tokens = append(tokens, token)
		mh.Push(rawToken)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return tokens, mh.Signature()
}
