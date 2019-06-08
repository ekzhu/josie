package topk

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

type rawTokenSet struct {
	ID        int64
	Tokens    []int64
	RawTokens [][]byte
}

// ListEntry is a set ID, size, and the matching position of the token
type ListEntry struct {
	ID            int64
	Size          int
	MatchPosition int
}

// Asc. ordering by the size of sets
type bySize []ListEntry

func (l bySize) Len() int           { return len(l) }
func (l bySize) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l bySize) Less(i, j int) bool { return l[i].Size < l[j].Size }

// Desc. ordering by the length of matching prefix
type byPrefixLength []ListEntry

func (l byPrefixLength) Len() int           { return len(l) }
func (l byPrefixLength) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l byPrefixLength) Less(i, j int) bool { return l[i].MatchPosition > l[j].MatchPosition }

// Asc. ordering by the length of matching suffix
type bySuffixLength []ListEntry

func (l bySuffixLength) Len() int      { return len(l) }
func (l bySuffixLength) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l bySuffixLength) Less(i, j int) bool {
	return (l[i].Size - l[i].MatchPosition) > (l[j].Size - l[j].MatchPosition)
}

// SetTokens read tokens from a given set.
func SetTokens(db *sql.DB, table string, setID int64) []int64 {
	s := fmt.Sprintf(`
	SELECT tokens FROM %s WHERE id = $1;`, table)
	var tokens []int64
	if err := db.QueryRow(s, setID).Scan(pq.Array(&tokens)); err != nil {
		panic(err)
	}
	return tokens
}

func setTokensPrefix(db *sql.DB, table string, setID int64, endPos int) []int64 {
	s := fmt.Sprintf(`
	SELECT tokens[1:$1] FROM %s WHERE id = $2;`, table)
	var tokens []int64
	if err := db.QueryRow(s, endPos+1, setID).Scan(pq.Array(&tokens)); err != nil {
		panic(err)
	}
	return tokens
}

func setTokensSuffix(db *sql.DB, table string, setID int64, startPos int) []int64 {
	s := fmt.Sprintf(`
	SELECT tokens[$1:size] FROM %s WHERE id = $2;`, table)
	var tokens []int64
	if err := db.QueryRow(s, startPos+1, setID).Scan(pq.Array(&tokens)); err != nil {
		panic(err)
	}
	return tokens
}

// startPos is an inclusive zero-start index
// endPos is a non-inclusive zero-start index
func setTokensSubset(db *sql.DB, table string, setID int64, startPos, endPos int) []int64 {
	s := fmt.Sprintf(`
	SELECT tokens[$1:$2] FROM %s WHERE id = $3;`, pq.QuoteIdentifier(table))
	var tokens []int64
	if err := db.QueryRow(s, startPos+1, endPos, setID).Scan(pq.Array(&tokens)); err != nil {
		panic(err)
	}
	return tokens
}

// InvertedList reads an inverted list from the database
func InvertedList(db *sql.DB, table string, token int64) (entries []ListEntry) {
	var setIDs, sizes, matchPositions []int64
	s := fmt.Sprintf(`
	SELECT set_ids, set_sizes, match_positions FROM %s WHERE token = $1`, pq.QuoteIdentifier(table))
	if err := db.QueryRow(s, token).Scan(pq.Array(&setIDs), pq.Array(&sizes), pq.Array(&matchPositions)); err != nil {
		panic(err)
	}
	entries = make([]ListEntry, len(setIDs))
	for i := range entries {
		entries[i] = ListEntry{
			ID:            setIDs[i],
			Size:          int(sizes[i]),
			MatchPosition: int(matchPositions[i]),
		}
	}
	return
}

func querySets(db *sql.DB, listTable, queryTable string) []rawTokenSet {
	rows, err := db.Query(fmt.Sprintf(`
		SELECT id, (
			SELECT array_agg(raw_token)
			FROM %s
			WHERE token = any(tokens)
		), tokens FROM %s`, pq.QuoteIdentifier(listTable), pq.QuoteIdentifier(queryTable)))
	if err != nil {
		panic(err)
	}
	queries := make([]rawTokenSet, 0)
	for rows.Next() {
		var query rawTokenSet
		var ba pq.ByteaArray
		if err := rows.Scan(&query.ID, &ba, pq.Array(&query.Tokens)); err != nil {
			panic(err)
		}
		query.RawTokens = ba
		queries = append(queries, query)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return queries
}
