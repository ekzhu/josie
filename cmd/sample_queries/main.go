package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/lib/pq"
)

var (
	pgServer, pgPort                                     string
	pgTableSets                                          string
	pgTableQueries                                       string
	numQueries, numIntervals, minQuerySize, maxQuerySize int
	samplePerStep                                        int
)

func main() {
	flag.StringVar(&pgServer, "pg-server", "localhost", "Postgres server addresss")
	flag.StringVar(&pgPort, "pg-port", "5442", "Postgres server port")
	flag.StringVar(&pgTableSets, "pg-table-sets", "canada_us_uk_sets", "Postgres table for sets")
	flag.StringVar(&pgTableQueries, "pg-table-queries", "", "Postgres table for the query sets")
	flag.IntVar(&numQueries, "sampling-num-query", 1000, "Number of sets to sample as queries")
	flag.IntVar(&numIntervals, "sampling-num-interval", 10, "Number of stratified sampling intervals for query sizes")
	flag.IntVar(&minQuerySize, "sampling-min-query-size", 10, "Minimum query set size to sample")
	flag.IntVar(&maxQuerySize, "sampling-max-query-size", 100000, "Maximum query set size to sample")
	flag.Parse()
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s sslmode=disable", pgServer, pgPort))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	sampleSetsStratified(db, minQuerySize, maxQuerySize, numIntervals, numQueries)
}

func sampleSetsStratified(db *sql.DB, minQuerySize, maxQuerySize, numIntervals, numQueries int) {
	if numIntervals < 2 {
		panic("at least 2 intervals is required")
	}
	intervalRangeSize := maxQuerySize / numIntervals
	if intervalRangeSize == 0 {
		panic("interval range size becomes 0")
	}
	intervalSampleSize := numQueries / numIntervals
	if intervalSampleSize == 0 {
		panic("interval sample size becomes 0")
	}

	// Create query table
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, pq.QuoteIdentifier(pgTableQueries)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (id integer, tokens integer[]);`,
		pq.QuoteIdentifier(pgTableQueries)))
	if err != nil {
		panic(err)
	}

	// Set random seed
	_, err = db.Exec(`SELECT setseed(0.618);`)
	if err != nil {
		panic(err)
	}

	s := fmt.Sprintf(`
	INSERT INTO %s (id, tokens) 
	SELECT id, tokens FROM %s
	WHERE num_non_singular_token >= $1 AND num_non_singular_token < $2 
	ORDER BY random()
	LIMIT $3;`, pq.QuoteIdentifier(pgTableQueries), pq.QuoteIdentifier(pgTableSets))
	for i := 0; i < numIntervals; i++ {
		var start, end int
		if i == 0 {
			start = minQuerySize
		} else {
			start = i * intervalRangeSize
		}
		end = (i + 1) * intervalRangeSize
		// Check if requirement can be statisfied
		var count int
		if err := db.QueryRow(fmt.Sprintf(
			`SELECT count(id) FROM %s WHERE num_non_singular_token >= $1 AND num_non_singular_token < $2;`,
			pq.QuoteIdentifier(pgTableSets)), start, end).Scan(&count); err != nil {
			panic(err)
		}
		if count < intervalSampleSize {
			log.Fatalf("Cannot sample %d sets from interval [%d, %d), which has %d sets",
				intervalSampleSize, start, end, count)
		}
		log.Printf("Sample %d sets in size interval [%d, %d)", intervalSampleSize, start, end)
		_, err := db.Exec(s, start, end, intervalSampleSize)
		if err != nil {
			panic(err)
		}
	}
}
