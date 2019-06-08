package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ekzhu/josie"
	"github.com/lib/pq"
)

var (
	pgServer, pgPort                             string
	pgTableSets                                  string
	pgTableLists                                 string
	pgTableQueries                               string
	pgTableReadSetCostSamples                    string
	pgTableReadListCostSamples                   string
	computeCostOnly                              bool
	minListLength, maxListLength, listLengthStep int
	samplePerStep                                int
)

func main() {
	flag.StringVar(&pgServer, "pg-server", "localhost", "Postgres server addresss")
	flag.StringVar(&pgPort, "pg-port", "5442", "Postgres server port")
	flag.StringVar(&pgTableSets, "pg-table-sets", "canada_us_uk_sets", "Postgres table for sets")
	flag.StringVar(&pgTableLists, "pg-table-lists", "canada_us_uk_inverted_lists", "Postgres table for inverted lists")
	flag.StringVar(&pgTableQueries, "pg-table-queries", "", "Postgres table for the query sets")
	flag.StringVar(&pgTableReadSetCostSamples, "pg-table-read-set-cost-samples", "canada_us_uk_read_set_cost_samples", "Postgres table for samples for read set cost estimation")
	flag.StringVar(&pgTableReadListCostSamples, "pg-table-read-list-cost-samples", "canada_us_uk_read_list_cost_samples", "Postgres table for samples for read list cost estimation")
	flag.IntVar(&minListLength, "cost-min-list-size", 0, "Minimum list length for cost estimation")
	flag.IntVar(&maxListLength, "cost-max-list-size", 4000, "Minimum list length for cost estimation")
	flag.IntVar(&listLengthStep, "cost-list-size-step", 100, "Step size for cost estimation")
	flag.IntVar(&samplePerStep, "cost-sample-per-size", 10, "Number of samples per each step")
	flag.Parse()
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s sslmode=disable", pgServer, pgPort))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	sampleReadSetCost(db, pgTableQueries, pgTableReadSetCostSamples)
	sampleReadListCost(db, pgTableReadListCostSamples, minListLength, maxListLength, listLengthStep, samplePerStep)
}

func sampleReadSetCost(db *sql.DB, pgTableQueries, pgTableReadSetCostSamples string) {
	// Create sample table
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s;`,
		pq.QuoteIdentifier(pgTableReadSetCostSamples)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (id integer, size integer, cost integer);`,
		pq.QuoteIdentifier(pgTableReadSetCostSamples)))
	if err != nil {
		panic(err)
	}

	// Obtain costs
	var sampleSetIDs []int64
	err = db.QueryRow(fmt.Sprintf(`SELECT array_agg(id) FROM %s;`,
		pq.QuoteIdentifier(pgTableQueries))).Scan(pq.Array(&sampleSetIDs))
	if err != nil {
		panic(err)
	}
	for i, id := range sampleSetIDs {
		log.Printf("Read set id = %d, #%d/%d", id, i+1, len(sampleSetIDs))
		start := time.Now()
		s := topk.SetTokens(db, pgTableSets, id)
		dur := time.Now().Sub(start)
		// Cost is the duration in ns
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (id, size, cost) VALUES ($1, $2, $3);`,
			pq.QuoteIdentifier(pgTableReadSetCostSamples)), id, len(s), dur)
		if err != nil {
			panic(err)
		}
	}
}

func sampleReadListCost(db *sql.DB, pgTableReadListCostSamples string, minLength, maxLength, step, sampleSizePerStep int) {
	// Create sample table
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s;`,
		pq.QuoteIdentifier(pgTableReadListCostSamples)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (token integer, frequency integer, cost integer);`,
		pq.QuoteIdentifier(pgTableReadListCostSamples)))
	if err != nil {
		panic(err)
	}

	// Set random seed
	_, err = db.Exec(`SELECT setseed(0.618);`)
	if err != nil {
		panic(err)
	}

	// Create sample sets
	s := fmt.Sprintf(`
	INSERT INTO %s (token, frequency) 
	SELECT token, frequency FROM %s
	WHERE frequency > $1 AND frequency <= $2
	ORDER BY random()
	LIMIT $3;`, pq.QuoteIdentifier(pgTableReadListCostSamples), pq.QuoteIdentifier(pgTableLists))
	for l := minLength; l < maxLength; l += step {
		log.Printf("Sample %d lists with length = (%d, %d]", sampleSizePerStep, l, l+step)
		_, err := db.Exec(s, l, l+step, sampleSizePerStep)
		if err != nil {
			panic(err)
		}
		var count int
		err = db.QueryRow(fmt.Sprintf(`SELECT count(token) FROM %s WHERE frequency > $1 AND frequency <= $2;`,
			pq.QuoteIdentifier(pgTableReadListCostSamples)), l, l+step).Scan(&count)
		if err != nil {
			panic(err)
		}
		if count < sampleSizePerStep {
			log.Printf("Could not sample %d lists with length = (%d, %d]", sampleSizePerStep, l, l+step)
		}
	}

	// Obtain costs
	var sampleListTokens []int64
	err = db.QueryRow(fmt.Sprintf(`SELECT array_agg(token) FROM %s;`,
		pq.QuoteIdentifier(pgTableReadListCostSamples))).Scan(pq.Array(&sampleListTokens))
	if err != nil {
		panic(err)
	}
	for i, token := range sampleListTokens {
		log.Printf("Read list token = %d, #%d/%d", token, i+1, len(sampleListTokens))
		start := time.Now()
		_ = topk.InvertedList(db, pgTableLists, token)
		dur := time.Now().Sub(start)
		// Cost is the duration in ns
		_, err = db.Exec(fmt.Sprintf(`UPDATE %s SET cost = $1 WHERE token = $2;`,
			pq.QuoteIdentifier(pgTableReadListCostSamples)), dur, token)
		if err != nil {
			panic(err)
		}
	}
}
