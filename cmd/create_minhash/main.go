package main

import (
	"database/sql"
	"flag"
	"fmt"

	"github.com/ekzhu/josie"
	"github.com/ekzhu/lshensemble"
	"github.com/lib/pq"
)

var (
	pgServer, pgPort string
	pgTableSets      string
	pgTableLists     string
	pgTableMinhash   string
	recreate         bool
)

type rawTokenSet struct {
	id        int64
	size      int
	rawTokens pq.ByteaArray
}

func main() {
	flag.StringVar(&pgServer, "pg-server", "localhost", "Postgres server addresss")
	flag.StringVar(&pgPort, "pg-port", "5442", "Postgres server port")
	flag.StringVar(&pgTableSets, "pg-table-sets", "canada_us_uk_sets", "Postgres table for sets")
	flag.StringVar(&pgTableLists, "pg-table-lists", "canada_us_uk_inverted_lists", "Postgres table for inverted lists")
	flag.StringVar(&pgTableMinhash, "pg-table-minhash", "canada_us_uk_minhash", "Postgres table for Minhash signatures")
	flag.BoolVar(&recreate, "recreate", false, "Whether to recreate the minhash table from scratch")
	flag.Parse()
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s sslmode=disable", pgServer, pgPort))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if recreate {
		_, err = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s;`,
			pq.QuoteIdentifier(pgTableMinhash)))
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (id integer, size integer, signature bytea);`,
			pq.QuoteIdentifier(pgTableMinhash)))
		if err != nil {
			panic(err)
		}
	}

	rows, err := db.Query(fmt.Sprintf(`
			WITH set2token AS (
				SELECT s.id, s.size, unnest(s.tokens) AS token
				FROM %s AS s
				LEFT JOIN %s AS m
				ON s.id = m.id
				WHERE m.id IS NULL
			)
			SELECT set2token.id, set2token.size, b.raw_token
			FROM set2token, %s AS b
			WHERE set2token.token = b.token;
			`,
		pq.QuoteIdentifier(pgTableSets),
		pq.QuoteIdentifier(pgTableMinhash),
		pq.QuoteIdentifier(pgTableLists)))
	if err != nil {
		panic(err)
	}
	var lastID int64
	var lastSize int
	var mh *lshensemble.Minhash
	insertedIDs := make(map[int64]bool)
	for rows.Next() {
		var id int64
		var size int
		var rawToken []byte
		if err := rows.Scan(&id, &size, &rawToken); err != nil {
			panic(err)
		}
		// Sanity check: inserted id should not appear again
		if _, again := insertedIDs[id]; again {
			panic("Inserted id appeared again!")
		}
		if id != lastID && mh != nil {
			// Insert
			sig := lshensemble.SigToBytes(mh.Signature())
			_, err := db.Exec(fmt.Sprintf(`
					INSERT INTO %s (id, size, signature) VALUES ($1, $2, $3);`,
				pq.QuoteIdentifier(pgTableMinhash)), lastID, lastSize, sig)
			if err != nil {
				panic(err)
			}
			mh = nil
			insertedIDs[lastID] = true
		}
		if mh == nil {
			mh = lshensemble.NewMinhash(topk.MinhashSeed, topk.MinhashSize)
		}
		mh.Push(rawToken)
		lastID = id
		lastSize = size
	}
	if mh != nil {
		// Insert the last one
		sig := lshensemble.SigToBytes(mh.Signature())
		_, err = db.Exec(fmt.Sprintf(`
					INSERT INTO %s (id, size, signature) VALUES ($1, $2, $3);`,
			pq.QuoteIdentifier(pgTableMinhash)), lastID, lastSize, sig)
		if err != nil {
			panic(err)
		}
		insertedIDs[lastID] = true
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}
