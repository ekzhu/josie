package main

import (
	"database/sql"
	"flag"
	"fmt"
	"path/filepath"

	"github.com/ekzhu/josie"
)

var (
	pgServer, pgPort string
	benchmark        string
	output           string
	cpuProfile       bool
)

func main() {
	flag.StringVar(&pgServer, "pg-server", "localhost", "Postgres server addresss")
	flag.StringVar(&pgPort, "pg-port", "5442", "Postgres server port")
	flag.StringVar(&benchmark, "benchmark", "canada_us_uk", "The name of the benchmark dataset to use")
	flag.StringVar(&output, "output", "results", "Output directory for results")
	flag.BoolVar(&cpuProfile, "cpu-profile", false, "Enable CPU profiling")
	flag.Parse()
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s sslmode=disable", pgServer, pgPort))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if benchmark == "canada_us_uk" {
		topk.RunOpenDataExperiments(db, filepath.Join(output, benchmark), cpuProfile, true)
	}
	if benchmark == "webtable" {
		topk.RunWebTableExperiments(db, filepath.Join(output, benchmark), cpuProfile, true)
	}
}
