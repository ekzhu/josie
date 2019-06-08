package topk

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ekzhu/lshensemble"
	"github.com/gocarina/gocsv"
	"github.com/lib/pq"
)

var (
	openDataSetTables = map[int]string{
		100: "canada_us_uk_sets",
		//75:  "canada_us_uk_sets_75pct",
		//50:  "canada_us_uk_sets_50pct",
		//25:  "canada_us_uk_sets_25pct",
	}
	openDataListTables = map[int]string{
		100: "canada_us_uk_inverted_lists",
		75:  "canada_us_uk_inverted_lists_75pct",
		50:  "canada_us_uk_inverted_lists_50pct",
		25:  "canada_us_uk_inverted_lists_25pct",
	}
	openDataReadSetCostSampleTables = map[int]string{
		100: "canada_us_uk_read_set_cost_samples",
		75:  "canada_us_uk_read_set_cost_samples_75pct",
		50:  "canada_us_uk_read_set_cost_samples_50pct",
		25:  "canada_us_uk_read_set_cost_samples_25pct",
	}
	openDataReadListCostSampleTables = map[int]string{
		100: "canada_us_uk_read_list_cost_samples",
		75:  "canada_us_uk_read_list_cost_samples_75pct",
		50:  "canada_us_uk_read_list_cost_samples_50pct",
		25:  "canada_us_uk_read_list_cost_samples_25pct",
	}
	openDataMinhashTables = map[int]string{
		100: "canada_us_uk_minhash",
		75:  "canada_us_uk_minhash_75pct",
		50:  "canada_us_uk_minhash_50pct",
		25:  "canada_us_uk_minhash_40pct",
	}
	openDataQueryTables = [][]string{
		[]string{"1k", "canada_us_uk_queries_1k"},
		[]string{"10k", "canada_us_uk_queries_10k"},
		[]string{"100k", "canada_us_uk_queries_100k"},
	}
	webtableSetTables = map[int]string{
		100: "webtable_sets",
		//75:  "webtable_sets_75pct",
		//50:  "webtable_sets_50pct",
		//25:  "webtable_sets_25pct",
	}
	webtableListTables = map[int]string{
		100: "webtable_inverted_lists",
		75:  "webtable_inverted_lists_75pct",
		50:  "webtable_inverted_lists_50pct",
		25:  "webtable_inverted_lists_25pct",
	}
	webtableReadSetCostSampleTables = map[int]string{
		100: "webtable_read_set_cost_samples",
		75:  "webtable_read_set_cost_samples_75pct",
		50:  "webtable_read_set_cost_samples_50pct",
		25:  "webtable_read_set_cost_samples_25pct",
	}
	webtableReadListCostSampleTables = map[int]string{
		100: "webtable_read_list_cost_samples",
		75:  "webtable_read_list_cost_samples_75pct",
		50:  "webtable_read_list_cost_samples_50pct",
		25:  "webtable_read_list_cost_samples_25pct",
	}
	webtableMinhashTables = map[int]string{
		100: "webtable_minhash",
		75:  "webtable_minhash_75pct",
		50:  "webtable_minhash_50pct",
		25:  "webtable_minhash_40pct",
	}
	webtableQueryTables = [][]string{
		[]string{"100", "webtable_queries_100"},
		[]string{"1k", "webtable_queries_1k"},
		[]string{"10k", "webtable_queries_10k"},
	}
	setTableSizeCounts = map[string]int{
		"webtable_sets": 163510917,
	}
	batchSizes = map[string]int{
		"webtable_queries_100":      100,
		"webtable_queries_1k":       100,
		"webtable_queries_10k":      100,
		"canada_us_uk_queries_1k":   20,
		"canada_us_uk_queries_10k":  20,
		"canada_us_uk_queries_100k": 20,
	}
	ks = []int{
		1,
		5,
		10,
		20,
		30,
		50,
	}
	// The budget for expensive estimation when choosing between
	// reading candidate set and reading the next batch of posting lists
	// This is the cap on num_candidate * num_estimation.
	// Setting this number 0 forces fast estimate for all candidates.
	// Setting this number of maximum int forces expensive estimation.
	expensiveEstimationBudget = int(math.MaxInt64)
	// expensiveEstimationBudget = 5000
	// The number of posting lists in a batch
	// Use 20 for canada_us_uk and 5 for webtable
	batchSize = 20
	// The algorithms to run
	algorithms = map[string]func(db *sql.DB, listTable, setTable string, tb tokenTable, q rawTokenSet, k int, ignoreSelf bool) ([]searchResult, experimentResult){
		// MergeList
		// "merge_list":                    searchMergeList,

		// MergeList-D
		"merge_distinct_list": searchMergeDistinctList,

		// ProbeSet
		// "probe_set_suffix":              searchProbeSetSuffix,

		// ProbeSet-D
		"probe_set_optimized":           searchProbeSetOptimized,

		// JOSIE
		"merge_probe_cost_model_greedy": searchMergeProbeCostModelGreedy,
	}
	lshAlgorithms = map[string]func(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, q rawTokenSet, k int, ignoreSelf bool, groundTruth []searchResult) ([]searchResult, experimentResult){
		"lsh_ensemble_precision_90": searchLSHEnsemblePrecision90,
		"lsh_ensemble_precision_60": searchLSHEnsemblePrecision60,
	}
	groundTruthExactAlgorithm = "merge_distinct_list"
)

type experimentResult struct {
	QueryID         int64  `csv:"query_id"`
	QuerySize       int    `csv:"query_size"`
	QueryNumToken   int    `csv:"query_num_token"`
	NumResult       int    `csv:"num_result"`
	Duration        int    `csv:"duration"`
	PreprocDuration int    `csv:"preproc_duration"`
	NumSetRead      int    `csv:"num_set_read"`
	NumListRead     int    `csv:"num_list_read"`
	NumByteRead     int    `csv:"num_byte_read"`
	MaxSetSizeRead  int    `csv:"max_set_size_read"`
	MaxListSizeRead int    `csv:"max_list_size_read"`
	MaxCounterSize  int    `csv:"max_counter_size"`
	IgnoreSize      int    `csv:"max_ignore_size"`
	Actions         string `csv:"actions"` // "l" means read a list, "s" means read a set, "o" means overlap size
	Results         string `csv:"results"` // "s" means a set, "o" means overlap size
	// These properties are for merge probe algorithm only
	BenefitCosts string `csv:"benefit_cost"`
	// These properties are for LSH Ensemble algorithm only
	LSHDuration  int     `csv:"lsh_duration"`
	LSHPrecision float64 `csv:"lsh_precision"`
}

func init() {
	rand.Seed(int64(43))
}

// RunOpenDataExperiments run all experiments using Open Data
func RunOpenDataExperiments(db *sql.DB, outputDir string, cpuProfile bool, useMemTokenTable bool) {
	for pct, setTable := range openDataSetTables {
		log.Printf("== Begin experiments using %d percent of sets", pct)
		// Make the subdirectory for this index size
		outputSubDir := filepath.Join(outputDir, strconv.Itoa(pct))
		// Get required table names
		listTable := openDataListTables[pct]
		minhashTable := openDataMinhashTables[pct]
		readListCostSampleTable := openDataReadListCostSampleTables[pct]
		readSetCostSampleTable := openDataReadSetCostSampleTables[pct]
		// Reset cost parameters
		resetCostFunctionParameters(db, readListCostSampleTable, readSetCostSampleTable)
		// Start experiments
		runExperiments(db, listTable, setTable, minhashTable, openDataQueryTables,
			ks, outputSubDir, cpuProfile, useMemTokenTable, pct == 100)
	}
}

// RunWebTableExperiments run all experiments using Web Table
func RunWebTableExperiments(db *sql.DB, outputDir string, cpuProfile bool, useMemTokenTable bool) {
	for pct, setTable := range webtableSetTables {
		log.Printf("== Begin experiments using %d percent of sets", pct)
		// Make the subdirectory for this index size
		outputSubDir := filepath.Join(outputDir, strconv.Itoa(pct))
		// Get required table names
		listTable := webtableListTables[pct]
		minhashTable := webtableMinhashTables[pct]
		readListCostSampleTable := webtableReadListCostSampleTables[pct]
		readSetCostSampleTable := webtableReadSetCostSampleTables[pct]
		// Reset cost parameters
		resetCostFunctionParameters(db, readListCostSampleTable, readSetCostSampleTable)
		// Start experiments
		runExperiments(db, listTable, setTable, minhashTable, webtableQueryTables,
			ks, outputSubDir, cpuProfile, useMemTokenTable, pct == 100)
	}
}

func runExperiments(db *sql.DB, listTable, setTable, minhashTable string, queryTables [][]string, ks []int, outputDir string, cpuProfile bool, useMemTokenTable bool, queryIgnoreSelf bool) {
	var lsh *lshensemble.LshEnsemble
	var tb tokenTable
	log.Println("Counting total number of sets")
	totalNumberOfSets = float64(countTotalNumberOfSets(db, setTable))
	log.Printf("Total number of sets is %.0f", totalNumberOfSets)
	log.Println("Creating token table...")
	if useMemTokenTable {
		tb = createTokenTableMem(db, listTable, queryIgnoreSelf)
	} else {
		tb = createTokenTableDisk(db, listTable, queryIgnoreSelf)
	}
	for _, t := range queryTables {
		scale := t[0]
		queryTable := t[1]
		log.Printf("=== Begin experiments for scale [%s] using queries in %s", scale, queryTable)
		queries := querySets(db, listTable, queryTable)
		for _, k := range ks {
			log.Printf("==== Begin experiments for k = %d", k)
			for name, searchFunc := range algorithms {
				if k != 10 && name == "merge_list" {
					log.Printf("Skipping k = %d for [%s] algorithm because it is the same for k = 10", k, name)
					continue
				}
				// Create output directory
				if err := os.MkdirAll(filepath.Join(outputDir, name), 0700); err != nil {
					panic(err)
				}
				outputFilename := filepath.Join(outputDir, name,
					fmt.Sprintf("%s_%d.csv", scale, k))
				// In case we need profiling
				var cpuProfileFilename string
				if cpuProfile {
					cpuProfileFilename = filepath.Join(outputDir, name,
						fmt.Sprintf("%s_%d.prof", scale, k))
				}
				log.Printf("Running algorithm [%s], output to %s", name, outputFilename)
				runExperiment(db, listTable, setTable, tb, queries, k, queryIgnoreSelf, searchFunc, outputFilename, cpuProfileFilename)
				log.Printf("Finished running algorithm [%s]", name)
			}
			var groundTruths map[int64][]searchResult
			for name, searchFunc := range lshAlgorithms {
				// Initialize LSH Ensemble index lazily
				if lsh == nil {
					lsh = createLSHEnsemble(db, minhashTable)
				}
				// Initalize ground truth results lazily
				if groundTruths == nil {
					groundTruthFilename := filepath.Join(outputDir, groundTruthExactAlgorithm,
						fmt.Sprintf("%s_%d.csv", scale, k))
					groundTruths = readGroundTruths(groundTruthFilename)
				}
				// Create output directory for the experimental result file
				if err := os.MkdirAll(filepath.Join(outputDir, name), 0700); err != nil {
					panic(err)
				}
				outputFilename := filepath.Join(outputDir, name,
					fmt.Sprintf("%s_%d.csv", scale, k))
				// In case we need to do profiling
				var cpuProfileFilename string
				if cpuProfile {
					cpuProfileFilename = filepath.Join(outputDir, name,
						fmt.Sprintf("%s_%d.prof", scale, k))
				}
				// Running the algorithm
				log.Printf("Running algorithm [%s], output to %s", name, outputFilename)
				runLSHExperiment(db, setTable, lsh, tb, queries, k, queryIgnoreSelf,
					groundTruths, searchFunc, outputFilename, cpuProfileFilename)
				log.Printf("Finished running algorithm [%s]", name)
			}
			log.Printf("Finished experiments for k = %d", k)
		}
		log.Printf("Finished experiments for scale [%s] using queries in %s", scale, queryTable)
	}
	log.Println("Conguratuation! You have finished all experiments!")
}

func runExperiment(
	db *sql.DB,
	listTable, setTable string,
	tb tokenTable,
	queries []rawTokenSet,
	k int,
	queryIgnoreSelf bool,
	searchFunc func(db *sql.DB, listTable, setTable string, tb tokenTable, q rawTokenSet, k int, queryIgnoreSelf bool) ([]searchResult, experimentResult),
	outputFilename, cpuProfileFilename string,
) {
	log.Println("Dropping system file cache...")
	if err := exec.Command("sudo", "/usr/local/bin/drop_caches").Run(); err != nil {
		panic(err)
	}
	if cpuProfileFilename != "" {
		f, err := os.Create(cpuProfileFilename)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	perfs := []*experimentResult{}
	start := time.Now()
	rand.Seed(int64(43))
	for i, j := range rand.Perm(len(queries)) {
		query := queries[j]
		// log.Printf("Running query (ID: %v, size: %v, #%v/%v)", query.ID, len(query.RawTokens), i+1, len(queries))
		_, expResult := searchFunc(db, listTable, setTable, tb, query, k, queryIgnoreSelf)
		// log.Printf("Finished query in %v ms, %v set I/Os, %v list I/Os", expResult.Duration, expResult.NumSetRead, expResult.NumListRead)
		// log.Printf("Results: %v", results)
		perfs = append(perfs, &expResult)
		if len(perfs)%100 == 0 {
			writeExperimentResults(perfs, outputFilename)
		}
		fmt.Printf("\r%d / %d queries", i+1, len(queries))
	}
	fmt.Println()
	log.Printf("Finished all queries in %d minutes", time.Now().Sub(start)/time.Minute)
	writeExperimentResults(perfs, outputFilename)
}

func runLSHExperiment(
	db *sql.DB,
	setTable string,
	lsh *lshensemble.LshEnsemble,
	tb tokenTable,
	queries []rawTokenSet,
	k int,
	queryIgnoreSelf bool,
	groundTruths map[int64][]searchResult,
	searchFunc func(db *sql.DB, setTable string, lsh *lshensemble.LshEnsemble, tb tokenTable, q rawTokenSet, k int, queryIgnoreSelf bool, groundTruth []searchResult) ([]searchResult, experimentResult),
	outputFilename, cpuProfileFilename string,
) {
	log.Println("Dropping system file cache...")
	if err := exec.Command("sudo", "/usr/local/bin/drop_caches").Run(); err != nil {
		panic(err)
	}
	if cpuProfileFilename != "" {
		f, err := os.Create(cpuProfileFilename)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	perfs := []*experimentResult{}
	rand.Seed(int64(43))
	for i, j := range rand.Perm(len(queries)) {
		query := queries[j]
		// log.Printf("Running query (ID: %v, size: %v, #%v/%v)", query.ID, len(query.RawTokens), i+1, len(queries))
		_, expResult := searchFunc(db, setTable, lsh, tb, query, k, queryIgnoreSelf, groundTruths[query.ID])
		// log.Printf("Finished query in %v ms, %v set I/Os, %v list I/Os", expResult.Duration, expResult.NumSetRead, expResult.NumListRead)
		// log.Printf("Results: %v", results)
		perfs = append(perfs, &expResult)
		if len(perfs)%100 == 0 {
			writeExperimentResults(perfs, outputFilename)
		}
		fmt.Printf("\r%d / %d queries", i+1, len(queries))
	}
	fmt.Println()
	writeExperimentResults(perfs, outputFilename)
}

func countTotalNumberOfSets(db *sql.DB, setTable string) int {
	if c, exists := setTableSizeCounts[setTable]; exists {
		return c
	}
	var count int
	err := db.QueryRow(fmt.Sprintf(`
		SELECT count(id) FROM %s;`, pq.QuoteIdentifier(setTable))).Scan(&count)
	if err != nil {
		panic(err)
	}
	return count
}

func writeExperimentResults(expResults []*experimentResult, filename string) {
	data, err := gocsv.MarshalBytes(&expResults)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(filename, data, os.ModePerm); err != nil {
		panic(err)
	}
}

func writeResultString(results []searchResult) string {
	resultStr := ""
	for _, result := range results {
		resultStr += fmt.Sprintf("s%do%d", result.ID, result.Overlap)
	}
	return resultStr
}

func readResultString(resultStr string) []searchResult {
	result := make([]searchResult, 0)
	if len(resultStr) == 0 {
		return result
	}
	for _, r := range strings.Split(resultStr, "s") {
		if len(r) == 0 {
			continue
		}
		p := strings.Split(r, "o")
		id, err := strconv.Atoi(p[0])
		if err != nil {
			panic(err)
		}
		overlap, err := strconv.Atoi(p[1])
		if err != nil {
			panic(err)
		}
		result = append(result, searchResult{int64(id), overlap})
	}
	return result
}

// Get the ground truth results
func readGroundTruths(filename string) map[int64][]searchResult {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	expResults := []*experimentResult{}
	if err := gocsv.UnmarshalFile(file, &expResults); err != nil {
		panic(err)
	}
	groundTruth := make(map[int64][]searchResult)
	for _, expResult := range expResults {
		results := readResultString(expResult.Results)
		groundTruth[expResult.QueryID] = results
	}
	return groundTruth
}

type collecter struct {
	items []string
	input chan string
	lock  sync.WaitGroup
}

func newCollecter(capacity int) *collecter {
	return &collecter{
		items: make([]string, 0, capacity),
		input: make(chan string),
		lock:  sync.WaitGroup{},
	}
}

func (c *collecter) start() {
	c.lock.Add(1)
	go func() {
		defer c.lock.Done()
		for item := range c.input {
			c.items = append(c.items, item)
		}
	}()
}

func (c *collecter) done() {
	close(c.input)
}

func (c *collecter) collect() string {
	c.lock.Wait()
	return strings.Join(c.items, "")
}

type actionCollecter struct {
	*collecter
}

func newActionCollecter(capacity int) *actionCollecter {
	return &actionCollecter{newCollecter(capacity)}
}

func (ac *actionCollecter) addReadList(length int) {
	ac.input <- fmt.Sprintf("l%d", length)
}

func (ac *actionCollecter) addReadSet(size, overlap int) {
	ac.input <- fmt.Sprintf("s%do%d", size, overlap)
}

type benefitCostCollecter struct {
	*collecter
}

func newBenefitCostCollecter(capacity int) *benefitCostCollecter {
	return &benefitCostCollecter{newCollecter(capacity)}
}

func (bc *benefitCostCollecter) add(readListBenefit, readListCost, readSetBenefit, readSetCost int) {
	bc.input <- fmt.Sprintf("l%dc%ds%dc%d", readListBenefit, readListCost, readSetBenefit, readSetCost)
}
