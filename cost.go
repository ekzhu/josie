package topk

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/lib/pq"
)

var (
	minReadCost           = 1000000.0
	readSetCostSlope      = 1253.19054300781
	readSetCostIntercept  = -9423326.99507381
	readListCostSlope     = 1661.93366983753
	readListCostIntercept = 1007857.48225696
)

func readListCost(length int) float64 {
	f := readListCostSlope*float64(length) + readListCostIntercept
	if f < minReadCost {
		f = minReadCost
	}
	return f / 1000000.0
}

func readSetCost(size int) float64 {
	f := readSetCostSlope*float64(size) + readSetCostIntercept
	if f < minReadCost {
		f = minReadCost
	}
	return f / 1000000.0
}

func readSetCostReduction(size, truncation int) float64 {
	return readSetCost(size) - readSetCost(size-truncation)
}

// resetCostFunctionParameters re-computes the slopes and intercepts of cost functions
func resetCostFunctionParameters(db *sql.DB, pgTableReadListCostSamples, pgTableReadSetCostSamples string) {
	var slope, intercept float64
	err := db.QueryRow(fmt.Sprintf(`
	SELECT regr_slope(cost, frequency), regr_intercept(cost, frequency) from %s;`,
		pq.QuoteIdentifier(pgTableReadListCostSamples))).Scan(&slope, &intercept)
	if err != nil {
		panic(err)
	}
	log.Printf("Reseting read list cost slope %.4f -> %.4f", readListCostSlope, slope)
	log.Printf("Reseting read list cost intercept %.4f -> %.4f", readListCostIntercept, intercept)
	readListCostSlope = slope
	readListCostIntercept = intercept

	err = db.QueryRow(fmt.Sprintf(`
	SELECT regr_slope(cost, size), regr_intercept(cost, size) from %s;`,
		pq.QuoteIdentifier(pgTableReadSetCostSamples))).Scan(&slope, &intercept)
	if err != nil {
		panic(err)
	}
	log.Printf("Reseting read set cost slope %.4f -> %.4f", readSetCostSlope, slope)
	log.Printf("Reseting read set cost intercept %.4f -> %.4f", readSetCostIntercept, intercept)
	readSetCostSlope = slope
	readSetCostIntercept = intercept
}
