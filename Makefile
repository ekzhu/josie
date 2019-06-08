sample_canada_us_uk: build
	sample_queries -pg-table-queries=canada_us_uk_queries_1k -sampling-max-query-size=1000 -sampling-num-interval=10 
	sample_queries -pg-table-queries=canada_us_uk_queries_10k -sampling-max-query-size=10000 -sampling-num-interval=10
	sample_queries -pg-table-queries=canada_us_uk_queries_100k -sampling-max-query-size=100000 -sampling-num-interval=10
	sample_queries -pg-table-queries=canada_us_uk_queries_200k -sampling-max-query-size=200000 -sampling-num-interval=10 --sampling-num-query=460

sample_webtable: build
	sample_queries -pg-table-sets=webtable_sets -pg-table-queries=webtable_queries_100 -sampling-max-query-size=100 -sampling-num-interval=5 -sampling-num-query=1000
	sample_queries -pg-table-sets=webtable_sets -pg-table-queries=webtable_queries_1k -sampling-max-query-size=1000 -sampling-num-interval=5 -sampling-num-query=1000
	sample_queries -pg-table-sets=webtable_sets -pg-table-queries=webtable_queries_10k -sampling-max-query-size=5000 -sampling-num-interval=5 -sampling-num-query=1000 

sample_cost_canada_us_uk: build
	sudo /usr/local/bin/drop_caches
	sample_cost -pg-table-read-set-cost-samples=canada_us_uk_read_set_cost_samples -pg-table-read-list-cost-samples=canada_us_uk_read_list_cost_samples -cost-max-list-size=4000 -cost-list-size-step=100 -cost-sample-per-size=10 -pg-table-queries=canada_us_uk_queries_100k

sample_cost_webtable: build
	sudo /usr/local/bin/drop_caches
	sample_cost -pg-table-sets=webtable_sets -pg-table-lists=webtable_inverted_lists -pg-table-read-set-cost-samples=webtable_read_set_cost_samples -pg-table-read-list-cost-samples=webtable_read_list_cost_samples -cost-max-list-size=10000 -cost-list-size-step=1000 -cost-sample-per-size=10 -pg-table-queries=webtable_queries_10k

minhash_canada_us_uk: build
	create_minhash -pg-table-lists=canada_us_uk_inverted_lists -pg-table-sets=canada_us_uk_sets -pg-table-minhash=canada_us_uk_minhash -nworker=32

minhash_webtable: build
	create_minhash -pg-table-lists=webtable_inverted_lists -pg-table-sets=webtable_sets -pg-table-minhash=webtable_minhash -nworker=32

canada_us_uk: build
	topk -benchmark=canada_us_uk

webtable: build
	topk -benchmark=webtable

plot:
	python plot_perf.py

plot_perf_vs_k:
	python plot_perf_vs_k.py --result-dir ./results --benchmark canada_us_uk --query-scale 1k
	python plot_perf_vs_k.py --result-dir ./results --benchmark canada_us_uk --query-scale 10k
	python plot_perf_vs_k.py --result-dir ./results --benchmark canada_us_uk --query-scale 100k

build:
	go install ./...

clean:
	rm -f search *.pdf
	ls results | grep -v ssd | xargs rm -rf
