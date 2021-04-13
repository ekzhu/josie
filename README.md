# JOSIE: Overlap Set Similarity Search

This repository contains the code and benchmarks for the SIGMOD 2019 paper:
[*JOSIE: Overlap Set Similarity Search for Finding Joinable Tables in Data 
Lakes*](https://dl.acm.org/doi/pdf/10.1145/3299869.3300065).
Follow the steps here to run the experiments.

## Requirements

PostgreSQL and Go are required to run the experiments.

### Postgres

1. [Download](https://www.postgresql.org/ftp/source/v10.0/) and 
[install](https://www.postgresql.org/docs/10/static/install-procedure.html) 
from source. Make sure to use `--prefix=$HOME` to install to user home directory,
and `--with-pgport=5442` to set the port for both server and client.
2. Initialize a database directory: `initdb -D pg_data`.
3. Use the configuration file in `conf/postgresql.conf` to start a server:
`postgres -D pg_data -c config_file=conf/postgresql.conf`.
4. Create a new database same as your Unix user name: `createdb <dbname>`.
5. Test the client-server connection using `psql -p 5442`.

### Go

1. Download and install the [Go programming language](https://golang.org/dl/)
2. Create a directory under your home directory `mkdir ~/go`, this will be your
   go path
3. Make sure you have [set up `$GOPATH`](https://golang.org/doc/install) 
in your bash environment by adding the following lines to your bash profile, then restart your bash session

```
export GOPATH=$HOME/go
export GOBIN=$GOPATH/bin
export PATH=$GOBIN:$PATH
```

4. **Important:** check out this repository under your go path: 
```
mkdir -p ~/go/src/github.com/ekzhu/josie
git clone git@github.com:ekzhu/josie.git ~/go/src/github.com/ekzhu/josie
```

## Run the benchmarks in the original paper

Now go into the project directory at `~/go/src/github.com/ekzhu/josie`.

### Prepare benchmarks

First download the benchmarks in the form of Postgres dumps.

* [Canada-US-UK Open Data](https://storage.googleapis.com/josie-benchmark/canada_us_uk_benchmark.sql.gz)
* [WDC Web Table 2015, English Relational Only](https://storage.googleapis.com/josie-benchmark/webtable_benchmark.sql.gz)

Uncompress the dump files (use `gzip -d`) 
and run the SQL files (or use `pg_restore`)
to load the benchmarks into Postgres. 
Make sure to use the port setting you used when installing 
Postgres earlier, so the dump files get imported into the 
right database.

Then, run the SQL script `create_indexes.sql` to create indexes for the 
sets and posting lists tables.

### Run experiments

We use the targets defined in `Makefile` to run experiments.
First you need to generate a cost sample table to compute the read
cost of sets and posting lists.

```
make sample_cost_canada_us_uk
make sample_cost_webtable
```

To run experiments using the Open Data benchmark:

```
make canada_us_uk
```

Web Table benchmark:

```
make webtable
```

**Notice:** the experiments can take many hours or even days depending on your
hardware environment (SSD will be much faster than HDD). 
To fine tune which experiments to run, you can modify
`exp.go`.

### Plot results

Results are located in the `results` directory. Use the targets defined
in the `Makefile` to plot results:

```
make plot
```

The output plots are located in the `plots` directory.
