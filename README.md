# ETL Command Line Interface

ETL CLI is a command line client that works with [ETL](https://github.com/vahana/etl) server by exposing various user friendly arguments/parameters and simplifies ETL operations. It works with ETL RESTful endpoints and generates a json payload based on user inputed arguments. 

It hides lots of verbosity and exposes common patterns and shortcuts. Can be used as standalone terminal command or as a library to build other python client.

## Setup and Usage
```
git clone https://github.com/vahana/etl_cli.git
mkvirtualenv etl_cli #or use other ways of creating virtual env
cd etl_cli
pip install --no-deps -r requirements.txt
```

Upon installation, it registers `etl.etl` command line tool.

Typical usage example:

```
# read from regions dataset (in standards databaes, regions collection of mongo), filter only US up to 100 items and create a target dataset in elastic search under `test.us_regions` index.
etl.etl --etl_api="localhost:6544/api" -s mongo/standards/regions -q country.code=US -q _limit=100 -t es/test/us_regions -o create

# read from regions dataset, match with cities dataset and save in us_cities with `city` as unique field
etl.etl --etl_api="localhost:6544/api" -s mongo/standards/regions -q country.code=US --mkeys=city -m mongo/other/cities -t es/test/us_cities -o create --pk=city

```
