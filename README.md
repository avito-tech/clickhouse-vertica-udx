
## Overview

UDF to seamlessly connect ClickHouse to Vertica using external tables.

Features:
* UDParser to get data from clickhouse table 
* UDTF to export data to clickhouse from vertica
* pushing down predicates, group by, limit, semijoins
* elimination of unused columns

Connector contains two components:
* vertica udx-library with staticly compiled c++17 based clickhouse client
* query service to parse queries for predicates, group by and so on

# Installation

Tested on debian 10.

Prerequirements:
* vertica
* clickhouse-server
* cmake 3.13+
* gcc 8+
* python 3.5+
* unzip
* build-essential

Run make to build ClickhouseUdf.so:
```
make all
```

To install ClickhouseUdf.so use install.sql:
```
scp ClickhouseUdf.so <host>:/tmp
vsql -U <user> -w <pass> -h <host> -d <DB> -f install.sql
```

# Test environment

Test environment can be set with docker compose:
```
docker-compose up
```
Run tests with:
```
DSN_ROOT=/opt/vertica-udf vsql -U dbadmin -p 54330 -f test.sql
```

# Usage

You basically create external table and use it in queries:

```
create external table clickhouse_tables
(
    database    varchar,
    name        varchar,
    engine      varchar,
    total_rows  int,
    total_bytes int
) as
COPY WITH
SOURCE VoidSource()
PARSER ClickhouseParser(
    relation='system.tables',
    dsn_path='<path/to/dsn.ini>',
    dsn='clickhouse_test'
);
```

In order to avoid credentials in DDL we use DSN file with format like 

```
[clickhouse_test]
database='default'
user='default'
password='*****'
host='127.0.0.1'
port='9000'
```

DSN file must be located on all vertica hosts for udf be able to read from it.

Basic examples can be found in test.sql


# License
MIT

