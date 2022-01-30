
\set dsn_path `echo "'${DSN_ROOT:-$PWD}/clickhouse.toml'"`
\echo :dsn_path

-- hello world example

drop table if exists clickhouse_tables;

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
    dsn_path=:dsn_path,
    dsn='clickhouse_test'
);

select throw_error(database || '.' || name || ' not found')
from (
    select 'system' as database, 'tables' as name
    minus
    select database, name
    from clickhouse_tables
) t;


-- pushdown examples

drop table if exists clickhouse_processes;

create external table clickhouse_processes
(
    "user"              varchar,
    elapsed             float, 
    read_rows           int, 
    read_bytes          int, 
    total_rows_approx   int,
    memory_usage        int, 
    peak_memory_usage   int,
    query               varchar(1024),
    query_id            varchar,
    row_count           int
)
as copy
with source VoidSource()
parser ClickhouseParser(
    relation='system.processes',
    dsn_path=:dsn_path,
    dsn='clickhouse_test'
);


select throw_error('query with pushed down predicate not found')
from (
    select query
    from clickhouse_processes
    where query like '%query%like%'
) t
having count(*) = 0;


select throw_error('query with pushed down group by not found')
from (
    select "user", max(query), sum(row_count)
    from clickhouse_processes
    where query like '%count(*)%group by%user%'
    group by 1
) t
having count(*) = 0;


select throw_error('query with pushed down limit not found')
from (
    select query
    from clickhouse_processes
    where query like '%limit 1%'
    limit 1
) t
having count(*) = 0;


-- semijoin example

create local temp table external_users 
on commit preserve rows as /*+direct*/ 
(
    select distinct "user"
    from clickhouse_processes
)
segmented by hash("user") all nodes;


select throw_error('query with pushed down semijoin not found')
from (
    select query
    from clickhouse_processes
    where "user" in 
    (  
        select ThroughClickhouse("user" using parameters 
                relation='default.test_semijoin_filter', 
                dsn_path=:dsn_path, dsn='clickhouse_test') over(partition nodes) 
        from external_users
    )
      and query like '%"user"%in%default.test_semijoin_filter%'
) t
having count(*) = 0;

