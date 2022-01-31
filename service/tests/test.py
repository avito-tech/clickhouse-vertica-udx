#!/usr/bin/env python
# -*- coding: utf-8 -*-


from bin.query_service import get_storage_access, cast_to_ansii


def test_sql1():
    sql = """
    create local temp table tmp_events on commit preserve rows as /*+direct*/
    select
        track_id,
        event_no,
        adjust_tracker,
        event_timestamp,
        event_type_id,
        url_id,
        url_hash,
        url
    from ext.clickstream
    where event_date between :first_date and :last_date
        and eid = 2063
    order by track_id, event_no
    segmented by hash(track_id) all nodes;
    """
    
    matched_tables = {'ext.clickstream': [
        'eid', 'event_date', 'track_id', 'event_no', 'url_hash', 'url',
        'adjust_tracker', 'event_timestamp', 'event_type_id', 'url_id',
    ]}
    
    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_date']])
    assert len(a.predicates) == 1, '\n'.join(a.predicates)
    assert all([c in a.predicates[0] for c in ['eid', 'event_date']])
    assert not a.limit


def test_sql2():
    sql = """
    INSERT /*+DIRECT*/ INTO v_temp_schema.tmp_events 
    SELECT 
        clickstream.track_id,
        clickstream.event_no, 
        clickstream.adjust_tracker, 
        clickstream.event_timestamp,
        clickstream.event_type_id, 
        clickstream.url_id, 
        clickstream.url_hash, 
        clickstream.url
    FROM ext.clickstream 
    WHERE ((clickstream.event_date >= '2021-07-18'::date)
       AND (clickstream.event_date <= '2021-07-18'::date) 
       AND (clickstream.eid = 2063)) 
    ORDER BY clickstream.track_id, clickstream.event_no
    """

    matched_tables = {'ext.clickstream': [
        'eid', 'event_date', 'track_id', 'event_no', 'url_hash', 'url',
        'adjust_tracker', 'event_timestamp', 'event_type_id', 'url_id',
    ]}

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_date']])
    assert len(a.predicates) == 1, '\n'.join(a.predicates)
    assert all([c in a.predicates[0] for c in ['eid', 'event_date']])
    assert not a.limit


def test_sql_cte_groupby():
    sql = """
    with t as (
        select eid, event_date, sum(row_count) as row_count
        from ext.clickstream
        group by 1,2
    )
    select *
    from dma.current_eventTypes
    join t on EventType_ext = eid 
    where event_date = '2021-07-18'
    """

    matched_tables = {'ext.clickstream': [
        'eid', 'event_date', 'row_count',
    ]}

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_date', 'row_count']])
    assert len(a.predicates) == 1, '\n'.join(a.predicates)
    assert all([c in a.predicates[0] for c in ['event_date']])
    assert a.groupby
    assert not a.limit


def test_sql_distinct():
    sql = """
    select distinct eid
    from ext.clickstream
    where event_date = '2021-07-18'
    """

    matched_tables = {'ext.clickstream': [
        'eid', 'event_date',
    ]}

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_date']])
    assert len(a.predicates) == 1, '\n'.join(a.predicates)
    assert all([c in a.predicates[0] for c in ['event_date']])
    assert a.groupby
    assert not a.limit


def test_sql_formal_columns():
    sql = """
    select event_timestamp, eid, cookie
    from ext.clickstream
    where launch_id = 12345
    limit 1000
    """

    matched_tables = {'ext.clickstream': [
        'eid', 'event_timestamp', 'cookie', 'launch_id'
    ]}

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_timestamp', 'cookie']])
    assert all([c in a.formal_columns for c in ['launch_id']])
    assert len(a.predicates) == 1, '\n'.join(a.predicates)
    assert all([c in a.predicates[0] for c in ['launch_id']])
    assert a.limit


def test_sql_cte_predicates():
    sql = """
    with t as (
        select eid, event_date, sum(row_count) as row_count
        from ext.clickstream
        group by 1,2
    ),
    t1 as (
        select event_date, row_count
        from t 
        where eid = 301
    )
    select *
    from t1
    where t1.event_date = cast(now() as date) - 1
    """

    matched_tables = {'ext.clickstream': [
        'eid', 'event_date', 'row_count',
    ]}

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_date', 'row_count']])
    assert len(a.predicates) == 1, '\n'.join(a.predicates)
    assert all([c in a.predicates[0] for c in ['event_date', 'eid']])
    assert a.groupby
    assert not a.limit


def test_sql_multicte_predicates():
    sql = """
    with t as (
        select eid, event_date, sum(row_count) as row_count
        from ext.clickstream
        group by 1,2
    ),
    t1 as (
        select event_date, row_count
        from t 
        where eid = 301
    ),
    t2 as (
        select event_date, row_count
        from t 
        where eid = 303
    )
    select t2.row_count / t1.row_count
    from t1
    join t2 using(event_date)
    where t1.event_date = cast(now() as date) - 1
    """

    matched_tables = {'ext.clickstream': [
        'eid', 'event_date', 'row_count',
    ]}

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_date', 'row_count']])
    assert len(a.predicates) == 2, '\n'.join(a.predicates)
    assert all([all(c in p for c in ['event_date', 'eid']) for p in a.predicates])
    assert a.groupby
    assert not a.limit


def test_cast():
    pred = "((event_date >= ((now())::date - 2)) and (hold_reason <> ''::varchar))"
    assert cast_to_ansii(pred).replace(" ", "") == \
           "((event_date >= (cast((now()) as date) - 2)) and (hold_reason <> ''))".replace(" ", "")


def test_semijoin_parsing():
    sql = """
    with stat as (
        select eid, event_date, sum(row_count) as row_count
        from ext.clickstream
        where event_date = cast(now() as date)
        group by 1,2
    )
    select t.eventtype_id
    from dma.current_eventtypes t
    where t.eventtype_ext in (select eid from stat)
    """

    matched_tables = {
        'ext.clickstream': ['eid', 'event_date', 'row_count'],
    }

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['eid', 'event_date', 'row_count']])
    assert len(a.predicates) == 1
    assert all(c in a.predicates[0] for c in ['event_date'])
    assert a.groupby
    assert not a.limit


def test_semijoin_filter():
    sql = """
    with u as (
        select  External_id
            ,case when ab_test_ext_id = 6549 then 'tarif' else 'services' end as test 
            ,ROW_NUMBER() over(partition by participant_id order by first_exposure_time) as rnk
            ,first_exposure_time::date as exposure_time
        from dma.ab_participant p
        join dict.ab_variant v on v.variant_id = p.variant_id
        join dma.current_user cu on cu.user_id=p.participant_id 
        where ab_test_ext_id in (6550,6549)
          and variant = 'test'
          and not istest
          and first_exposure_time is not null
          and event_date >= '2021-10-05'
    ) 
    select  test
            ,event_date
            ,"user"
            ,is_enough_money
            ,sum(case when eid=2788 then row_count else 0 end) as cnt
    from (
        select "user", event_date, eid, is_enough_money, row_count
        from ext.clickstream cc
        where eid in (2788,5317,5318,5320,5319,5321,5322,5323,5324,5325,5326,5327,5328,2119,5328,3448,4965)
          and business_platform=3
          and "user" in (select External_id from u)
          and event_date >= '2021-12-01'
    ) a 
    join u on "user" = External_id
    where event_date>=exposure_time
    group by 1,2,3,4
    """

    matched_tables = {'ext.clickstream': [
        'eid', 'event_date', 'row_count', 'business_platform', 'user', 'is_enough_money'
    ]}

    a = get_storage_access(sql, matched_tables)

    assert a.found_table == 'ext.clickstream'
    assert all([c in a.columns for c in ['user']])
    assert len(a.predicates) == 1, '\n'.join(a.predicates)
    assert all(c in a.predicates[0] for c in ['eid', 'event_date', 'business_platform'])
    assert 'user' in a.semijoin
    assert a.groupby
    assert not a.limit
