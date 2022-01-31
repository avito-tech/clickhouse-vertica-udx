#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import os
import re
import sys
from collections import defaultdict, namedtuple
from datetime import timedelta

from flask import (
    Flask,
    jsonify,
    render_template_string,
    request,
)
from vertica_parser import db_catalog, make_parser
from vertica_parser.analyzer import (
    CopyNode,
    ExplainNode,
    RootNode,
    SelectNode,
    SimpleSelectNode,
    TableNode,
    find_all_nodes,
    parse_statement,
    list_joins,
)
from vertica_parser.rewriter import (
    fmt_predicate,
    rewrite_statement,
)
from vertica_python import connect, parse_dsn
from threading import Lock

PORT = 9876
DEBUG = False
AGG_FUNCS = ['count', 'sum', 'avg', 'max', 'min']

mutex = Lock()

StorageAccess = namedtuple('StorageAccess', [
    'found_table', 'columns', 'formal_columns',
    'predicates', 'semijoin', 'groupby', 'expr', 'limit'
])

app = Flask(__name__)
app.url_map.strict_slashes = False
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(days=1)


replace_array_in = re.compile(r'=\s*ANY\s*\(\s*ARRAY\s*\[([^]]*)\]\s*\)', flags=re.I)
replace_array_notin = re.compile(r'<>\s*ALL\s*\(\s*ARRAY\s*\[([^]]*)\]\s*\)', flags=re.I)
replace_cast = re.compile(r'::([a-z]+(\([^()]*\))?)', flags=re.I)


try:
    unicode
except BaseException:
    unicode = str


def balanced_parens_reversed(s):
    stack = 0

    rev = reversed(s)
    for i, c in enumerate(rev):
        if c == ')':
            stack += 1
        elif c == '(':
            stack -= 1

        if stack < 0:
            return ''
        elif stack == 0 and s[-1] == ')':
            return s[len(s)-i-1:]

    return ''


def cast_to_ansii(pred):
    subs_pred = pred
    for m in replace_cast.finditer(pred):
        cast = m.group()
        expr = balanced_parens_reversed(pred[0:m.start()])
        if expr:
            cast_ansii = ' cast(' + expr + ' as ' + cast[2:] + ')'
            subs_pred = subs_pred.replace(expr + cast, cast_ansii)
    return replace_cast.sub('', subs_pred)


def is_agg_expr(col):
    return re.search(r'\b(' + '|'.join(AGG_FUNCS) + r')\(', col.expr, flags=re.I) \
           and not col.func


def get_groupby_pushdown(node):
    assert isinstance(node, SimpleSelectNode)
    return node.groupby_list \
           or node.is_distinct and [c.expr for c in node.column_list] \
           or any([is_agg_expr(c) for c in node.column_list]) and ['NOTHING']


def qualify_table_name(table_name):
    return ((table_name.rpartition('.')[0] or 'public')
            + '.' + table_name.rpartition('.')[2]).lower()


def unqualified_table_name(table_name, schemas=('public', 'v_temp_schema')):
    if not schemas or table_name.rpartition('.')[0].lower() in schemas:
        return table_name.rpartition('.')[2].lower()
    return table_name.lower()


def get_cte_refs(cte):
    if isinstance(cte, TableNode) and isinstance(cte.parent, SelectNode):
        refs = find_all_nodes(cte.parent, TableNode)
        return [r for r in refs if r.select_clause == cte.select_clause and r != cte]


def list_join_predicates(select_node):
    predicate_list = list()
    for j in list_joins(select_node):
        if j.join_predicate:
            predicate_list.append(j.join_predicate)
    return predicate_list


@app.errorhandler(Exception)
def handle_500(error):
    app.logger.exception(error)
    response = jsonify({'error': str(error)})
    response.status_code = 500
    return response


def find_table(cur, rel_id, rel_name):
    name_re = re.sub(r'(_local)$', '', rel_name.rpartition('.')[2], flags=re.I)

    b = r'\b{}\b'.format
    if rel_id:
        rel_re = r'({i}.*{n})|({n}.*{i})'.format(i=b(rel_id), n=b(rel_name))
    else:
        rel_re = b(rel_name)

    cur.execute(
        r"""
        select lower(table_schema || '.' || table_name), table_id::varchar
        from tables
        where regexp_like(table_schema || '.' || table_name || ' '
            || regexp_replace(table_definition, '\-\-.*', ''), '{}', 'in')
           or regexp_like(table_name, '{}', 'in')
        """.format(rel_re, name_re)
    )
    related_tables = {table_id: table_name for table_name, table_id in cur.fetchall()}
    if not related_tables:
        return {}

    cur.execute(
        """
        select table_id::varchar, lower(column_name)
        from columns
        where table_id in ({})
        order by column_id
        """.format(','.join(related_tables.keys()))
    )
    matches = defaultdict(list)
    for table_id, column_name in cur.fetchall():
        matches[related_tables[table_id]].append(column_name)

    return matches


def get_current_statement(cur, session_id):
    cur.execute(
        """
        select current_statement
        from sessions
        where session_id = '{sid}' and node_name = '{node}'
        """.format(sid=session_id, node=session_id.partition('-')[0])
    )
    sql = (cur.fetchall() or [['']])[-1][0]
    if sql and ';' not in sql and '--' not in sql and len(sql) < 32000:
        return sql

    cur.execute(
        """
        select coalesce(request, current_statement)
        from sessions s
        left join (
            select request, s.session_id
            from sessions s
            join dc_requests_issued r using(session_id)
            where s.session_id = '{sid}' and s.node_name = '{node}'
              and (s.transaction_id = r.transaction_id and s.statement_id = r.statement_id
                   or s.transaction_id = r.transaction_id and s.statement_start < r.time
                   or s.transaction_description ilike '%CTAS: INSERT%')
            order by r.request_id desc
            limit 2
        ) r using(session_id)
        where s.session_id = '{sid}' and s.node_name = '{node}'
        """.format(sid=session_id, node=session_id.partition('-')[0])
    )
    sql = (cur.fetchall() or [['']])[-1][0]
    return sql


def expand_query(cur, sql):
    views = re.findall(r'\b[_a-z0-9]+\.[_a-z0-9]*v_[_a-z0-9]+\b', sql, flags=re.I)
    views = [str(v.lower()) for v in views]

    if not views:
        return sql

    cur.execute(
        """
        select lower(table_schema || '.' || table_name), view_definition 
        from views 
        where lower(table_schema || '.' || table_name) = any(array{views}) 
        """.format(views=str(views))
    )
    view_definition = {row[0]: row[1] for row in cur.fetchall()}

    patched_sql = sql
    for n, ddl in view_definition.items():
        patched_sql = patched_sql.replace(n, '(' + ddl + ') as ' + n.partition('.')[2])

    return patched_sql


def predicate_to_ansii(pred):
    pred = replace_array_notin.sub(r'not in (\1)', pred)
    pred = replace_array_in.sub(r'in (\1)', pred)
    pred = cast_to_ansii(pred)
    pred = pred.replace('!~~*', 'not ilike')
    pred = pred.replace('~~*', 'ilike')
    pred = pred.replace('!~~', 'not like')
    pred = pred.replace('~~', 'like')
    return pred


def get_storage_access(sql, matched_tables):
    # parse query
    parser = make_parser(trap_errors=True)
    syntax_tree = parser.parse(sql, lexer=parser.lexer)

    # analyze nodes
    mutex.acquire()

    db_catalog.table_columns = dict()
    for table_name, table_columns in matched_tables.items():
        db_catalog.table_columns[qualify_table_name(table_name)] = table_columns
        db_catalog.table_columns[unqualified_table_name(table_name)] = table_columns

    root = RootNode()
    parse_statement(syntax_tree, root)

    mutex.release()

    # rewrite predicates
    stmt = root.statements[0]
    if isinstance(stmt, ExplainNode):
        stmt = stmt.stmt
    rewrite_statement(stmt)

    # find storage access node
    found_table = None
    found_cnt = 0

    columns = set()
    filter_columns = set()
    predicates = list()
    semijoin = list()
    groupby = list()
    expr = list()
    limit = None

    for rel in find_all_nodes(stmt, TableNode):
        if not rel.table_ref:
            continue
        if qualify_table_name(rel.table_ref) not in matched_tables:
            continue
        found_table = qualify_table_name(rel.table_ref)
        found_cnt += 1

        table_names = filter(
            None, [
                qualify_table_name(found_table),
                unqualified_table_name(found_table, schemas=None),
                rel.alias,
            ],
        )
        unqualify = re.compile(r'\b(' + '|'.join(table_names) + r')\.', flags=re.I)

        if rel.filter_predicate:
            pred = unqualify.sub('', fmt_predicate(rel.filter_predicate, semijoin=False))
            semi = unqualify.sub('', fmt_predicate(rel.filter_predicate, semijoin=True))
            if pred:
                predicates.append(predicate_to_ansii(pred))
            semijoin = predicate_to_ansii(semi)

        parent_node = rel.parent
        while parent_node and not isinstance(parent_node, SimpleSelectNode):
            parent_node = parent_node.parent

        if isinstance(parent_node, SimpleSelectNode):
            groupby = get_groupby_pushdown(parent_node)
            expr = [c.expr for c in parent_node.column_list if is_agg_expr(c)]

            inputs = sum([p.input_columns for p in list_join_predicates(parent_node)], [])
            inputs += sum([c.input_columns for c in parent_node.column_list], [])
            columns.update({unqualify.sub('', i) for i in inputs})

            filter_inputs = parent_node.where_clause.input_columns
            filter_columns.update({unqualify.sub('', i) for i in filter_inputs})

        while parent_node and not groupby:
            grandpa = parent_node.parent
            if isinstance(parent_node, TableNode) and isinstance(grandpa, SelectNode):
                refs = get_cte_refs(parent_node) or [parent_node]
                grandpa = refs[0].parent
            elif isinstance(parent_node, SimpleSelectNode):
                groupby = get_groupby_pushdown(parent_node)
            limit = limit or getattr(parent_node, 'limit', None)
            parent_node = grandpa

    if isinstance(stmt, CopyNode):
        found_table = stmt.name
        columns = {'*'}
        matched_tables[found_table] = columns

    return StorageAccess(found_table, columns | filter_columns, filter_columns - columns,
                         predicates, semijoin if found_cnt == 1 else None,
                         groupby, expr, limit)


@app.route('/storage_access/<path:session_id>')
def storage_access(session_id):
    assert request.args.get('table'), 'no table specified'
    rel_name = request.args.get('table').lower()
    rel_id = request.args.get('id')

    # fill query context
    with connect(**get_credits(request.remote_addr)) as con:
        cur = con.cursor()

        matched_tables = find_table(cur, rel_id, rel_name)

        sql = get_current_statement(cur, session_id)
        assert sql, 'no running sql statement found'
        assert '--' not in sql or '\n' in sql, 'single line comments not allowed'

        sql = expand_query(cur, sql)

    sql = unicode(sql)

    # parse query
    app.logger.info(u'parse [{}] for query:\n{}'.format('|'.join(matched_tables), json.dumps(sql)))

    a = get_storage_access(sql, matched_tables)

    assert matched_tables, 'no matching external or copy table found for [%s]' % rel_name
    assert a.found_table, 'no external table for [%s] found in a query' % rel_name

    # fmt storage access
    col_names = [c for c in matched_tables[a.found_table] if c in a.columns]
    res = u'table:{}\ncolumns:{}'.format(rel_name, ','.join(col_names))
    if a.formal_columns:
        res += u'\nformal_columns:{}'.format(','.join(a.formal_columns))
    if a.predicates:
        res += u'\nfilter:({})'.format(' or '.join(a.predicates))
    if a.semijoin:
        res += u'\nsemijoin:{}'.format(a.semijoin)
    if a.expr:
        res += u'\nexpr:{}'.format(';'.join(a.expr))
    if a.groupby:
        res += u'\ngroupby:{}'.format(';'.join(a.groupby))
    if a.limit:
        res += u'\nlimit:{}'.format(a.limit)
    app.logger.info(u'storage access:\n' + res)
    return res


@app.route('/')
def index(**kwargs):
    return render_template_string("""
        <head>
            <title>query service</title>
            <link rel="icon" href="data:;base64,iVBORw0KGgo=">
        </head>
        <body>
            <h1> Query analyzer service for vertica </h1>
            <p>
                <b> /storage_access/[session_id]?table=[tbl] </b> <br/>
                Get storage access query on current statement in [session_id] for [tbl]
            </p>
        </body>
    """)


def get_credits(host):
    vcred = parse_dsn(os.getenv('CREDITS'))
    vcred['host'] = host
    return vcred


if __name__ == '__main__':
    # run app
    sys.setrecursionlimit(10000)
    app.logger.setLevel(20)
    app.run(port=PORT, host='0.0.0.0', debug=DEBUG)
