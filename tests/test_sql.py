from __future__ import annotations

import ibis
from ibis.backends.sql.compilers.postgres import PostgresCompiler

from ibis_dsql import compile as compile_expr
from ibis_dsql import to_sql
from ibis_dsql.dialect import DSQLDialect


def test_to_sql_supports_order_limit_queries():
    users = ibis.table(
        [("id", "int64"), ("name", "string"), ("score", "int64")], name="users"
    )
    expr = users.filter(users.score > 0).order_by(users.name.desc()).limit(5)

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT * FROM users AS t0 WHERE t0.score > 0 ORDER BY t0.name DESC NULLS LAST LIMIT 5"
    )


def test_to_sql_rewrites_startswith_literal_to_like():
    users = ibis.table([("name", "string")], name="users")
    expr = users.filter(users.name.startswith("ab"))

    sql = to_sql(expr)

    assert sql == "SELECT * FROM users AS t0 WHERE t0.name LIKE 'ab%'"


def test_to_sql_supports_group_filter_queries():
    users = ibis.table([("name", "string"), ("score", "int64")], name="users")
    expr = (
        users.group_by(users.name)
        .aggregate(total=users.score.sum())
        .filter(lambda t: t.total > 10)
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT * FROM (SELECT t0.name, SUM(t0.score) AS total FROM users AS t0 GROUP BY 1) AS t1 WHERE t1.total > 10"
    )


def test_to_sql_supports_join_queries():
    users = ibis.table([("id", "int64"), ("name", "string")], name="users")
    orders = ibis.table([("user_id", "int64"), ("amount", "int64")], name="orders")
    expr = users.join(orders, users.id == orders.user_id).select(users.name, orders.amount)

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t2.name, t3.amount FROM users AS t2 INNER JOIN orders AS t3 ON t2.id = t3.user_id"
    )


def test_to_sql_emits_cte_for_reused_relations():
    users = ibis.table([("id", "int64"), ("score", "int64")], name="users")
    base = users.filter(users.score > 5)
    right = base.view()
    expr = base.join(right, base.id == right.id).select(
        base.id, left_score=base.score, right_score=right.score
    )

    sql = to_sql(expr)

    assert (
        sql
        == "WITH t1 AS (SELECT * FROM users AS t0 WHERE t0.score > 5) SELECT t3.id, t3.score AS left_score, t4.score AS right_score FROM t1 AS t3 INNER JOIN t1 AS t4 ON t3.id = t4.id"
    )


def test_compile_optimize_reparses_compiled_sql():
    users = ibis.table([("id", "int64")], name="users")
    expr = users.filter(ibis.literal(True) & (users.id > 1))

    optimized = compile_expr(expr, optimize=True, schema={"users": {"id": "BIGINT"}})

    assert optimized.sql(dialect=DSQLDialect) == (
        "SELECT t0.id AS id FROM users AS t0 WHERE t0.id > 1"
    )


def test_dsql_overrides_postgres_function_and_type_output():
    users = ibis.table([("id", "int64")], name="users")
    cast_expr = users.select(users.id.cast("string").name("id_text"))

    assert to_sql(cast_expr) == (
        "SELECT CAST(t0.id AS STRING) AS id_text FROM users AS t0"
    )

    postgres_sql = PostgresCompiler().to_sqlglot(ibis.random()).sql(dialect="postgres")

    assert postgres_sql == 'SELECT RANDOM() AS "RandomScalar()"'
    assert to_sql(ibis.random()) == "SELECT RAND() AS RandomScalar()"
