from __future__ import annotations

import ibis
import ibis.common.exceptions as com
import pytest
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
        == "SELECT t0.id, t0.name, t0.score FROM users AS t0 WHERE t0.score > 0 ORDER BY t0.name DESC NULLS LAST LIMIT 5"
    )


def test_to_sql_rewrites_startswith_literal_to_like():
    users = ibis.table([("name", "string")], name="users")
    expr = users.filter(users.name.startswith("ab"))

    sql = to_sql(expr)

    assert sql == "SELECT t0.name FROM users AS t0 WHERE t0.name LIKE 'ab%'"


def test_to_sql_rewrites_endswith_literal_to_like():
    users = ibis.table([("name", "string")], name="users")
    expr = users.filter(users.name.endswith("yz"))

    sql = to_sql(expr)

    assert sql == "SELECT t0.name FROM users AS t0 WHERE t0.name LIKE '%yz'"


def test_to_sql_rewrites_string_concat_to_concat_function():
    strings = ibis.table([("a", "string"), ("b", "string")], name="strings")
    expr = strings.select((strings.a + strings.b).name("c"))

    sql = to_sql(expr)

    assert sql == "SELECT CONCAT(t0.a, t0.b) AS c FROM strings AS t0"


def test_to_sql_rewrites_nested_string_concat_to_nested_concat_function():
    strings = ibis.table([("a", "string"), ("b", "string")], name="strings")
    expr = strings.select((strings.a + strings.b + ibis.literal("x")).name("c"))

    sql = to_sql(expr)

    assert sql == "SELECT CONCAT(CONCAT(t0.a, t0.b), 'x') AS c FROM strings AS t0"


def test_to_sql_rewrites_timestamp_string_concat_to_concat_function():
    expr = ibis.timestamp(ibis.now().cast("date").strftime("%Y-%m-%d") + " 09:00:00")

    sql = to_sql(expr)

    assert sql == (
        "SELECT CAST(CONCAT(TO_CHAR(CAST(CURRENT_TIMESTAMP AS DATE), 'YYYY-MM-DD'), "
        "' 09:00:00') AS TIMESTAMP) AS Cast(StringConcat((Strftime(Cast(TimestampNow(), "
        "date), '%Y-%m-%d'), ' 09:00:00')), timestamp)"
    )
    assert "||" not in sql


def test_to_sql_rejects_dynamic_startswith_patterns():
    users = ibis.table([("name", "string"), ("prefix", "string")], name="users")
    expr = users.filter(users.name.startswith(users.prefix))

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support dynamic startswith patterns",
    ):
        to_sql(expr)


def test_to_sql_rejects_dynamic_endswith_patterns():
    users = ibis.table([("name", "string"), ("suffix", "string")], name="users")
    expr = users.filter(users.name.endswith(users.suffix))

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support dynamic endswith patterns",
    ):
        to_sql(expr)


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
        == "SELECT t1.name, t1.total FROM (SELECT t0.name, SUM(t0.score) AS total FROM users AS t0 GROUP BY 1) AS t1 WHERE t1.total > 10"
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
        == "WITH t1 AS (SELECT t0.id, t0.score FROM users AS t0 WHERE t0.score > 5) SELECT t3.id, t3.score AS left_score, t4.score AS right_score FROM t1 AS t3 INNER JOIN t1 AS t4 ON t3.id = t4.id"
    )


def test_to_sql_preserves_count_star_alias():
    users = ibis.table([("id", "int64")], name="users")
    expr = users.aggregate(total=users.count())

    sql = to_sql(expr)

    assert sql == "SELECT COUNT(*) AS total FROM users AS t0"


def test_to_sql_formats_interval_with_separate_unit():
    users = ibis.table([("id", "int64")], name="users")
    expr = users.select(
        (ibis.timestamp("2024-01-01 00:00:00") + ibis.interval(days=1)).name("x")
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT CAST('2024-01-01T00:00:00' AS TIMESTAMP) + INTERVAL '1' DAY AS x FROM users AS t0"
    )


def test_to_sql_rewrites_position_to_instr():
    users = ibis.table([("name", "string")], name="users")
    expr = users.select(users.name.find("abc").name("pos"))

    sql = to_sql(expr)

    assert sql == "SELECT INSTR(t0.name, 'abc') - 1 AS pos FROM users AS t0"


def test_to_sql_rewrites_not_in_to_postfix_not():
    users = ibis.table([("id", "int64")], name="users")
    expr = users.filter(~users.id.isin([1, 2]))

    sql = to_sql(expr)

    assert sql == "SELECT t0.id FROM users AS t0 WHERE t0.id NOT IN (1, 2)"


def test_to_sql_rewrites_not_like_to_postfix_not():
    users = ibis.table([("name", "string")], name="users")
    expr = users.filter(~users.name.like("%x%"))

    sql = to_sql(expr)

    assert sql == "SELECT t0.name FROM users AS t0 WHERE t0.name NOT LIKE '%x%'"


def test_to_sql_rewrites_not_is_null_to_is_not_null():
    users = ibis.table([("name", "string")], name="users")
    expr = users.filter(~users.name.isnull())

    sql = to_sql(expr)

    assert sql == "SELECT t0.name FROM users AS t0 WHERE t0.name IS NOT NULL"


def test_to_sql_rewrites_not_in_subquery_to_postfix_not():
    users = ibis.table([("id", "int64")], name="users")
    rel = users.filter(users.id > 1)
    expr = users.filter(~users.id.isin(rel.id))

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id FROM users AS t0 WHERE t0.id NOT IN (SELECT t0.id FROM users AS t0 WHERE t0.id > 1)"
    )


def test_to_sql_expands_top_level_star_selection():
    users = ibis.table([("id", "int64"), ("name", "string")], name="users")

    sql = to_sql(users)

    assert sql == "SELECT users.id, users.name FROM users"


def test_to_sql_rejects_scalar_subquery_in_where_clause():
    users = ibis.table([("id", "int64"), ("name", "string")], name="users")
    scalar = users.aggregate(mx=users.id.max()).mx.as_scalar()
    expr = users.filter(users.id > scalar)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support scalar subqueries",
    ):
        to_sql(expr)


def test_to_sql_rejects_scalar_subquery_in_select_list():
    users = ibis.table([("id", "int64"), ("name", "string")], name="users")
    scalar = users.aggregate(mx=users.id.max()).mx.as_scalar()
    expr = users.select(mx=scalar)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support scalar subqueries",
    ):
        to_sql(expr)


def test_to_sql_uses_dsql_float_type_names():
    users = ibis.table([("id", "int64")], name="users")
    expr = users.select(
        users.id.cast("float32").name("f32"),
        users.id.cast("float64").name("f64"),
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT CAST(t0.id AS FLOAT) AS f32, CAST(t0.id AS DOUBLE) AS f64 FROM users AS t0"
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
