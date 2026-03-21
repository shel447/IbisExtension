from __future__ import annotations

import ibis

from ibis_dsql import compile as compile_expr
from ibis_dsql import to_sql
from ibis_dsql.api import to_sqlglot


def test_to_sql_uses_dsql_generator_for_postgres_like_queries():
    table = ibis.table([("id", "int64"), ("name", "string")], name="users")
    expr = table.select(table.id.name("user_id"), table.name).filter(table.id > 1)

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id AS user_id, t0.name FROM users AS t0 WHERE t0.id > 1"
    )


def test_compile_and_to_sqlglot_return_matching_ast():
    table = ibis.table([("value", "int64")], name="metrics")
    expr = table.aggregate(total=table.value.sum())

    compiled = compile_expr(expr)
    alias = to_sqlglot(expr)

    assert compiled == alias
