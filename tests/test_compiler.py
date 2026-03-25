from __future__ import annotations

import ibis
import sqlglot.expressions as sge

from ibis_dsql import compile as compile_expr


def test_compile_returns_ast_with_expected_cast_shape():
    users = ibis.table([("id", "int64")], name="users")
    expr = users.select(users.id.cast("string").name("id_text"))

    compiled = compile_expr(expr)
    cast = compiled.find(sge.Cast)
    column = cast.this

    assert isinstance(cast, sge.Cast)
    assert isinstance(column, sge.Column)
    assert column.this.name == "id"
    assert column.table == "t0"
    assert cast.to.this == sge.DataType.Type.VARCHAR


def test_compile_rewrites_epoch_millis_cast_to_from_unixtime_timestamp():
    metrics = ibis.table([("ts_ms", "int64")], name="metrics")
    expr = metrics.select(metrics.ts_ms.cast("timestamp").name("ts"))

    compiled = compile_expr(expr)
    cast = compiled.find(sge.Cast)

    assert cast is not None
    assert cast.to.this == sge.DataType.Type.TIMESTAMP
    assert compiled.find(sge.UnixToTime) is None

    from_unixtime = next(
        node
        for node in compiled.find_all(sge.Anonymous)
        if node.name.upper() == "FROM_UNIXTIME"
    )
    division = from_unixtime.expressions[0]

    assert isinstance(division, sge.Div)
    assert isinstance(division.this, sge.Column)
    assert division.this.this.name == "ts_ms"
    assert division.this.table == "t0"
    assert division.expression == sge.convert(1000)


def test_compile_rewrites_epoch_millis_timestamp_comparison_to_bigint_comparison():
    metrics = ibis.table([("ts_ms", "int64")], name="metrics")
    expr = metrics.filter(metrics.ts_ms.cast("timestamp") >= ibis.timestamp("2026-01-01 08:00:00"))

    compiled = compile_expr(expr)
    comparison = compiled.find(sge.GTE)

    assert comparison is not None
    assert isinstance(comparison.this, sge.Column)
    assert comparison.this.this.name == "ts_ms"
    assert comparison.this.table == "t0"
    assert compiled.find(sge.UnixToTime) is None
    assert isinstance(comparison.expression, sge.Cast)
    assert comparison.expression.to.this == sge.DataType.Type.BIGINT

    multiply = comparison.expression.this
    assert isinstance(multiply, sge.Mul)
    assert multiply.expression == sge.convert(1000)

    unix_timestamp = multiply.this
    assert isinstance(unix_timestamp, sge.Anonymous)
    assert unix_timestamp.name.upper() == "UNIX_TIMESTAMP"
