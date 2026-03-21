from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import ibis.expr.types as ir
import sqlglot as sg
from sqlglot.optimizer import optimize as optimize_expression

from ibis_dsql.compiler import DSQLCompiler
from ibis_dsql.dialect import DSQLDialect


def compile(
    expr: ir.Expr,
    *,
    params: Mapping[ir.Expr, Any] | None = None,
    optimize: bool = False,
    schema: Mapping[str, Any] | None = None,
) -> sg.Expression:
    expression = DSQLCompiler().to_sqlglot(expr, params=params)

    if optimize:
        sql = expression.sql(dialect=DSQLDialect, identify=False)
        expression = optimize_expression(
            sql,
            schema=schema,
            dialect=DSQLDialect,
        )

    return expression


def to_sqlglot(
    expr: ir.Expr,
    *,
    params: Mapping[ir.Expr, Any] | None = None,
    optimize: bool = False,
    schema: Mapping[str, Any] | None = None,
) -> sg.Expression:
    return compile(expr, params=params, optimize=optimize, schema=schema)


def to_sql(
    expr: ir.Expr,
    *,
    params: Mapping[ir.Expr, Any] | None = None,
    optimize: bool = False,
    schema: Mapping[str, Any] | None = None,
    pretty: bool = False,
    identify: bool = False,
    **sql_kwargs: Any,
) -> str:
    expression = compile(expr, params=params, optimize=optimize, schema=schema)
    return expression.sql(
        dialect=DSQLDialect,
        pretty=pretty,
        identify=identify,
        **sql_kwargs,
    )
