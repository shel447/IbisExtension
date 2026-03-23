from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import ibis
import ibis.common.exceptions as com
import ibis.expr.operations as ops
import ibis.expr.types as ir
import sqlglot as sg
import sqlglot.expressions as sge
from sqlglot.optimizer import optimize as optimize_expression

from ibis_dsql.compiler import (
    CONNECT_CHILD_KEY,
    CONNECT_CTE_NAME,
    CONNECT_NOCYCLE,
    CONNECT_PARENT_KEY,
    CONNECT_START_WITH,
    DSQLCompiler,
)
from ibis_dsql.dialect import DSQLDialect


def _bind_single(table: ir.Table, spec: Any, *, name: str) -> ir.Value:
    bound = tuple(table.bind(spec))
    if len(bound) != 1:
        raise com.IbisInputError(f"{name} must resolve to exactly one expression")
    return bound[0]


def _validate_table_reference(
    table: ir.Table, expr: ir.Value, *, name: str, allow_literal: bool = False
) -> None:
    relations = expr.op().relations
    if not relations and allow_literal:
        return
    if relations != frozenset({table.op()}):
        raise com.IbisInputError(f"{name} must reference only the input table")


def connect_by(
    table: ir.Table,
    *,
    start_with: Any,
    parent_key: Any,
    child_key: Any,
    nocycle: bool = False,
    level_name: str = "level",
) -> ir.Table:
    if level_name in table.columns:
        raise com.IbisInputError("level_name conflicts with an existing column")

    start_with_expr = _bind_single(table, start_with, name="start_with")
    parent_key_expr = _bind_single(table, parent_key, name="parent_key")
    child_key_expr = _bind_single(table, child_key, name="child_key")

    if not start_with_expr.type().is_boolean():
        raise com.IbisTypeError("start_with must be a boolean expression")

    _validate_table_reference(table, parent_key_expr, name="parent_key")
    _validate_table_reference(table, child_key_expr, name="child_key")
    _validate_table_reference(
        table, start_with_expr, name="start_with", allow_literal=True
    )

    relation = table.mutate(
        **{
            level_name: ibis.literal(0).cast("int64"),
            CONNECT_START_WITH: start_with_expr,
            CONNECT_PARENT_KEY: parent_key_expr,
            CONNECT_CHILD_KEY: child_key_expr,
            CONNECT_NOCYCLE: ibis.literal(bool(nocycle)),
        }
    )
    relation = ops.View(child=relation, name=CONNECT_CTE_NAME).to_expr()
    return relation.select(*table.columns, level_name)


def compile(
    expr: ir.Expr,
    *,
    params: Mapping[ir.Expr, Any] | None = None,
    optimize: bool = False,
    schema: Mapping[str, Any] | None = None,
) -> sg.Expression:
    expression = DSQLCompiler().to_sqlglot(expr, params=params)

    if optimize and expression.find(sge.Connect) is None:
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
