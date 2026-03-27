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


def _sql_key(expression: sge.Expression) -> str:
    return expression.sql(dialect=DSQLDialect, identify=False)


def _extract_signatures(expression: sge.Expression) -> set[tuple[str, str]]:
    signatures: set[tuple[str, str]] = set()
    for extract in expression.find_all(sge.Extract):
        signatures.add((_sql_key(extract.this), _sql_key(extract.expression)))
    return signatures


def _is_timestamp_cast(expression: sge.Expression) -> bool:
    return (
        isinstance(expression, sge.Cast)
        and isinstance(expression.to, sge.DataType)
        and expression.to.this in {sge.DataType.Type.TIMESTAMP, sge.DataType.Type.DATETIME}
    )


def _strip_redundant_extract_timestamp_casts(
    expression: sge.Expression,
    *,
    original: sge.Expression,
) -> sge.Expression:
    original_extracts = _extract_signatures(original)

    def rewrite(node: sge.Expression) -> sge.Expression:
        if not isinstance(node, sge.Extract):
            return node

        extract_arg = node.expression
        if not _is_timestamp_cast(extract_arg):
            return node

        part_sql = _sql_key(node.this)
        inner_sql = _sql_key(extract_arg.this)
        cast_sql = _sql_key(extract_arg)
        if (
            (part_sql, inner_sql) in original_extracts
            and (part_sql, cast_sql) not in original_extracts
        ):
            rewritten = node.copy()
            rewritten.set("expression", extract_arg.this.copy())
            return rewritten

        return node

    return expression.transform(rewrite, copy=True)


def _strip_identity_column_aliases(expression: sge.Expression) -> sge.Expression:
    def rewrite(node: sge.Expression) -> sge.Expression:
        if (
            isinstance(node, sge.Alias)
            and isinstance(node.this, sge.Column)
            and node.alias == node.this.name
        ):
            return node.this.copy()

        return node

    return expression.transform(rewrite, copy=True)


def _inline_single_cte_passthrough(expression: sge.Expression) -> sge.Expression:
    if not isinstance(expression, sge.Select):
        return expression

    with_ = expression.args.get("with_")
    if with_ is None or len(with_.expressions) != 1:
        return expression

    if any(
        expression.args.get(key)
        for key in ("where", "group", "having", "qualify", "joins", "limit", "offset")
    ):
        return expression

    root_from = expression.args.get("from_")
    if root_from is None or not isinstance(root_from.this, sge.Table):
        return expression

    source = root_from.this
    cte = with_.expressions[0]
    cte_name = cte.alias_or_name
    if source.name != cte_name:
        return expression

    inner = cte.this
    if not isinstance(inner, sge.Select):
        return expression

    inner_by_name = {}
    for projection in inner.expressions:
        output_name = projection.output_name
        if not output_name or output_name in inner_by_name:
            return expression
        inner_by_name[output_name] = projection.copy()

    source_alias = source.alias_or_name
    new_projections: list[sge.Expression] = []
    for projection in expression.expressions:
        underlying = projection.this if isinstance(projection, sge.Alias) else projection
        if not isinstance(underlying, sge.Column) or underlying.table != source_alias:
            return expression

        replacement = inner_by_name.get(underlying.name)
        if replacement is None:
            return expression

        alias_name = projection.output_name
        rewritten = replacement.copy()
        if alias_name and rewritten.output_name != alias_name:
            rewritten = rewritten.as_(alias_name, quoted=False, copy=False)
        new_projections.append(rewritten)

    def rewrite_order_column(node: sge.Expression) -> sge.Expression:
        if (
            isinstance(node, sge.Column)
            and node.table == source_alias
            and node.name in inner_by_name
        ):
            return sg.column(node.name, quoted=False)
        return node

    rewritten = inner.copy()
    rewritten.set("expressions", new_projections)

    order = expression.args.get("order")
    if order is not None:
        rewritten.set("order", order.transform(rewrite_order_column, copy=True))

    return rewritten


def _restore_between_predicates(expression: sge.Expression) -> sge.Expression:
    def rewrite(node: sge.Expression) -> sge.Expression:
        if not isinstance(node, sge.And):
            return node

        comparisons = (node.this, node.expression)
        lower = upper = target = None
        for comparison in comparisons:
            if not isinstance(comparison, (sge.GTE, sge.LTE)):
                return node

            if target is None:
                target = _sql_key(comparison.this)
            elif target != _sql_key(comparison.this):
                return node

            if isinstance(comparison, sge.GTE):
                lower = comparison.expression.copy()
            else:
                upper = comparison.expression.copy()

        if target is None or lower is None or upper is None:
            return node

        return sge.Between(this=node.this.this.copy(), low=lower, high=upper)

    return expression.transform(rewrite, copy=True)


def _restore_in_semijoins(expression: sge.Expression) -> sge.Expression:
    if not isinstance(expression, sge.Select):
        return expression

    joins = expression.args.get("joins") or []
    if len(joins) != 1:
        return expression

    join = joins[0]
    if join.args.get("side") != "LEFT" or join.args.get("kind") is not None:
        return expression

    root_from = expression.args.get("from_")
    if root_from is None or not isinstance(root_from.this, sge.Table):
        return expression

    main_alias = root_from.this.alias_or_name
    join_table = join.this
    if not isinstance(join_table, sge.Table):
        return expression

    join_alias = join_table.alias_or_name
    where = expression.args.get("where")
    if where is None:
        return expression

    null_check = where.this
    negate = False
    if isinstance(null_check, sge.Not):
        negate = True
        null_check = null_check.this

    if (
        not isinstance(null_check, sge.Is)
        or not isinstance(null_check.this, sge.Column)
        or null_check.this.table != join_alias
        or not isinstance(null_check.expression, sge.Null)
    ):
        return expression

    on = join.args.get("on")
    if not isinstance(on, sge.EQ):
        return expression

    join_column = None
    main_column = None
    for left, right in ((on.this, on.expression), (on.expression, on.this)):
        if (
            isinstance(left, sge.Column)
            and left.table == join_alias
            and isinstance(right, sge.Column)
            and right.table == main_alias
        ):
            join_column = left
            main_column = right
            break

    if join_column is None or main_column is None:
        return expression

    with_ = expression.args.get("with_")
    if with_ is None:
        return expression

    matched_cte = None
    remaining_ctes = []
    for cte in with_.expressions:
        if cte.alias_or_name == join_table.name and matched_cte is None:
            matched_cte = cte
        else:
            remaining_ctes.append(cte)

    if matched_cte is None or not isinstance(matched_cte.this, sge.Select):
        return expression

    subquery = matched_cte.this.copy()
    if len(subquery.expressions) == 1:
        projection = subquery.expressions[0]
        if isinstance(projection, sge.Alias):
            subquery.set("expressions", [projection.this.copy()])

        group = subquery.args.get("group")
        if (
            group is not None
            and len(group.expressions) == 1
            and _sql_key(group.expressions[0]) == _sql_key(subquery.expressions[0])
        ):
            subquery.args.pop("group", None)

    rewritten = expression.copy()
    rewritten.set("joins", [])
    rewritten.set(
        "where",
        sge.Where(
            this=(
                sge.In(
                    this=main_column.copy(),
                    query=sge.Subquery(this=subquery, copy=False),
                )
                if negate
                else sge.Not(
                    this=sge.In(
                        this=main_column.copy(),
                        query=sge.Subquery(this=subquery, copy=False),
                    )
                )
            )
        ),
    )

    if remaining_ctes:
        rewritten.args["with_"].set("expressions", remaining_ctes)
    else:
        rewritten.args.pop("with_", None)

    return rewritten


def _restore_explicit_inner_joins(expression: sge.Expression) -> sge.Expression:
    def rewrite(node: sge.Expression) -> sge.Expression:
        if (
            isinstance(node, sge.Join)
            and node.args.get("side") is None
            and node.args.get("kind") is None
        ):
            rewritten = node.copy()
            rewritten.set("kind", "INNER")
            return rewritten

        return node

    return expression.transform(rewrite, copy=True)


def _strip_self_table_aliases(expression: sge.Expression) -> sge.Expression:
    def rewrite(node: sge.Expression) -> sge.Expression:
        if (
            isinstance(node, sge.Table)
            and node.args.get("alias") is not None
            and node.alias_or_name == node.name
        ):
            rewritten = node.copy()
            rewritten.set("alias", None)
            return rewritten

        return node

    return expression.transform(rewrite, copy=True)


def _normalize_optimizer_aliases(expression: sge.Expression) -> sge.Expression:
    current = expression.copy()

    for _ in range(8):
        changed = False

        def rewrite(node: sge.Expression) -> sge.Expression:
            nonlocal changed

            if not isinstance(node, (sge.Table, sge.Subquery, sge.CTE)):
                return node

            alias = node.args.get("alias")
            if alias is None or isinstance(alias, sge.TableAlias):
                return node

            if isinstance(alias, sge.Identifier):
                alias_identifier = alias.copy()
            elif isinstance(alias, str):
                alias_identifier = sg.to_identifier(alias, quoted=False)
            else:
                return node

            rewritten = node.copy()
            rewritten.set("alias", sge.TableAlias(this=alias_identifier))
            changed = True
            return rewritten

        current = current.transform(rewrite, copy=True)
        if not changed:
            break

    return current


def _optimize_sqlglot(expression: sge.Expression, *, schema: Mapping[str, Any] | None) -> sg.Expression:
    normalized = _normalize_optimizer_aliases(expression)
    optimized = optimize_expression(normalized, schema=schema, dialect=DSQLDialect)
    optimized = _strip_redundant_extract_timestamp_casts(optimized, original=expression)
    optimized = _strip_identity_column_aliases(optimized)
    optimized = _restore_between_predicates(optimized)
    optimized = _restore_in_semijoins(optimized)
    optimized = _restore_explicit_inner_joins(optimized)
    optimized = _strip_self_table_aliases(optimized)
    optimized = _inline_single_cte_passthrough(optimized)
    optimized = _strip_identity_column_aliases(optimized)
    return optimized


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

    if optimize:
        expression = _optimize_sqlglot(expression, schema=schema)

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
    expression = compile(expr, params=params, optimize=False, schema=schema)
    expression = _optimize_sqlglot(expression, schema=schema)
    return expression.sql(
        dialect=DSQLDialect,
        pretty=pretty,
        identify=identify,
        **sql_kwargs,
    )
