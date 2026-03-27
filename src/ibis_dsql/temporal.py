from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import ibis.expr.operations as ops
import sqlglot as sg
import sqlglot.expressions as sge

from ibis_dsql.exceptions import UnsupportedSyntaxException

TIMEZONE_AWARE_EPOCH_MILLIS_ERROR = (
    "DSQL does not support timezone-aware timestamp fields in epoch-millis comparisons"
)
EPOCH_MILLIS_RAW_META_KEY = "ibis_dsql_epoch_millis_raw"


def is_epoch_millis_timestamp_cast(op: ops.Node) -> bool:
    return (
        isinstance(op, ops.Cast)
        and op.arg.dtype.is_integer()
        and op.to.is_timestamp()
    )


def epoch_millis_source_op(
    op: ops.Node, *, _seen: set[ops.Node] | None = None
) -> ops.Node | None:
    if is_epoch_millis_timestamp_cast(op):
        return op.arg

    if not isinstance(op, ops.Field):
        return None

    if _seen is None:
        _seen = set()
    if op in _seen:
        return None
    _seen.add(op)

    values = getattr(op.rel, "values", None)
    if values is None or op.name not in values:
        return None

    value = values[op.name]
    if value is op:
        return None

    return epoch_millis_source_op(value, _seen=_seen)


def is_temporal_dtype(dtype) -> bool:
    return dtype.is_timestamp() or dtype.is_date()


def timestamp_timezone(dtype) -> str | None:
    if not dtype.is_timestamp():
        return None
    return getattr(dtype, "timezone", None)


def is_timezone_aware_timestamp_dtype(dtype) -> bool:
    return dtype.is_timestamp() and timestamp_timezone(dtype) is not None


def is_sql_int_literal(expression: sge.Expression, value: int) -> bool:
    return (
        isinstance(expression, sge.Literal)
        and not expression.is_string
        and str(expression.this) == str(value)
    )


def is_anonymous_function(expression: sge.Expression, name: str) -> bool:
    return (
        isinstance(expression, sge.Anonymous)
        and expression.name.upper() == name.upper()
    )


@dataclass(slots=True)
class EpochMillisTemporalPolicy:
    compiler: Any

    def source_op(self, op: ops.Node) -> ops.Node | None:
        return epoch_millis_source_op(op)

    def build_timestamp(self, raw: sge.Expression, dtype) -> sge.Expression:
        seconds = self.compiler.binop(sge.Div, raw.copy(), sge.convert(1000))
        expression = self.compiler.cast(sg.func("FROM_UNIXTIME", seconds), dtype)
        expression.meta[EPOCH_MILLIS_RAW_META_KEY] = raw.copy()
        return expression

    def unwrap_timestamp(self, expression: sge.Expression) -> sge.Expression | None:
        raw = expression.meta.get(EPOCH_MILLIS_RAW_META_KEY)
        if isinstance(raw, sge.Expression):
            return raw.copy()

        if not isinstance(expression, sge.Cast):
            return None

        to = expression.to
        if not isinstance(to, sge.DataType) or to.this != sge.DataType.Type.TIMESTAMP:
            return None

        inner = expression.this
        if not is_anonymous_function(inner, "FROM_UNIXTIME") or len(inner.expressions) != 1:
            return None

        division = inner.expressions[0]
        if not isinstance(division, sge.Div) or not is_sql_int_literal(division.expression, 1000):
            return None

        return division.this.copy()

    def timestamp_to_epoch_millis(self, expression: sge.Expression) -> sge.Expression:
        raw = self.unwrap_timestamp(expression)
        if raw is not None:
            return raw

        seconds = sg.func("UNIX_TIMESTAMP", expression.copy())
        return self.compiler.binop(sge.Mul, seconds, sge.convert(1000))

    def operand_to_epoch_millis(
        self, operand: ops.Node, expression: sge.Expression
    ) -> sge.Expression:
        if self.source_op(operand) is not None:
            raw = self.unwrap_timestamp(expression)
            return raw if raw is not None else expression.copy()

        return self.timestamp_to_epoch_millis(expression)

    def restore_timestamp(
        self, operand: ops.Node, expression: sge.Expression
    ) -> sge.Expression:
        if self.source_op(operand) is None:
            return expression

        if self.unwrap_timestamp(expression) is not None:
            return expression

        return self.build_timestamp(expression.copy(), operand.dtype)

    def rewrite_projection(self, expression: sge.Expression) -> sge.Expression:
        if isinstance(expression, sge.Alias):
            raw = self.unwrap_timestamp(expression.this)
            if raw is None:
                return expression

            rewritten = expression.copy()
            rewritten.set("this", raw)
            return rewritten

        raw = self.unwrap_timestamp(expression)
        return raw if raw is not None else expression

    def ensure_supported_temporal_operands(self, *operands: ops.Node) -> None:
        if any(is_timezone_aware_timestamp_dtype(operand.dtype) for operand in operands):
            raise UnsupportedSyntaxException(TIMEZONE_AWARE_EPOCH_MILLIS_ERROR)

    def should_rewrite_temporal_comparison(
        self, left_op: ops.Node, right_op: ops.Node
    ) -> bool:
        if not (
            (self.source_op(left_op) is not None or self.source_op(right_op) is not None)
            and is_temporal_dtype(left_op.dtype)
            and is_temporal_dtype(right_op.dtype)
        ):
            return False

        self.ensure_supported_temporal_operands(left_op, right_op)
        return True
