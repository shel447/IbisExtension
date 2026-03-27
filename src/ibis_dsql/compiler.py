from __future__ import annotations

from datetime import date as pydate
from datetime import datetime as pydatetime
from decimal import Decimal
from functools import wraps

import ibis.expr.operations as ops
import ibis.expr.datatypes as dt
import ibis.common.exceptions as com
from ibis.backends.sql.compilers.postgres import PostgresCompiler
import sqlglot as sg
import sqlglot.expressions as sge

from ibis_dsql.dialect import DSQLDialect
from ibis_dsql.exceptions import UnsupportedSyntaxException
from ibis_dsql.rewrites import DSQL_POST_REWRITES, DSQL_REWRITES
from ibis_dsql.temporal import EpochMillisTemporalPolicy
from ibis_dsql.temporal import epoch_millis_source_op
from ibis_dsql.temporal import is_epoch_millis_timestamp_cast
from ibis_dsql.temporal import is_temporal_dtype

CONNECT_CTE_NAME = "__connect"
CONNECT_START_WITH = "__connect_start_with"
CONNECT_PARENT_KEY = "__connect_parent_key"
CONNECT_CHILD_KEY = "__connect_child_key"
CONNECT_NOCYCLE = "__connect_nocycle"
CONNECT_METADATA = {
    CONNECT_START_WITH,
    CONNECT_PARENT_KEY,
    CONNECT_CHILD_KEY,
    CONNECT_NOCYCLE,
}


def _raise_on_leaked_derived_fields(op: ops.Node) -> None:
    seen: set[tuple[ops.Node, frozenset[ops.Relation]]] = set()

    def walk_values(values, visible_relations: frozenset[ops.Relation]) -> None:
        for item in values:
            if isinstance(item, ops.Node):
                walk(item, visible_relations)

    def walk(node: ops.Node, visible_relations: frozenset[ops.Relation]) -> None:
        key = (node, visible_relations)
        if key in seen:
            return
        seen.add(key)

        if isinstance(node, ops.Filter):
            current_relations = visible_relations | frozenset((node.parent,))

            for predicate in node.predicates:
                if any(rel not in current_relations for rel in predicate.relations):
                    raise UnsupportedSyntaxException(
                        "DSQL does not support using a derived table field as a scalar expression"
                    )

            walk(node.parent, visible_relations)
            walk_values(node.predicates, current_relations)
            return

        if isinstance(node, ops.Project):
            current_relations = visible_relations | frozenset((node.parent,))

            walk(node.parent, visible_relations)
            walk_values(node.values.values(), current_relations)
            return

        for argname in node.__argnames__:
            value = getattr(node, argname)

            if isinstance(value, ops.Node):
                walk(value, visible_relations)
            elif isinstance(value, tuple):
                walk_values(value, visible_relations)
            elif hasattr(value, "values"):
                walk_values(value.values(), visible_relations)

    walk(op, frozenset())


def _connect_cte(expression: sge.Select) -> sge.CTE | None:
    with_ = expression.args.get("with_")
    if with_ is None:
        return None

    matches = [cte for cte in with_.expressions if cte.alias_or_name == CONNECT_CTE_NAME]
    if not matches:
        return None
    if len(matches) > 1:
        raise UnsupportedSyntaxException(
            "DSQL supports at most one connect_by relation per query"
        )
    return matches[0]


def _extract_connect_parts(
    cte: sge.CTE,
) -> tuple[sge.Expression, str | None, str, dict[str, sge.Expression]]:
    query = cte.this
    if not isinstance(query, sge.Select):
        raise UnsupportedSyntaxException("DSQL connect_by lowering expected a SELECT CTE")

    source = query.args.get("from_")
    if source is None or source.this is None:
        raise UnsupportedSyntaxException("DSQL connect_by lowering expected a FROM source")

    metadata: dict[str, sge.Expression] = {}
    level_name: str | None = None
    source_expressions: list[sge.Expression] = []

    for expression in query.expressions:
        output_name = expression.output_name
        if output_name in CONNECT_METADATA:
            metadata[output_name] = expression.this if isinstance(expression, sge.Alias) else expression
        elif isinstance(expression, sge.Alias):
            level_name = output_name
        else:
            source_expressions.append(expression.copy())

    missing = CONNECT_METADATA.difference(metadata)
    if missing:
        raise UnsupportedSyntaxException(
            f"DSQL connect_by lowering missing metadata columns: {sorted(missing)}"
        )
    if level_name is None:
        raise UnsupportedSyntaxException("DSQL connect_by lowering could not determine level column")

    source_alias = getattr(source.this, "alias_or_name", None)
    source_query = query.copy()
    source_query.set("expressions", source_expressions)

    simple_projection = (
        source_query.args.get("where") is None
        and source_query.args.get("group") is None
        and source_query.args.get("having") is None
        and source_query.args.get("qualify") is None
        and source_query.args.get("order") is None
        and source_query.args.get("limit") is None
        and source_query.args.get("offset") is None
        and source_query.args.get("joins") is None
        and source_query.args.get("with_") is None
        and all(
            isinstance(expression, sge.Column) and expression.table == source_alias
            for expression in source_expressions
        )
    )

    if simple_projection:
        source_relation = source.this.copy()
    else:
        source_relation = source_query.subquery(alias=source_alias, copy=False)

    return source_relation, getattr(source_relation, "alias_or_name", None), level_name, metadata


def _rewrite_connect_references(
    expression: sge.Expression,
    *,
    outer_alias: str | None,
    source_alias: str | None,
    level_name: str,
) -> sge.Expression:
    def rewrite(node: sge.Expression) -> sge.Expression:
        if not isinstance(node, sge.Column):
            return node

        if node.table != outer_alias:
            return node

        if node.name == level_name:
            return sg.column("LEVEL", quoted=False)

        return sg.column(node.name, table=source_alias or None, quoted=False)

    return expression.transform(rewrite, copy=True)


def _contains_node(node: sge.Expression, ancestor: sge.Expression) -> bool:
    current = node
    while current is not None:
        if current is ancestor:
            return True
        current = current.parent
    return False


def _lower_connect_select(expression: sge.Select, *, connect_cte: sge.CTE) -> sge.Select:
    if _contains_node(expression, connect_cte.this):
        return expression

    outer_from = expression.args.get("from_")
    if outer_from is None or outer_from.this is None:
        return expression

    outer_source = outer_from.this
    if not isinstance(outer_source, sge.Table) or outer_source.name != CONNECT_CTE_NAME:
        return expression

    source_relation, source_alias, level_name, metadata = _extract_connect_parts(connect_cte)
    outer_alias = outer_source.alias_or_name

    rewritten_projections = []
    for projection in expression.expressions:
        output_name = projection.output_name
        rewritten = _rewrite_connect_references(
            projection,
            outer_alias=outer_alias,
            source_alias=source_alias,
            level_name=level_name,
        )
        if output_name == level_name and not isinstance(rewritten, sge.Alias):
            rewritten = rewritten.as_(level_name, quoted=False, copy=False)
        rewritten_projections.append(rewritten)

    expression.set("expressions", rewritten_projections)
    outer_from.set("this", source_relation)

    for key in ("where", "order", "group", "having", "qualify"):
        clause = expression.args.get(key)
        if clause is not None:
            expression.set(
                key,
                _rewrite_connect_references(
                    clause,
                    outer_alias=outer_alias,
                    source_alias=source_alias,
                    level_name=level_name,
                ),
            )

    expression.set(
        "connect",
        sge.Connect(
            start=_rewrite_connect_references(
                metadata[CONNECT_START_WITH],
                outer_alias=source_alias,
                source_alias=source_alias,
                level_name=level_name,
            ),
            connect=sge.EQ(
                this=sge.Prior(
                    this=_rewrite_connect_references(
                        metadata[CONNECT_PARENT_KEY],
                        outer_alias=source_alias,
                        source_alias=source_alias,
                        level_name=level_name,
                    )
                ),
                expression=_rewrite_connect_references(
                    metadata[CONNECT_CHILD_KEY],
                    outer_alias=source_alias,
                    source_alias=source_alias,
                    level_name=level_name,
                ),
            ),
            nocycle=bool(getattr(metadata[CONNECT_NOCYCLE], "this", False)),
        ),
    )

    return expression


def _lower_connect_tree(expression: sge.Select) -> sge.Select:
    connect_cte = _connect_cte(expression)
    if connect_cte is None:
        return expression

    for select in list(expression.find_all(sge.Select)):
        _lower_connect_select(select, connect_cte=connect_cte)

    remaining_refs = [
        table
        for table in expression.find_all(sge.Table)
        if table.name == CONNECT_CTE_NAME and not _contains_node(table, connect_cte)
    ]
    if not remaining_refs:
        with_ = expression.args.get("with_")
        if with_ is not None:
            remaining = [cte for cte in with_.expressions if cte is not connect_cte]
            if remaining:
                with_.set("expressions", remaining)
            else:
                expression.args.pop("with_", None)

    return expression


def _restore_temporal_arg(method):
    @wraps(method)
    def wrapped(self, op, *, arg, **kwargs):
        arg = self.temporal.restore_timestamp(op.arg, arg)
        return method(self, op, arg=arg, **kwargs)

    return wrapped


def _restore_temporal_left(method):
    @wraps(method)
    def wrapped(self, op, *, left, right, **kwargs):
        left = self.temporal.restore_timestamp(op.left, left)
        return method(self, op, left=left, right=right, **kwargs)

    return wrapped


class DSQLCompiler(PostgresCompiler):
    __slots__ = ("temporal",)

    dialect = DSQLDialect
    rewrites = (*DSQL_REWRITES, *PostgresCompiler.rewrites)
    post_rewrites = (*DSQL_POST_REWRITES, *PostgresCompiler.post_rewrites)

    def __init__(self):
        super().__init__()
        self.temporal = EpochMillisTemporalPolicy(self)

    @staticmethod
    def _literal_node_value(node: ops.Node):
        return node.value if isinstance(node, ops.Literal) else None

    @staticmethod
    def _coerce_timestamp_parts(second_value) -> tuple[int, int]:
        second_decimal = Decimal(str(second_value))
        second_int = int(second_decimal)
        micros = int((second_decimal - second_int) * Decimal("1000000"))
        if micros >= 1000000:
            second_int += 1
            micros -= 1000000
        return second_int, micros

    @staticmethod
    def _day_interval() -> sge.Interval:
        return sge.Interval(this=sge.Literal.string("1"), unit=sge.Var(this="DAY"))

    def _monday_week_start(self, arg: sge.Expression) -> sge.Expression:
        day_interval = self._day_interval()
        shifted = self.binop(sge.Sub, arg.copy(), day_interval.copy())
        week_start = self.f.date_trunc("week", shifted)
        return self.binop(sge.Add, week_start, day_interval)

    def _concat_sql(self, *parts: sge.Expression) -> sge.Expression:
        return sg.func("CONCAT", *parts)

    def _stringify_sql(self, expression: sge.Expression) -> sge.Expression:
        return self.cast(expression.copy(), dt.string)

    def _zero_pad_sql_part(self, expression: sge.Expression, width: int = 2) -> sge.Expression:
        return sg.func(
            "LPAD",
            self._stringify_sql(expression),
            sge.convert(width),
            sge.Literal.string("0"),
        )

    def _timestamp_second_sql_part(
        self, operand: ops.Node, expression: sge.Expression
    ) -> sge.Expression:
        if operand.dtype.is_integer():
            return self._zero_pad_sql_part(expression, 2)

        second_text = self._stringify_sql(expression)
        if operand.dtype.is_floating():
            return self.if_(
                self.binop(sge.LT, expression.copy(), sge.convert(10)),
                self._concat_sql(sge.Literal.string("0"), second_text.copy()),
                second_text,
            )

        return second_text

    @staticmethod
    def _format_temporal_literal(value, *, is_timestamp: bool) -> str:
        timespec = "microseconds" if getattr(value, "microsecond", 0) else "seconds"
        if is_timestamp:
            return value.isoformat(sep=" ", timespec=timespec)
        return value.isoformat(timespec=timespec)

    @staticmethod
    def _string_literal(value: str) -> sge.Literal:
        return sge.Literal.string(value)

    def _rewrite_temporal_binop(self, op, sg_cls, left, right):
        if not self.temporal.should_rewrite_temporal_comparison(op.left, op.right):
            return self.binop(sg_cls, left, right)

        return self.binop(
            sg_cls,
            self.temporal.operand_to_epoch_millis(op.left, left),
            self.temporal.operand_to_epoch_millis(op.right, right),
        )

    def visit_StartsWith(self, op, *, arg, start):
        if not isinstance(start, sge.Literal) or not start.is_string:
            raise UnsupportedSyntaxException(
                "DSQL does not support dynamic startswith patterns"
            )

        return sge.Like(this=arg, expression=sge.Literal.string(f"{start.this}%"))

    def visit_EndsWith(self, op, *, arg, end):
        if not isinstance(end, sge.Literal) or not end.is_string:
            raise UnsupportedSyntaxException(
                "DSQL does not support dynamic endswith patterns"
            )

        return sge.Like(this=arg, expression=sge.Literal.string(f"%{end.this}"))

    def visit_ScalarSubquery(self, op, *, rel):
        raise UnsupportedSyntaxException(
            "DSQL does not support scalar subqueries"
        )

    def visit_Cast(self, op, *, arg, to):
        if is_epoch_millis_timestamp_cast(op):
            return self.temporal.build_timestamp(arg, to)

        if op.arg.dtype.is_timestamp() and (to.is_date() or to.is_time()):
            arg = self.temporal.restore_timestamp(op.arg, arg)

        return super().visit_Cast(op, arg=arg, to=to)

    def visit_DefaultLiteral(self, op, *, value, dtype):
        if dtype.is_date():
            return self._string_literal(value.isoformat())
        if dtype.is_timestamp():
            return self._string_literal(
                self._format_temporal_literal(value, is_timestamp=True)
            )
        if dtype.is_time():
            return self.cast(self._format_temporal_literal(value, is_timestamp=False), dtype)
        return super().visit_DefaultLiteral(op, value=value, dtype=dtype)

    def visit_DateFromYMD(self, op, *, year, month, day):
        if all(
            isinstance(node, ops.Literal) for node in (op.year, op.month, op.day)
        ):
            literal_date = pydate(
                int(self._literal_node_value(op.year)),
                int(self._literal_node_value(op.month)),
                int(self._literal_node_value(op.day)),
            )
            return self._string_literal(literal_date.isoformat())

        date_text = self._concat_sql(
            self._stringify_sql(year),
            sge.Literal.string("-"),
            self._zero_pad_sql_part(month, 2),
            sge.Literal.string("-"),
            self._zero_pad_sql_part(day, 2),
        )
        return self.cast(date_text, dt.date)

    def visit_TimestampFromYMDHMS(self, op, *, year, month, day, hours, minutes, seconds):
        if all(
            isinstance(node, ops.Literal)
            for node in (op.year, op.month, op.day, op.hours, op.minutes, op.seconds)
        ):
            second_int, microseconds = self._coerce_timestamp_parts(
                self._literal_node_value(op.seconds)
            )
            literal_timestamp = pydatetime(
                int(self._literal_node_value(op.year)),
                int(self._literal_node_value(op.month)),
                int(self._literal_node_value(op.day)),
                int(self._literal_node_value(op.hours)),
                int(self._literal_node_value(op.minutes)),
                second_int,
                microseconds,
            )
            return self._string_literal(
                self._format_temporal_literal(literal_timestamp, is_timestamp=True),
            )

        timestamp_text = self._concat_sql(
            self._stringify_sql(year),
            sge.Literal.string("-"),
            self._zero_pad_sql_part(month, 2),
            sge.Literal.string("-"),
            self._zero_pad_sql_part(day, 2),
            sge.Literal.string(" "),
            self._zero_pad_sql_part(hours, 2),
            sge.Literal.string(":"),
            self._zero_pad_sql_part(minutes, 2),
            sge.Literal.string(":"),
            self._timestamp_second_sql_part(op.seconds, seconds),
        )
        return self.cast(timestamp_text, dt.timestamp)

    @_restore_temporal_arg
    def visit_Date(self, op, *, arg):
        return self.f.date_trunc("day", arg)

    @_restore_temporal_arg
    def visit_Strftime(self, op, *, arg, format_str):
        return super().visit_Strftime(op, arg=arg, format_str=format_str)

    @_restore_temporal_arg
    def visit_ExtractEpochSeconds(self, op, *, arg):
        return super().visit_ExtractEpochSeconds(op, arg=arg)

    @_restore_temporal_arg
    def visit_ExtractYear(self, op, *, arg):
        return super().visit_ExtractYear(op, arg=arg)

    @_restore_temporal_arg
    def visit_ExtractMonth(self, op, *, arg):
        return super().visit_ExtractMonth(op, arg=arg)

    @_restore_temporal_arg
    def visit_ExtractDay(self, op, *, arg):
        return super().visit_ExtractDay(op, arg=arg)

    @_restore_temporal_arg
    def visit_ExtractHour(self, op, *, arg):
        return super().visit_ExtractHour(op, arg=arg)

    @_restore_temporal_arg
    def visit_ExtractMinute(self, op, *, arg):
        return super().visit_ExtractMinute(op, arg=arg)

    @_restore_temporal_arg
    def visit_ExtractSecond(self, op, *, arg):
        return super().visit_ExtractSecond(op, arg=arg)

    @_restore_temporal_arg
    def visit_TimestampTruncate(self, op, *, arg, unit):
        if unit.short == "W":
            return self._monday_week_start(arg)
        return super().visit_TimestampTruncate(op, arg=arg, unit=unit)

    @_restore_temporal_left
    def visit_TimestampAdd(self, op, *, left, right):
        return super().visit_TimestampAdd(op, left=left, right=right)

    @_restore_temporal_left
    def visit_TimestampSub(self, op, *, left, right):
        return super().visit_TimestampSub(op, left=left, right=right)

    def visit_SortKey(self, op, *, expr, ascending: bool, nulls_first: bool):
        expr = self.temporal.restore_timestamp(op.expr, expr)
        return super().visit_SortKey(
            op, expr=expr, ascending=ascending, nulls_first=nulls_first
        )

    def visit_Equals(self, op, *, left, right):
        return self._rewrite_temporal_binop(op, sge.EQ, left, right)

    def visit_NotEquals(self, op, *, left, right):
        return self._rewrite_temporal_binop(op, sge.NEQ, left, right)

    def visit_Greater(self, op, *, left, right):
        return self._rewrite_temporal_binop(op, sge.GT, left, right)

    def visit_GreaterEqual(self, op, *, left, right):
        return self._rewrite_temporal_binop(op, sge.GTE, left, right)

    def visit_Less(self, op, *, left, right):
        return self._rewrite_temporal_binop(op, sge.LT, left, right)

    def visit_LessEqual(self, op, *, left, right):
        return self._rewrite_temporal_binop(op, sge.LTE, left, right)

    def visit_Between(self, op, *, arg, lower_bound, upper_bound):
        if not (
            epoch_millis_source_op(op.arg) is not None
            and is_temporal_dtype(op.arg.dtype)
            and is_temporal_dtype(op.lower_bound.dtype)
            and is_temporal_dtype(op.upper_bound.dtype)
        ):
            return super().visit_Between(
                op, arg=arg, lower_bound=lower_bound, upper_bound=upper_bound
            )

        self.temporal.ensure_supported_temporal_operands(
            op.arg,
            op.lower_bound,
            op.upper_bound,
        )

        return sge.Between(
            this=self.temporal.operand_to_epoch_millis(op.arg, arg),
            low=self.temporal.operand_to_epoch_millis(op.lower_bound, lower_bound),
            high=self.temporal.operand_to_epoch_millis(op.upper_bound, upper_bound),
        )

    def _star_fields(self, names, relation):
        table = getattr(relation, "alias_or_name", None)

        if not table and isinstance(relation, sge.Select):
            source = relation.args.get("from_")
            source = None if source is None else source.this
            table = getattr(source, "alias_or_name", None)

        return [
            sg.column(name, table=table or None, quoted=self.quoted, copy=False)
            for name in names
        ]

    def to_sqlglot(self, expr, *, limit=None, params=None):
        _raise_on_leaked_derived_fields(expr.as_table().op())

        sql = super().to_sqlglot(expr, limit=limit, params=params)

        if isinstance(sql, sge.Select):
            expressions = sql.args.get("expressions") or []
            if len(expressions) == 1 and isinstance(expressions[0], sge.Star):
                sql.set("expressions", self._star_fields(expr.as_table().schema().names, sql))
            sql = _lower_connect_tree(sql)
            for select in sql.find_all(sge.Select):
                select.set(
                    "expressions",
                    [self.temporal.rewrite_projection(expr) for expr in select.expressions],
                )

        return sql

    def visit_Select(
        self, op, *, parent, selections, predicates, qualified, sort_keys, distinct
    ):
        if not (selections or predicates or qualified or sort_keys or distinct):
            return parent

        result = parent

        if selections:
            if op.is_star_selection():
                fields = self._star_fields(op.schema.names, parent)
            else:
                fields = self._cleanup_names(selections)
            result = sg.select(*fields, copy=False).from_(result, copy=False)

        if predicates:
            result = result.where(*predicates, copy=False)

        if qualified:
            result = result.qualify(*qualified, copy=False)

        if sort_keys:
            result = result.order_by(*sort_keys, copy=False)

        if distinct:
            result = result.distinct()

        return result
