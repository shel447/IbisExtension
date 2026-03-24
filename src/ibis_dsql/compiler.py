from __future__ import annotations

import ibis.expr.operations as ops
import ibis.common.exceptions as com
from ibis.backends.sql.compilers.postgres import PostgresCompiler
import sqlglot as sg
import sqlglot.expressions as sge

from ibis_dsql.dialect import DSQLDialect
from ibis_dsql.rewrites import DSQL_POST_REWRITES, DSQL_REWRITES

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
                    raise com.UnsupportedOperationError(
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
        raise com.UnsupportedOperationError(
            "DSQL supports at most one connect_by relation per query"
        )
    return matches[0]


def _extract_connect_parts(
    cte: sge.CTE,
) -> tuple[sge.Expression, str | None, str, dict[str, sge.Expression]]:
    query = cte.this
    if not isinstance(query, sge.Select):
        raise com.UnsupportedOperationError("DSQL connect_by lowering expected a SELECT CTE")

    source = query.args.get("from_")
    if source is None or source.this is None:
        raise com.UnsupportedOperationError("DSQL connect_by lowering expected a FROM source")

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
        raise com.UnsupportedOperationError(
            f"DSQL connect_by lowering missing metadata columns: {sorted(missing)}"
        )
    if level_name is None:
        raise com.UnsupportedOperationError("DSQL connect_by lowering could not determine level column")

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


class DSQLCompiler(PostgresCompiler):
    __slots__ = ()

    dialect = DSQLDialect
    rewrites = (*DSQL_REWRITES, *PostgresCompiler.rewrites)
    post_rewrites = (*DSQL_POST_REWRITES, *PostgresCompiler.post_rewrites)

    def visit_StartsWith(self, op, *, arg, start):
        if not isinstance(start, sge.Literal) or not start.is_string:
            raise com.UnsupportedOperationError(
                "DSQL does not support dynamic startswith patterns"
            )

        return sge.Like(this=arg, expression=sge.Literal.string(f"{start.this}%"))

    def visit_EndsWith(self, op, *, arg, end):
        if not isinstance(end, sge.Literal) or not end.is_string:
            raise com.UnsupportedOperationError(
                "DSQL does not support dynamic endswith patterns"
            )

        return sge.Like(this=arg, expression=sge.Literal.string(f"%{end.this}"))

    def visit_ScalarSubquery(self, op, *, rel):
        raise com.UnsupportedOperationError(
            "DSQL does not support scalar subqueries"
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
