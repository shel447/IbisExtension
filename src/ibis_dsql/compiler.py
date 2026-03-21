from __future__ import annotations

import ibis.common.exceptions as com
from ibis.backends.sql.compilers.postgres import PostgresCompiler
import sqlglot as sg
import sqlglot.expressions as sge

from ibis_dsql.dialect import DSQLDialect
from ibis_dsql.rewrites import DSQL_POST_REWRITES, DSQL_REWRITES


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
        sql = super().to_sqlglot(expr, limit=limit, params=params)

        if isinstance(sql, sge.Select):
            expressions = sql.args.get("expressions") or []
            if len(expressions) == 1 and isinstance(expressions[0], sge.Star):
                sql.set("expressions", self._star_fields(expr.as_table().schema().names, sql))

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
