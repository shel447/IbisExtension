from __future__ import annotations

import ibis.common.exceptions as com
from ibis.backends.sql.compilers.postgres import PostgresCompiler
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
