from __future__ import annotations

from ibis.backends.sql.compilers.postgres import PostgresCompiler

from ibis_dsql.dialect import DSQLDialect
from ibis_dsql.rewrites import DSQL_POST_REWRITES, DSQL_REWRITES


class DSQLCompiler(PostgresCompiler):
    __slots__ = ()

    dialect = DSQLDialect
    rewrites = (*DSQL_REWRITES, *PostgresCompiler.rewrites)
    post_rewrites = (*DSQL_POST_REWRITES, *PostgresCompiler.post_rewrites)
