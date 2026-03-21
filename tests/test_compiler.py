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
