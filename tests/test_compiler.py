from __future__ import annotations

import unittest

import ibis
import sqlglot.expressions as sge

from ibis_dsql import compile as compile_expr


class CompilerTest(unittest.TestCase):
    def test_compile_returns_ast_with_expected_cast_shape(self):
        users = ibis.table([("id", "int64")], name="users")
        expr = users.select(users.id.cast("string").name("id_text"))

        compiled = compile_expr(expr)
        cast = compiled.find(sge.Cast)
        column = cast.this

        self.assertIsInstance(cast, sge.Cast)
        self.assertIsInstance(column, sge.Column)
        self.assertEqual(column.this.name, "id")
        self.assertEqual(column.table, "t0")
        self.assertEqual(cast.to.this, sge.DataType.Type.VARCHAR)
