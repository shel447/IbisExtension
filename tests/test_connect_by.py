from __future__ import annotations

import ibis
import ibis.common.exceptions as com
import pytest
import sqlglot.expressions as sge

from ibis_dsql import compile as compile_expr
from ibis_dsql import connect_by, to_sql, to_sqlglot
from ibis_dsql.dialect import DSQLDialect


def test_to_sql_lowers_basic_connect_by_query():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")

    expr = connect_by(
        tree,
        start_with=tree.id == 1,
        parent_key=tree.id,
        child_key=tree.parent_id,
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id, t0.parent_id, LEVEL AS level FROM tree AS t0 START WITH t0.id = 1 CONNECT BY PRIOR t0.id = t0.parent_id"
    )


def test_to_sql_lowers_connect_by_with_level_filter_and_nocycle():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")

    expr = (
        connect_by(
            tree,
            start_with=tree.id == 1,
            parent_key=tree.id,
            child_key=tree.parent_id,
            nocycle=True,
        )
        .filter(lambda t: t.level > 1)
        .order_by(lambda t: t.level)
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id, t0.parent_id, LEVEL AS level FROM tree AS t0 WHERE LEVEL > 1 START WITH t0.id = 1 CONNECT BY NOCYCLE PRIOR t0.id = t0.parent_id ORDER BY LEVEL ASC"
    )


def test_to_sql_lowers_connect_by_start_with_in_subquery():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    roots = ibis.table([("id", "int64")], name="roots")

    expr = connect_by(
        tree,
        start_with=tree.id.isin(roots.id),
        parent_key=tree.id,
        child_key=tree.parent_id,
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id, t0.parent_id, LEVEL AS level FROM tree AS t0 START WITH t0.id IN (SELECT t1.id FROM roots AS t1) CONNECT BY PRIOR t0.id = t0.parent_id"
    )


def test_to_sql_lowers_connect_by_start_with_exists_subquery():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    roots = ibis.table([("id", "int64")], name="roots")

    expr = connect_by(
        tree,
        start_with=(tree.id == roots.id).any(),
        parent_key=tree.id,
        child_key=tree.parent_id,
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id, t0.parent_id, LEVEL AS level FROM tree AS t0 START WITH EXISTS(SELECT 1 FROM roots AS t1 WHERE t0.id = t1.id) CONNECT BY PRIOR t0.id = t0.parent_id"
    )


def test_to_sql_lowers_connect_by_on_derived_input():
    tree = ibis.table(
        [("id", "int64"), ("parent_id", "int64"), ("active", "boolean")], name="tree"
    )
    base = tree.filter(tree.active).select("id", "parent_id")

    expr = connect_by(
        base,
        start_with=base.id == 1,
        parent_key=base.id,
        child_key=base.parent_id,
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id, t0.parent_id, LEVEL AS level FROM (SELECT t0.id, t0.parent_id FROM tree AS t0 WHERE t0.active) AS t0 START WITH t0.id = 1 CONNECT BY PRIOR t0.id = t0.parent_id"
    )


def test_to_sqlglot_lowers_connect_protocol_to_connect_ast():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")

    expr = connect_by(
        tree,
        start_with=tree.id == 1,
        parent_key=tree.id,
        child_key=tree.parent_id,
    ).filter(lambda t: t.level > 1)

    compiled = to_sqlglot(expr)

    assert isinstance(compiled, sge.Select)
    assert "__connect" not in compiled.sql(dialect="postgres")
    assert isinstance(compiled.args.get("connect"), sge.Connect)
    assert compiled.args["connect"].sql(dialect="postgres") == (
        "START WITH t0.id = 1 CONNECT BY PRIOR t0.id = t0.parent_id"
    )
    assert compiled.args["where"].sql(dialect="postgres") == "WHERE LEVEL > 1"


def test_connect_by_rejects_non_boolean_start_with():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")

    with pytest.raises(com.IbisTypeError, match="start_with must be a boolean expression"):
        connect_by(
            tree,
            start_with=tree.id,
            parent_key=tree.id,
            child_key=tree.parent_id,
        )


def test_connect_by_rejects_foreign_key_expressions():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    other = ibis.table([("id", "int64")], name="other")

    with pytest.raises(
        com.IbisInputError,
        match="parent_key must reference only the input table",
    ):
        connect_by(
            tree,
            start_with=tree.id == 1,
            parent_key=other.id,
            child_key=tree.parent_id,
        )


def test_connect_by_rejects_level_name_conflict():
    tree = ibis.table(
        [("id", "int64"), ("parent_id", "int64"), ("level", "int64")], name="tree"
    )

    with pytest.raises(com.IbisInputError, match="level_name conflicts with an existing column"):
        connect_by(
            tree,
            start_with=tree.id == 1,
            parent_key=tree.id,
            child_key=tree.parent_id,
        )


def test_to_sql_allows_scalar_subquery_on_connect_start_with_comparison_rhs():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    scalar = tree.aggregate(mx=tree.id.max()).mx.as_scalar()
    expr = connect_by(
        tree,
        start_with=tree.id == scalar,
        parent_key=tree.id,
        child_key=tree.parent_id,
    )

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t0.id, t0.parent_id, LEVEL AS level FROM tree AS t0 START WITH t0.id = (SELECT t1.mx FROM (SELECT MAX(t0.id) AS mx FROM tree AS t0) AS t1) CONNECT BY PRIOR t0.id = t0.parent_id"
    )


def test_compile_optimize_keeps_lowered_connect_by_query_stable():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    expr = connect_by(
        tree,
        start_with=tree.id == 1,
        parent_key=tree.id,
        child_key=tree.parent_id,
    ).filter(lambda t: t.level > 1)

    compiled = compile_expr(
        expr,
        optimize=True,
        schema={"tree": {"id": "BIGINT", "parent_id": "BIGINT"}},
    )

    assert compiled.sql(dialect=DSQLDialect) == (
        "SELECT t0.id, t0.parent_id, LEVEL AS level FROM tree AS t0 WHERE LEVEL > 1 START WITH t0.id = 1 CONNECT BY PRIOR t0.id = t0.parent_id"
    )


def test_to_sql_lowers_connect_by_before_joining_to_another_table():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    labels = ibis.table([("id", "int64"), ("name", "string")], name="labels")

    hierarchy = connect_by(
        tree,
        start_with=tree.id == 1,
        parent_key=tree.id,
        child_key=tree.parent_id,
    )
    expr = hierarchy.join(labels, [hierarchy.id == labels.id]).select(
        hierarchy.id, hierarchy.level, labels.name
    )

    sql = to_sql(expr)

    assert "__connect" not in sql
    assert (
        sql
        == "SELECT t6.id, t6.level, t2.name FROM (SELECT t1.id, t1.parent_id, LEVEL AS level FROM tree AS t1 START WITH t1.id = 1 CONNECT BY PRIOR t1.id = t1.parent_id) AS t6 INNER JOIN labels AS t2 ON t6.id = t2.id"
    )


def test_to_sql_lowers_connect_by_before_outer_aggregate():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")

    expr = (
        connect_by(
            tree,
            start_with=tree.id == 1,
            parent_key=tree.id,
            child_key=tree.parent_id,
        )
        .group_by("level")
        .aggregate(cnt=lambda t: t.id.count())
        .order_by("level")
    )

    sql = to_sql(expr)

    assert "__connect" not in sql
    assert (
        sql
        == "SELECT t4.level, t4.cnt FROM (SELECT t3.level, COUNT(t3.id) AS cnt FROM (SELECT t0.id, t0.parent_id, LEVEL AS level FROM tree AS t0 START WITH t0.id = 1 CONNECT BY PRIOR t0.id = t0.parent_id) AS t3 GROUP BY 1) AS t4 ORDER BY t4.level ASC"
    )


def test_to_sql_lowers_connect_by_when_used_in_outer_in_subquery():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    items = ibis.table([("node_id", "int64"), ("amount", "int64")], name="items")

    hierarchy = connect_by(
        tree,
        start_with=tree.id == 1,
        parent_key=tree.id,
        child_key=tree.parent_id,
    )
    expr = items.filter(items.node_id.isin(hierarchy.id)).select("node_id", "amount")

    sql = to_sql(expr)

    assert "__connect" not in sql
    assert (
        sql
        == "SELECT t5.node_id, t5.amount FROM (SELECT t0.node_id, t0.amount FROM items AS t0 WHERE t0.node_id IN (SELECT t1.id FROM tree AS t1 START WITH t1.id = 1 CONNECT BY PRIOR t1.id = t1.parent_id)) AS t5"
    )
