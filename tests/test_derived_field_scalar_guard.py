from __future__ import annotations

import ibis
import ibis.common.exceptions as com
import pytest

from ibis_dsql import connect_by, to_sql


def test_guard_allows_plain_filter_equality():
    users = ibis.table([("name", "string")], name="users")

    sql = to_sql(users.filter(users.name == "abc"))

    assert sql == "SELECT t0.name FROM users AS t0 WHERE t0.name = 'abc'"


def test_guard_allows_plain_filter_isnull():
    users = ibis.table([("name", "string")], name="users")

    sql = to_sql(users.filter(users.name.isnull()))

    assert sql == "SELECT t0.name FROM users AS t0 WHERE t0.name IS NULL"


def test_guard_allows_filter_with_in_subquery():
    users = ibis.table([("id", "int64")], name="users")
    rel = users.filter(users.id > 1)

    sql = to_sql(users.filter(users.id.isin(rel.id)))

    assert (
        sql
        == "SELECT t0.id FROM users AS t0 WHERE t0.id IN (SELECT t0.id FROM users AS t0 WHERE t0.id > 1)"
    )


def test_guard_allows_filter_with_in_subquery_over_aliased_derived_chain():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    mid = devs.select(fid=devs.sid)

    sql = to_sql(table_b.filter(table_b.id.isin(mid.fid)))

    assert (
        sql
        == "SELECT t0.id FROM TableB AS t0 WHERE t0.id IN (SELECT t1.sid AS fid FROM TableA AS t1 WHERE t1.x = 'abc')"
    )


def test_guard_allows_filter_with_exists_subquery():
    left = ibis.table([("id", "int64")], name="left_t")
    right = ibis.table([("id", "int64")], name="right_t")

    sql = to_sql(left.filter((left.id == right.id).any()))

    assert (
        sql
        == "SELECT t0.id FROM left_t AS t0 WHERE EXISTS(SELECT 1 FROM right_t AS t1 WHERE t0.id = t1.id)"
    )


def test_guard_allows_join_against_derived_table_in_scope():
    users = ibis.table([("id", "int64"), ("name", "string")], name="users")
    right = users.filter(users.id > 1).view()
    expr = users.join(right, users.id == right.id).select(users.name)

    sql = to_sql(expr)

    assert (
        sql
        == "SELECT t1.name FROM users AS t1 INNER JOIN (SELECT t0.id, t0.name FROM users AS t0 WHERE t0.id > 1) AS t3 ON t1.id = t3.id"
    )


def test_guard_allows_cte_reuse_join():
    users = ibis.table([("id", "int64"), ("score", "int64")], name="users")
    base = users.filter(users.score > 5)
    right = base.view()
    expr = base.join(right, base.id == right.id).select(
        base.id, left_score=base.score, right_score=right.score
    )

    sql = to_sql(expr)

    assert (
        sql
        == "WITH t1 AS (SELECT t0.id, t0.score FROM users AS t0 WHERE t0.score > 5) SELECT t3.id, t3.score AS left_score, t4.score AS right_score FROM t1 AS t3 INNER JOIN t1 AS t4 ON t3.id = t4.id"
    )


def test_guard_allows_connect_by_outer_join():
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


def test_guard_rejects_same_table_derived_field_scalar_leak():
    table = ibis.table(
        [("x", "string"), ("id", "int64"), ("sid", "int64")],
        name="TableA",
    )
    devs = table.filter(table.x == "abc")
    expr = table.filter(table.id == devs.sid)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_cross_table_derived_field_scalar_leak():
    table_a = ibis.table(
        [("x", "string"), ("id", "int64"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    expr = table_b.filter(table_b.id == devs.sid)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_flipped_comparison_with_derived_field():
    table_a = ibis.table(
        [("x", "string"), ("id", "int64"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    expr = table_b.filter(devs.sid == table_b.id)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_derived_field_inside_arithmetic_expression():
    table_a = ibis.table(
        [("x", "string"), ("id", "int64"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    expr = table_b.filter((devs.sid + 1) == table_b.id)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_derived_field_inside_function_call():
    table_a = ibis.table(
        [("x", "string"), ("id", "int64"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    expr = table_b.filter(table_b.id == ibis.coalesce(devs.sid, 0))

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_projected_alias_derived_field_scalar_leak():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    mid = devs.select(fid=devs.sid)
    expr = table_b.filter(table_b.id == mid.fid)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_projected_alias_derived_field_scalar_leak_inside_in_subquery():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64"), ("fid", "int64")], name="TableB")
    table_c = ibis.table([("id", "int64")], name="TableC")
    devs = table_a.filter(table_a.x == "abc")
    mid = devs.select(fid=devs.sid)
    kt = table_b.filter(table_b.id == mid.fid)
    expr = table_c.filter(table_c.id.isin(kt.fid))

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_projected_alias_derived_field_scalar_leak_inside_exists_subquery():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64"), ("fid", "int64")], name="TableB")
    table_c = ibis.table([("id", "int64")], name="TableC")
    devs = table_a.filter(table_a.x == "abc")
    mid = devs.select(fid=devs.sid)
    kt = table_b.filter(table_b.id == mid.fid)
    expr = table_c.filter((table_c.id == kt.fid).any())

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_mutated_alias_derived_field_scalar_leak():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    mid = devs.mutate(fid=devs.sid)
    expr = table_b.filter(table_b.id == mid.fid)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_chained_project_alias_derived_field_scalar_leak():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    mid = devs.select(fid=devs.sid).select(fid2=lambda t: t.fid)
    expr = table_b.filter(table_b.id == mid.fid2)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_rejects_view_wrapped_project_alias_derived_field_scalar_leak():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc").view()
    mid = devs.select(fid=devs.sid)
    expr = table_b.filter(table_b.id == mid.fid)

    with pytest.raises(
        com.UnsupportedOperationError,
        match="DSQL does not support using a derived table field as a scalar expression",
    ):
        to_sql(expr)


def test_guard_unary_predicate_from_projected_alias_derived_field_is_rejected_upstream():
    table_a = ibis.table(
        [("x", "string"), ("sid", "int64")],
        name="TableA",
    )
    table_b = ibis.table([("id", "int64")], name="TableB")
    devs = table_a.filter(table_a.x == "abc")
    mid = devs.select(fid=devs.sid)

    with pytest.raises(
        com.IntegrityError,
        match="belong to another relation",
    ):
        table_b.filter(mid.fid.isnull())


def test_guard_connect_by_start_with_foreign_relation_is_rejected_upstream():
    tree = ibis.table([("id", "int64"), ("parent_id", "int64")], name="tree")
    roots = tree.filter(tree.id > 1)

    with pytest.raises(
        com.IbisInputError,
        match="start_with must reference only the input table",
    ):
        connect_by(
            tree,
            start_with=tree.id == roots.id,
            parent_key=tree.id,
            child_key=tree.parent_id,
        )
