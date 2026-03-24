# Derived Field Scalar Guard Design

## Problem

`ibis-framework` can compile expressions such as `table.id == derived_table.field` into SQL that references a derived-table alias which never enters the current `FROM` / `JOIN` scope. This produces invalid SQL like `t0.id = t2.sid`.

The earlier attempt to detect this at the sqlglot scope layer proved too broad. Legal queries such as plain filters, `IN (subquery)`, `EXISTS`, and DSQL `CONNECT BY` lowering can share similar SQL surface shapes, making SQL-side heuristics fragile.

## Goal

Reject only the genuinely unsupported pattern:

- a field from a derived relation is used directly as a scalar value in the current expression context
- the derived relation is not one of the current relational inputs
- the field is not wrapped in an explicit subquery boundary such as `InSubquery`, `ExistsSubquery`, or `ScalarSubquery`

At the same time, preserve all existing legal behavior:

- ordinary filters and boolean predicates
- joins against derived relations that actually appear in `JOIN`
- `IN` / `EXISTS`
- `view()` / CTE reuse
- `connect_by` queries and their outer uses

## Recommended Approach

Perform validation on the Ibis operation tree before SQL serialization.

The compiler already has the fully lowered Ibis relation tree available through `expr.as_table().op()`. Instead of asking whether the generated SQL looks suspicious, walk the Ibis relation graph and identify scalar expression positions that reference `ops.Field` nodes from foreign derived relations.

This validator should operate on relation-local expression slots:

- filter predicates
- project selections
- aggregate metrics
- sort keys
- join predicates
- connect-by metadata expressions if they are compiled through normal relation nodes

It must skip or stop at explicit subquery boundary nodes:

- `ops.InSubquery`
- `ops.ExistsSubquery`
- `ops.ScalarSubquery`

Those nodes establish valid query boundaries and should not be treated as leaked fields.

## Detection Rule

For each relation node that owns scalar expressions:

1. Determine the set of relation inputs that are valid in that relation’s local scope.
2. Traverse each owned scalar expression.
3. Ignore fields whose `rel` is one of the valid local inputs.
4. Ignore fields under explicit subquery-boundary nodes.
5. If a field’s `rel` is a derived relation outside the local input set, raise `UnsupportedOperationError`.

This rule is intentionally narrow. It focuses on Ibis relation ownership instead of trying to infer legality from SQL aliasing after lowering.

## Test Matrix

Legal cases:

- `field == 'abc'`
- `field.isnull()`
- compound filters using `&` / `|`
- derived-table joins where the derived table is part of the join inputs
- `IN (subquery)`
- `EXISTS (...)`
- reused `view()` / CTE joins
- `connect_by` with `START WITH IN/EXISTS`
- outer `JOIN / GROUP BY / IN` on top of `connect_by`

Illegal cases:

- same-table derived field used directly as scalar: `TableA.id == devs.sid`
- cross-table derived field used directly as scalar: `TableB.id == devs.sid`
- flipped comparison: `devs.sid == TableB.id`
- derived field used in arithmetic
- derived field passed into a function call
- derived field leaked into `connect_by.start_with`

## Files

- `src/ibis_dsql/compiler.py`
- `tests/test_derived_field_scalar_guard.py`
- optionally small touch-ups to existing tests if overlap is unavoidable
