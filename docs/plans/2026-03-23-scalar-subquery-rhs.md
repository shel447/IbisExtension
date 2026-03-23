# Scalar Subquery RHS Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Allow scalar subqueries only when they appear as the right-hand side of direct comparison predicates, while preserving the rest of the DSQL scalar-subquery ban.

**Architecture:** Keep scalar-subquery emission in the compiler, then enforce the DSQL policy in a focused sqlglot AST validator. This avoids widening behavior globally and keeps the allowlist tied to the final SQL structure.

**Tech Stack:** Python, `ibis-framework==11.0.0`, `sqlglot==28.1.0`, `pytest`

---

### Task 1: Add regression tests

**Files:**
- Modify: `tests/test_sql.py`
- Test: `tests/test_sql.py`

**Step 1: Write the failing test**

Add a passing-intent test for `derived.refParent.as_scalar()` on the right side of a comparison, plus a failing-intent test that keeps rejecting a scalar subquery on the left side.

**Step 2: Run test to verify it fails**

Run: `E:\code\codex_projects\IbisExtension\.venv\Scripts\python -m pytest tests/test_sql.py::test_to_sql_allows_scalar_subquery_on_comparison_rhs tests/test_sql.py::test_to_sql_rejects_scalar_subquery_on_comparison_lhs -q`

Expected: at least the new allowed test fails because `visit_ScalarSubquery` still raises.

### Task 2: Implement scalar-subquery allowlist

**Files:**
- Modify: `src/ibis_dsql/compiler.py`
- Test: `tests/test_sql.py`

**Step 1: Write minimal implementation**

- stop raising in `visit_ScalarSubquery`
- add a validator that only allows scalar subqueries under comparison parents on the `expression` side
- call that validator from `to_sqlglot()`

**Step 2: Run focused tests to verify they pass**

Run: `E:\code\codex_projects\IbisExtension\.venv\Scripts\python -m pytest tests/test_sql.py::test_to_sql_allows_scalar_subquery_on_comparison_rhs tests/test_sql.py::test_to_sql_rejects_scalar_subquery_on_comparison_lhs tests/test_sql.py::test_to_sql_rejects_scalar_subquery_in_where_clause tests/test_sql.py::test_to_sql_rejects_scalar_subquery_in_select_list -q`

Expected: PASS

### Task 3: Verify and commit

**Files:**
- Modify: `src/ibis_dsql/compiler.py`
- Modify: `tests/test_sql.py`
- Create: `docs/plans/2026-03-23-scalar-subquery-rhs-design.md`
- Create: `docs/plans/2026-03-23-scalar-subquery-rhs.md`

**Step 1: Run the full suite**

Run: `E:\code\codex_projects\IbisExtension\.venv\Scripts\python -m pytest -q`

Expected: PASS

**Step 2: Commit**

```bash
git add docs/plans/2026-03-23-scalar-subquery-rhs-design.md docs/plans/2026-03-23-scalar-subquery-rhs.md tests/test_sql.py src/ibis_dsql/compiler.py
git commit -m "fix: allow scalar subqueries on comparison rhs"
```
