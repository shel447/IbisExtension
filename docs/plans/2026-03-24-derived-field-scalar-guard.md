# Derived Field Scalar Guard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reject only unsupported direct scalar use of foreign derived-table fields without regressing legal filters, joins, subqueries, or connect-by queries.

**Architecture:** Add a broad regression matrix first, then implement a compiler-side validator over the Ibis relation tree. The validator checks only local scalar expression slots and explicitly skips real subquery-boundary nodes, avoiding the false positives that came from SQL-side scope heuristics.

**Tech Stack:** Python, `ibis-framework==11.0.0`, `sqlglot==28.1.0`, `unittest`

---

### Task 1: Add regression matrix

**Files:**
- Create: `tests/test_derived_field_scalar_guard.py`

**Step 1: Write the failing tests**

Add legal-case tests for:

- plain filter equality
- `isnull()`
- derived join where the derived relation is in `JOIN`
- `IN (subquery)`
- `EXISTS`
- connect-by outer usage

Add illegal-case tests for:

- same-table derived scalar leak
- cross-table derived scalar leak
- flipped comparison
- arithmetic/function-call leak
- connect-by start-with leak

**Step 2: Run test to verify it fails**

Run: `E:\code\codex_projects\IbisExtension\.venv\Scripts\python -m unittest tests.test_derived_field_scalar_guard`

Expected: illegal-case tests fail because the compiler still produces bad SQL instead of rejecting them.

### Task 2: Implement Ibis relation-context validation

**Files:**
- Modify: `src/ibis_dsql/compiler.py`
- Test: `tests/test_derived_field_scalar_guard.py`

**Step 1: Write minimal implementation**

- add a validator that walks the Ibis relation tree before SQL serialization
- inspect only scalar expression slots owned by each relation node
- stop descent at `InSubquery`, `ExistsSubquery`, and `ScalarSubquery`
- raise `UnsupportedSyntaxException` when a foreign derived relation field is used directly as a scalar

**Step 2: Run focused tests**

Run: `E:\code\codex_projects\IbisExtension\.venv\Scripts\python -m unittest tests.test_derived_field_scalar_guard`

Expected: PASS

### Task 3: Regression verification and commit

**Files:**
- Modify: `src/ibis_dsql/compiler.py`
- Create: `tests/test_derived_field_scalar_guard.py`
- Create: `docs/plans/2026-03-24-derived-field-scalar-guard-design.md`
- Create: `docs/plans/2026-03-24-derived-field-scalar-guard.md`

**Step 1: Run the full suite**

Run: `E:\code\codex_projects\IbisExtension\.venv\Scripts\python -m unittest discover -s tests -p "test_*.py" -t .`

Expected: PASS

**Step 2: Commit**

```bash
git add src/ibis_dsql/compiler.py tests/test_derived_field_scalar_guard.py docs/plans/2026-03-24-derived-field-scalar-guard-design.md docs/plans/2026-03-24-derived-field-scalar-guard.md
git commit -m "fix: reject leaked derived fields in scalar contexts"
```
