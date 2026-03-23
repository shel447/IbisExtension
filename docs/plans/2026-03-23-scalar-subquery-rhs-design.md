# Scalar Subquery RHS Design

**Problem**

`ibis_dsql` currently rejects every `ScalarSubquery` in `DSQLCompiler.visit_ScalarSubquery`. That blocks the newly requested escape hatch where `derived_table["col"].as_scalar()` appears on the right-hand side of a direct comparison predicate and should be passed through to the execution engine.

**Goal**

Keep the existing DSQL default of rejecting scalar subqueries, but allow exactly one narrow shape:

- the scalar subquery is the right-hand operand of a direct comparison
- the comparison is one of `=`, `!=`, `>`, `>=`, `<`, `<=`

**Design**

The compiler will stop raising immediately in `visit_ScalarSubquery` and instead emit the normal sqlglot scalar-subquery AST. A new post-compile validator in `src/ibis_dsql/compiler.py` will then reject every scalar subquery except the explicitly allowed comparison-RHS form.

This keeps the policy centralized in DSQL validation rather than relying on brittle ibis op-shape checks. The existing external-column validation remains in place, so the original bad `derived_table["col"]` scalar leak is still rejected unless the user explicitly opted into `.as_scalar()`.

**Validation Rules**

- allow `sqlglot.expressions.Subquery` when its parent is one of `EQ`, `NEQ`, `GT`, `GTE`, `LT`, `LTE` and the subquery occupies the `expression` argument
- reject scalar subqueries everywhere else
- continue allowing DSQL `LEVEL` pseudo-column in the separate external-column validator

**Tests**

- add a passing regression for `table.id == derived.refParent.as_scalar()`
- keep rejecting scalar subqueries in select lists
- add a rejection for scalar subqueries on the left-hand side of a comparison
