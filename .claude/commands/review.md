---
description: Review the current branch against a base ref for correctness, Spark performance pitfalls and assignment requirements. Read-only. Use before opening or merging a PR.
argument-hint: [base ref, default main]
allowed-tools: Bash(git status *), Bash(git diff *), Bash(git log *), Bash(git merge-base *), Bash(make check), Bash(npx --no -- commitlint *), Read, Grep, Glob
disable-model-invocation: true
---

# Review

## Context

- Branch: !`git branch --show-current`
- Status: !`git status --short`
- Commits not on main: !`git log --oneline main..HEAD`

## Task

Review every change between the base ref and the working tree. Base: `$ARGUMENTS` (if empty, use `main`).
This is **read-only**: report findings and don't edit files.

## Steps

1. Get the diff: `git diff <base>...HEAD`, plus `git diff HEAD` for uncommitted work. Read the full changed files, not just the hunks.
2. Run `make check` and record the result.
3. Run `npx --no -- commitlint --from <base> --to HEAD` and record any failures.
4. Go through the checklist below. Every finding needs a concrete failure scenario. Drop anything you can't back up with one.

## Checklist

**Correctness**
- M1 filters match the spec exactly: `fare_amount ≤ 0`, `trip_distance ≤ 0`, `passenger_count = 0 or > 6`, dropoff ≤ pickup, location IDs outside 1–263. Removal counts are per criterion and per year
- Null handling is explicit and the schema is enforced, not inferred
- Zone lookup is joined twice (PU and DO) where required. Join keys and types match
- Window specs (M3): correct partition/order, frame for the rolling 3-month share, 1.5σ deviation flag

**Spark performance**
- No `collect()` / `toLocalIterator()` on large data; no actions inside loops
- The zone lookup (265 rows) is broadcast
- DataFrames that are reused get cached, and the cache is released; nothing is cached if it's used only once
- UDFs only where no built-in function exists
- Unnecessary `repartition` / `coalesce`; partitioning on output writes (M2 is partitioned by borough)
- Skew-prone keys, shuffle-heavy wide transformations

**Reproducibility**
- No hard-coded data paths; input path and scale come from CLI args
- Any non-default Spark config is documented in README

**Repo hygiene**
- No dataset files (`*.parquet`, `data/`) and no secrets
- Kotlin: idiomatic null safety (no unjustified `!!`); new logic has tests

## Output

1. `make check` result and commitlint result (one line each)
2. Findings ranked by severity (**blocker / major / minor**), each with `file:line`, what goes wrong, and a suggested fix
3. Verdict: **ready to merge** or **needs changes**

## Success Criteria

- [ ] Every changed file was read
- [ ] Every finding has `file:line` and a concrete scenario
- [ ] No files were modified
