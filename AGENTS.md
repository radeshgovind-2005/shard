/## Project Overview

SHARD is ComPar 26/27 Project 1 (NOVA FCT): large-scale analytics over NYC TLC Yellow Taxi trips (2009–2026, Parquet) on Apache Spark, written in **Kotlin against Spark's JVM (Scala) API**. That is a deliberate choice: Spark runs on JVM bytecode, so Kotlin uses the Scala API directly.

What's graded is the **performance engineering**, not the analytics: profile → hypothesise → optimise → measure → repeat, written up as a 4-page report (`docs/report/`). Every optimisation needs a measured bottleneck before it and a before/after measurement after it.

Operations: M1 ingestion & cleaning, M2 temporal demand, M3 zone statistics, plus at least 2 of O1–O4 (TODO: which). Deadline: 2026-11-06 23:59. Full spec: `docs/Project_Description.md`.

## Commands

The Makefile is the single entry point; hooks and CI call the same targets.

- `make setup` — install Node tooling and Husky Git hooks (run once after cloning)
- `make gh-setup` — one-time GitHub config: squash-only merges, title-only squash commits, main ruleset (needs CI on main first)
- `make build` — compile without tests
- `make test` — run tests
- `make lint` — check formatting (Spotless + ktlint)
- `make format` — apply formatting
- `make check` — lint + build + test (pre-push and CI)
- `make run` — run the application
- `make clean` — remove build outputs

Commits follow Conventional Commits (enforced by commitlint).

## Architecture Map

TODO, once the pipeline exists: packages, entry point, and the data flow (load → M1 → M2 / M3 / O*), with the files that implement each operation.

## Code Conventions

- **Style:** Kotlin official style, enforced by Spotless + ktlint (`intellij_idea`, 120 cols). Run `make format`; never hand-format.
- **Structure:** follow the `kotlin-stepdown` skill (`.claude/skills/kotlin-stepdown/`). Entry point first, helpers below in call order, one level of abstraction per function, one concept per file, packages by feature (`com.kognito.ingest`, `.demand`, `.zones`, `.cli`).
- **Spark** (details in `references/spark.md` of that skill):
  - Steps are extension functions on `Dataset<Row>` that return `Dataset<Row>` and **never run actions**. Hidden `count()`/`show()` calls add Spark jobs and distort profiling.
  - Enforce the schema once at load; keep column names as constants; prefer built-in `functions.*` over UDFs.
  - Keep performance decisions (`persist`/`unpersist`, `broadcast()`, repartitioning) visible in the top-level pipeline function.
- **Kotlin ↔ Scala API:** use `Column` methods (`.gt()`, `.and()`, `.equalTo()`), since Scala operators aren't available. Avoid `Dataset.transform` (it needs a Scala `Function1`). Convert collections explicitly when an API wants a Scala `Seq`.
- **Inputs:** data paths and dataset scale come from CLI arguments, never hard-coded. Any non-default `spark.*` setting is documented in README → Configuration.

## Testing and Verification

- `make check` = lint + build + test. CI and the `pre-push` hook run the same thing.
- TODO, once the pipeline exists: unit tests per operation, and a correctness check on a one-month sample (row counts plus an `except`-diff in both directions). Every optimisation must keep results identical.

## Workflow

Once `make gh-setup` has run, `main` is protected: every change goes through a PR, is squash-merged, and lands as one title-only Conventional Commit.

1. **Branch** — `<type>/<kebab-description>` from an up-to-date main, e.g. `feat/m2-temporal-demand`, `perf/broadcast-zone-lookup`. The `pre-push` hook rejects other names.
2. **Commit** — one logical change per commit, title only: `type(scope): subject`, ≤ 72 chars, imperative, lowercase. Types: `feat fix perf refactor test docs build ci chore style revert`. Scopes: `m1`–`m3`, `o1`–`o4`, `ingest`, `spark`, `report`, `ci`, `tooling`.
3. **PR** — the PR title follows the same convention, because it becomes the commit on main (CI lints it). Fill in the template; `perf` PRs cite H/OPT ids and run-ids from `experiments/LOG.md`.
4. **Merge** — squash only, after `build` and `commitlint` pass. The branch is deleted automatically.

Never commit dataset files (`*.parquet`, `data/`), and never force-push or bypass hooks.

## Boundaries

- **Never commit** dataset files (`*.parquet`, `data/`), Spark event logs, or secrets (`.env*`).
- **No speculative optimisation.** Only change performance-related code for a hypothesis logged in `experiments/LOG.md` with a measured symptom, as the assignment requires.
- **Never change what an operation computes** as a "performance fix". Verify that results are identical.
- **Every number** in the report or a PR traces to a run-id in `experiments/LOG.md`. Never estimate or round in your favour.
- **Read-only:** `docs/lectures/` (reference material), `docs/report/acmart.cls`, `docs/report/acm.bst`.
- **Git:** never force-push, use `--no-verify`, or merge with `--admin`.

## Further docs

- `docs/Project_Description.md`: assignment spec, report structure, grading
- `docs/lectures/index.md`: lecture knowledge base (start here; it has a keyword lookup). For numbers and formulas: `docs/lectures/formulas-cheatsheet.md`
- `docs/report/template.tex`: report template (acmart). Build with `latexmk -pdf -cd docs/report/template.tex`
- `experiments/LOG.md`: environments, runs, hypotheses, optimisations
- `.claude/skills/kotlin-stepdown/`: code style, plus `references/spark.md` for Spark specifics
