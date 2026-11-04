# CLAUDE.md

@AGENTS.md

## Claude-Specific Notes

- **Don't commit, push or merge unless the user asks.** They drive Git through `/commit`, `/pr`, `/merge` and `/update-repo`.
- A PostToolUse hook runs `npm run format` (Gradle `spotlessApply`) after every Edit/Write, so expect Kotlin files to be reformatted after each edit.
- Performance work goes through the agents below, and through `experiments/LOG.md`. Don't optimise inline without a logged hypothesis.

## Where Things Live

| Path | What |
|---|---|
| `src/main/kotlin/` | Pipeline code (`com.kognito`) |
| `src/test/kotlin/` | Tests (`make test`) |
| `data/` | Local dataset only: gitignored, blocked by `pre-commit` |
| `experiments/LOG.md` | Environments, runs, hypotheses, optimisations: the source of every reported number |
| `experiments/runs/<run-id>/` | Raw stage JSON and plans (event logs gitignored) |
| `experiments/analysis/` | CSVs for report charts |
| `docs/Project_Description.md` | Assignment spec |
| `docs/lectures/` | Lecture knowledge base (`index.md` first) |
| `docs/report/` | LaTeX report (`template.tex`) |
| `.claude/agents/`, `commands/`, `skills/` | Agents, slash commands, the `kotlin-stepdown` skill |
| `.claude/templates/` | Templates for new agents, commands and rules (not loaded) |
| `.github/` | CI workflow, `main` ruleset, PR template |
| `.husky/`, `Makefile`, `scripts/` | Git hooks, command entry point, `gh-setup.sh` |

## Subagents & Skills

Git workflow (see AGENTS.md → Workflow): `/new-branch` → `/commit` → `/pr` → `/review` → `/merge`

- `/new-branch <type> <description>`: create `type/kebab-description` from a freshly pulled main
- `/commit [hint]`: group changes into logical commits with title-only Conventional Commit messages (never pushes, refuses on main)
- `/pr [title]`: push + open or update the PR with a validated title and the filled-in template
- `/merge [#]`: squash-merge once checks pass (asks first), delete the branch, sync main
- `/update-repo [main]`: detects the state and runs only the steps needed (branch, commit, sync with main, push + PR); with `main` it continues through the merge
- `/review [base]`: read-only branch review (correctness, Spark performance, assignment spec, `make check`)
- `kotlin-stepdown` skill: top-down, one-abstraction-level-per-function style for Kotlin/Spark code (preloaded in `spark-optimizer`)

Performance loop (profile → hypothesise → optimise → measure → repeat), with handoff through `experiments/LOG.md`:

1. `spark-profiler`: baseline + evidence-backed hypotheses (read-only on code)
2. `spark-optimizer`: one change per hypothesis, verifies identical results; refuses speculative changes
3. `benchmark-runner`: controlled before/after runs (warm-up + n ≥ 3, median), verdict per optimisation
4. `scalability-analyst`: dataset-scale and cluster-size analysis (speedup relative to 2 nodes, Amdahl/Gustafson)
5. `report-writer`: LaTeX report from the log; every number must trace to a run-id

## Gotchas

- **Agent, command and skill files must start with `---` on line 1.** Otherwise Claude Code silently treats them as documentation and they vanish (`claude plugin validate` doesn't catch it).
- **The PR title becomes the commit on `main`** (squash-merge, empty body), so it must pass commitlint. CI lints it.
- **Apply the ruleset only after CI is on `main`.** `make gh-setup` refuses otherwise; without CI, every PR would wait forever on required checks.
- **AQE rewrites plans at runtime.** The `explain()` output before execution says `isFinalPlan=false`, so diagnose from the final plan (after the action, or from the Spark UI's SQL tab).
- **Java vs Spark:** the toolchain is JDK 24, but Spark 4 officially supports 17/21. If Spark fails at runtime with module-access errors (`IllegalAccessError`, `InaccessibleObjectException`), add the needed `--add-opens` JVM args or drop the toolchain to 21.
- **Kotlin ↔ Scala:** no Scala operator syntax on `Column`, and no implicit `Function1`/`Seq` conversion. See `.claude/skills/kotlin-stepdown/references/spark.md`.
