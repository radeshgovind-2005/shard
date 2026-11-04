---
description: Group the current changes into logical commits with title-only Conventional Commit messages, lint, and commit each. Use when the user wants to commit their work.
argument-hint: [optional hint, e.g. "m2 temporal demand"]
allowed-tools: Bash(git status *), Bash(git diff *), Bash(git log *), Bash(git add *), Bash(git restore --staged *), Bash(git commit *), Bash(git branch *), Bash(make lint), Bash(make format), Bash(npx --no -- commitlint *)
disable-model-invocation: true
---

# Commit

## Context

- Branch: !`git branch --show-current`
- Status: !`git status --short`
- Staged diff: !`git diff --cached`
- Unstaged diff: !`git diff`
- Recent commits: !`git log --oneline -10`

## Task

Turn the current changes into one or more **logical commits**, each with a **title-only** Conventional Commit message.
Hint from the user: $ARGUMENTS

## Steps

1. **Branch check.** If on `main`, stop: the ruleset only accepts changes through PRs. Suggest `/new-branch <type> <description>` and end.
2. **Lint first.** Run `make lint`. If it fails, run `make format`; the formatting changes become part of the commits below.
3. **Plan the commits.** Group all changed files (staged, unstaged and untracked) by **one logical change each**. Typical splits: build/config vs source, a feature vs the refactor it needed, code vs docs. Show the plan and wait for the user to confirm:
   ```
   1. feat(m2): add temporal demand aggregation    src/.../TemporalDemand.kt, src/.../Pipeline.kt
   2. test(m2): cover day-of-week bucketing        src/test/.../TemporalDemandTest.kt
   3. docs: document m2 output layout              README.md
   ```
   If the user had already staged a specific set, propose that set as commit 1.
4. **Write each title:**
   - `type(scope): subject`, with type in `feat fix perf refactor test docs build ci chore style revert`
   - Scope is optional. Prefer the pipeline area: `m1`, `m2`, `m3`, `o1`–`o4`, `ingest`, `spark`, `report`, `ci`, `tooling`
   - Subject: imperative, lowercase, no trailing period, ≤ 72 characters total
   - **No body.** One line only.
5. **Commit each group in order:**
   - `git restore --staged .` once at the start, so only the planned files go into each commit
   - `git add <explicit paths>` → `echo "<title>" | npx --no -- commitlint` → `git commit -m "<title>"`
   - The Husky hooks run on each commit. If one fails, report the output and stop. Don't continue with the remaining groups.

## Rules

- Title only: never pass a second `-m`, never open an editor.
- Never use `--no-verify`, `--amend`, `git push`, `git add -A` or `git add .`.
- Never stage `*.parquet`, `data/`, `.env*` or build output.
- If one file mixes two concerns, say so and commit it with the larger concern. Interactive hunk staging isn't available.

## Output

The list of new commits (`git log --oneline <first>^..HEAD`), any files left uncommitted, and the next step: `/pr`.

## Success Criteria

- [ ] Not on `main`
- [ ] `make lint` passes
- [ ] Each commit is one logical change with a title that passes commitlint
- [ ] Nothing pushed
