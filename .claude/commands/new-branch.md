---
description: Create a conventionally named branch (type/short-description) from an up-to-date main. Use when starting a feature, fix, perf change, refactor or any new piece of work.
argument-hint: <type> <short description>, e.g. feat m2 temporal demand
allowed-tools: Bash(git status *), Bash(git branch *), Bash(git switch *), Bash(git fetch *), Bash(git pull *), Bash(git rev-parse *), Bash(git log *)
disable-model-invocation: true
---

# New Branch

## Context

- Current branch: !`git branch --show-current`
- Status: !`git status --short`

## Task

Create and switch to a branch for: $ARGUMENTS

## Steps

1. **Work out the name:** `<type>/<kebab-description>`.
   - `type` is one of `feat fix perf refactor test docs build ci chore style revert`. If it's missing or ambiguous, infer it from the description and say which you picked.
   - Description: 2–5 lowercase words joined by `-`, no ticket noise. E.g. `feat/m2-temporal-demand`, `perf/broadcast-zone-lookup`, `refactor/split-cleaning-steps`.
   - Check it doesn't already exist: `git branch --list <name>`.
2. **Start from a fresh main:**
   - Clean working tree: `git switch main` → `git pull --ff-only` → `git switch -c <name>`
   - Uncommitted changes: don't stash or discard them. Run `git switch -c <name>` from where you are so the changes come along, and say that the branch starts from the current HEAD, not a freshly pulled main.
3. Confirm with `git branch --show-current`.

## Rules

- Never `git stash`, `git reset`, `git checkout -- .`, or delete anything.
- If `git pull --ff-only` fails because main has diverged, stop and report. Don't merge or rebase main on your own.

## Output

`Switched to <name> (from main @ <short-sha>)`, plus a reminder of the next step: make changes, then `/commit`.
