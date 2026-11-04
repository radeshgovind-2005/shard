---
description: Squash-merge the current branch's PR once checks pass (title-only commit on main), delete the branch, and sync local main. Use when a PR is approved and green.
argument-hint: [PR number, default: PR for the current branch]
allowed-tools: Bash(git status *), Bash(git branch *), Bash(git switch *), Bash(git pull *), Bash(git fetch *), Bash(git log *), Bash(gh pr view *), Bash(gh pr checks *), Bash(gh pr merge *)
disable-model-invocation: true
---

# Merge

## Context

- Branch: !`git branch --show-current`
- Status: !`git status --short`

## Task

Squash-merge PR `$ARGUMENTS` (default: the PR for the current branch) so `main` gets exactly one commit whose message is the PR title, then clean up.

## Steps

1. **Inspect:** `gh pr view $ARGUMENTS --json number,title,state,isDraft,mergeable,reviewDecision,headRefName,url`
   - Stop if it's not `OPEN`, is a draft, or `mergeable` is `CONFLICTING`. For conflicts, suggest updating the branch from main and say what conflicts.
2. **Checks:** run `gh pr checks $ARGUMENTS`. Every required check (`build`, `commitlint`) must pass. If any is pending, report and stop; don't wait in a loop. If any failed, show which and stop.
3. **Title:** confirm the PR title follows the convention (CI's `commitlint` job checks it). It becomes the commit on `main`.
4. **Confirm with the user** before merging: show `#<n> <title> → main (squash)`. Merging is hard to undo.
5. **Merge:** `gh pr merge $ARGUMENTS --squash --delete-branch --subject "<PR title>" --body ""`. The empty body gives a title-only commit on main.
6. **Sync:** `git switch main` → `git pull --ff-only` → `git log --oneline -3`. Delete the local branch if `gh` didn't: `git branch -d <branch>`. Use lowercase `-d` only; it refuses to delete unmerged work.

## Rules

- Squash only. Never `--merge`, `--rebase`, or `--admin` (which bypasses the ruleset).
- Never force-delete a branch (`-D`).
- If the working tree has uncommitted changes, stop before switching branches.

## Output

`Merged #<n> as <sha> "<title>"`, branch deleted, local main up to date. Next: `/new-branch` for the next piece of work.
