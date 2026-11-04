---
description: Push the current branch and open (or update) a GitHub PR with a Conventional Commit title and the filled-in PR template. Use when a branch's commits are ready for review.
argument-hint: [optional title override]
allowed-tools: Bash(git status *), Bash(git branch *), Bash(git log *), Bash(git diff *), Bash(git push *), Bash(gh pr create *), Bash(gh pr view *), Bash(gh pr edit *), Bash(npx --no -- commitlint *), Read, Write
disable-model-invocation: true
---

# Pull Request

## Context

- Branch: !`git branch --show-current`
- Status: !`git status --short`
- Commits on this branch: !`git log --oneline origin/main..HEAD`
- Diff summary: !`git diff --stat origin/main...HEAD`

## Task

Push this branch and open a PR. PRs are **squash-merged**, so **the PR title becomes the single commit on `main`**, and it must follow the convention.
Title override: $ARGUMENTS

## Steps

1. **Guards:**
   - On `main` → stop and suggest `/new-branch`
   - Uncommitted changes → stop and suggest `/commit`
   - No commits ahead of `origin/main` → stop, nothing to open
2. **Title:** `type(scope): subject`, ≤ 72 characters. Take it from the override, from the only commit if there is just one, or summarise the branch otherwise. The type should match the branch prefix. Validate it: `echo "<title>" | npx --no -- commitlint`.
3. **Body:** read `.github/pull_request_template.md` and fill it from the commits and diff:
   - *Summary* and *Changes*: from the commits and diff
   - *Type of change*: tick the box that matches the type
   - *How to test*: concrete commands (`make check`, how to run the affected operation)
   - *Performance*: for `perf` branches, fill it from `experiments/LOG.md` (H/OPT ids, run-ids, numbers). For any other branch, delete the section. Never invent numbers; leave a `TODO` if they're missing.
   - Delete *Screenshots* if it doesn't apply. Tick the checklist only for what's verifiably true.
   - Write the body to `.git/PR_BODY.md`. Git never tracks files inside `.git/`.
4. **Push:** `git push -u origin HEAD`. The `pre-push` hook runs `make check`. If it fails, report the output and stop.
5. **Open the PR, or update it if one already exists:**
   - Check first: `gh pr view --json url,title`
   - New: `gh pr create --base main --title "<title>" --body-file .git/PR_BODY.md`
   - Existing: `gh pr edit --title "<title>" --body-file .git/PR_BODY.md`

## Rules

- Never force-push. If the push is rejected, report and stop.
- Never merge here. That's `/merge`.

## Output

The PR URL and title, plus the next step: wait for CI, optionally run `/review`, then `/merge`.
