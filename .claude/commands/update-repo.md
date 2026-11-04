---
description: Work out where the current work stands (uncommitted, unpushed, no PR, behind main, ready to merge) and run only the steps needed to bring it up to date. Without an argument it stops once the PR is up to date; with "main" it carries on through the squash merge. Use when the user wants to sync, save, ship or "update the repo".
argument-hint: [target: current branch (default) | main]
allowed-tools: Bash(git status *), Bash(git diff *), Bash(git log *), Bash(git branch *), Bash(git switch *), Bash(git fetch *), Bash(git pull *), Bash(git merge *), Bash(git rev-parse *), Bash(git rev-list *), Bash(git add *), Bash(git restore --staged *), Bash(git commit *), Bash(git push *), Bash(make lint), Bash(make format), Bash(npx --no -- commitlint *), Bash(gh pr view *), Bash(gh pr create *), Bash(gh pr edit *), Bash(gh pr checks *), Bash(gh pr merge *), Bash(gh pr update-branch *), Read, Write
disable-model-invocation: true
---

# Update Repo

## Context

- Branch: !`git branch --show-current`
- Status: !`git status --short --branch`
- Recent commits: !`git log --oneline -5`

## Task

Bring the work up to date with the remote, choosing the steps from the repo's actual state.

**Target:** `$ARGUMENTS`
- Empty or the current branch's name: go as far as **an up-to-date PR**, and stop there.
- `main`: go all the way: **squash-merge into main**, delete the branch, sync local main.
- Any other branch name: if the working tree is clean, `git switch` to it first, then treat it as the current branch. Otherwise stop and say why.

The individual steps live in the other commands. **Read the matching file and follow its steps and rules exactly** instead of improvising:

| Step | Source |
|---|---|
| create branch | `.claude/commands/new-branch.md` |
| commit | `.claude/commands/commit.md` |
| push + PR | `.claude/commands/pr.md` |
| merge | `.claude/commands/merge.md` |

## Steps

1. **Refresh:** `git fetch --prune origin`.
2. **Detect state.** Collect all of these before deciding anything:
   - `branch`: current branch (`main` or a feature branch)
   - `dirty`: any staged, unstaged or untracked changes (`git status --porcelain`)
   - `ahead/behind`: vs its upstream (`git rev-list --left-right --count @{u}...HEAD`; no upstream means never pushed)
   - `main_moved`: commits on `origin/main` not in this branch (`git rev-list --count HEAD..origin/main`)
   - `pr`: `gh pr view --json number,state,title,isDraft,mergeable,url`, or none
   - `checks`: `gh pr checks` if a PR exists (pass / pending / fail)
3. **Plan.** Walk this decision list and collect every step that applies, in order:

   | Situation | Step |
   |---|---|
   | on `main`, dirty | create a branch (infer `type` and description from the diff), then commit |
   | on `main`, clean, behind `origin/main` | `git pull --ff-only`, then you're done |
   | on `main`, clean, up to date | nothing to do; say so |
   | feature branch, dirty | commit |
   | feature branch, `main_moved > 0` | update the branch from main: `gh pr update-branch` if a PR exists, else `git merge --no-edit origin/main`. Merge commits are squashed away on merge and ignored by commitlint |
   | unpushed commits, or no PR yet | push + PR |
   | PR exists, its title or body is out of date with the new commits | update the PR (`gh pr edit`) |
   | target is `main` and the PR is open | merge (only if checks pass) |
   | after merge | switch to `main`, `git pull --ff-only`, delete the local branch |

4. **Show the state and the plan, and wait for one confirmation:**
   ```
   State: feat/m2-temporal-demand · 3 files changed · 2 commits unpushed · no PR · main moved by 1
   Plan:
     1. commit   feat(m2): add borough-level demand aggregation   (2 files)
                 test(m2): cover hour-of-day bucketing            (1 file)
     2. update   merge origin/main into the branch
     3. push+PR  "feat(m2): add temporal demand analysis"
   Target: current branch, so stop after the PR
   ```
5. **Execute** step by step, using each source command's rules. **Stop at the first failure** (a hook, a conflict, a rejected push, failing checks), report what happened, and say what's left of the plan.
6. **Merge gate:** for target `main`, if checks are pending, stop and report. Don't poll or sleep; the user can re-run `/update-repo main` later. Even after the plan confirmation, **confirm again right before merging**, because a merge is hard to undo.

## Rules

- All the rules of the source commands still apply. In particular: never `--no-verify`, `--amend`, force-push, `git add -A`, stash, `git reset`, `-D`, or `gh pr merge --admin`.
- Never rebase. Branches are updated by merge, so no history is rewritten and no force-push is needed.
- Merge conflicts: stop and list the conflicting files. Don't resolve them silently.
- Don't do steps beyond the target.

## Output

```
Done: <steps executed>
Now: <branch> · <PR url + check status | merged as <sha>>
Next: <e.g. "checks pending, run /update-repo main when green">
```
