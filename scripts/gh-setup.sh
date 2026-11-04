#!/usr/bin/env sh
# One-time GitHub setup: squash-only merges with title-only commits, auto-deleted branches, and the main ruleset.
# Safe to re-run: the ruleset is updated in place if it already exists.
set -eu

repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
ruleset=.github/rulesets/main.json
name=$(node -p "require('./$ruleset').name")

# The ruleset requires the CI checks, so CI must exist on main first or every PR blocks forever.
if ! gh api "repos/$repo/contents/.github/workflows/ci.yml" --silent 2>/dev/null; then
  echo "gh-setup: push .github/workflows/ci.yml to main and let it run once before applying the ruleset." >&2
  exit 1
fi

gh api --method PATCH "repos/$repo" --silent \
  -F allow_squash_merge=true -F allow_merge_commit=false -F allow_rebase_merge=false \
  -f squash_merge_commit_title=PR_TITLE -f squash_merge_commit_message=BLANK \
  -F delete_branch_on_merge=true
echo "Merge settings: squash only, commit = PR title, branches auto-deleted"

id=$(gh api "repos/$repo/rulesets" --jq ".[] | select(.name == \"$name\") | .id")
if [ -n "$id" ]; then
  gh api --method PUT "repos/$repo/rulesets/$id" --input "$ruleset" --silent
  echo "Ruleset '$name' updated ($id)"
else
  gh api --method POST "repos/$repo/rulesets" --input "$ruleset" --silent
  echo "Ruleset '$name' created"
fi
