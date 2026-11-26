#!/usr/bin/env bash
# Keep this project in step with the template it was generated from.
#
#   bash scripts/template-update.sh           say whether the template has a newer version
#   bash scripts/template-update.sh --apply   update to the newest version
#
# Without arguments it prints the steps to update when the project is behind, and nothing when
# it is current or the template cannot be reached. It always exits 0, so it is safe to run at
# the start of every session.
#
# --apply runs `copier update`, then merges the files copier cannot merge itself: files git does
# not track here (they are excluded locally), and, in a project the template was applied to after
# the fact (`adopt: true`), every file the template installs. Each is merged three ways (this
# project's file, the previous template version, the new one). A file removed from the project
# stays removed. Conflicts are left as markers and listed. Exit codes: 0 updated (or nothing to
# do), 1 could not update, 2 updated with conflicts to resolve.
#
# Env: TEMPLATE_UPDATE_TIMEOUT (seconds to wait for the template's tag list, default 15),
#      TEMPLATE_UPDATE_ROOT (project directory, default: the parent of this script's directory).
set -uo pipefail

main() {
  root="${TEMPLATE_UPDATE_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
  answers="$root/.copier-answers.yml"
  [ -f "$answers" ] || exit 0
  source_path="$(field _src_path)"
  recorded="$(field _commit)"
  [ -n "$source_path" ] && [ -n "$recorded" ] || exit 0

  # A project generated between two tags records `v1.2.0-4-gabc1234`: it is at least at v1.2.0.
  current="$(printf '%s\n' "$recorded" | sed 's/-[0-9][0-9]*-g[0-9a-f]*$//')"
  case "$current" in v[0-9]*) ;; *) exit 0 ;; esac

  latest="$(GIT_TERMINAL_PROMPT=0 timeout "${TEMPLATE_UPDATE_TIMEOUT:-15}" \
    git ls-remote --tags --refs "$source_path" 'v[0-9]*' 2>/dev/null |
    sed 's|.*refs/tags/||' | sort -V | tail -n 1)"
  if [ -z "$latest" ]; then
    [ "${1:-}" = "--apply" ] && { echo "cannot read the template's tags at $source_path"; exit 1; }
    exit 0
  fi
  [ "$latest" != "$current" ] || exit 0
  [ "$(printf '%s\n%s\n' "$current" "$latest" | sort -V | tail -n 1)" = "$latest" ] || exit 0

  if [ "${1:-}" = "--apply" ]; then apply; else notice; fi
}

field() { sed -n "s/^$1:[[:space:]]*//p" "$answers" | head -n 1 | tr -d "'\"" | tr -d '\r'; }

run_copier() { if command -v copier >/dev/null 2>&1; then command copier "$@"; else uvx copier "$@"; fi; }

notice() {
  cat <<EOF
Template update available: this project is at $recorded and the template ($source_path) is at $latest.
Update before starting other work, unless the user says not to:
1. Run \`git status --porcelain\`. If it prints anything, tell the user the update is waiting on uncommitted changes and carry on with their request.
2. Run \`bash scripts/template-update.sh --apply\`. It updates the project and lists any file left with conflict markers.
3. Read the template's CHANGELOG.md for every version after $current up to $latest and do each "Manual action in existing projects".
4. Resolve every conflict marker (\`<<<<<<<\`), keeping the project's own content and taking the template's tooling and rules.
5. Run the project's check command and fix what fails.
6. Commit as \`chore: update to template $latest\` and push.
If a step cannot be finished, run \`git reset --hard && git clean -fd\` to return to the last commit, and tell the user what blocked it.
EOF
}

# Render the template at one version into a directory, with this project's answers.
render() { # ref destination
  run_copier copy --trust --defaults --skip-tasks -q --vcs-ref "$1" --data-file "$work/answers.yml" "$source_path" "$2" >/dev/null 2>&1
}

# Is this path merged here instead of by copier?
merged_here() { # path relative to the project
  [ "$1" != ".copier-answers.yml" ] || return 1
  [ "$adopted" = true ] && return 0
  ! git -C "$root" ls-files --error-unmatch -- "$1" >/dev/null 2>&1
}

apply() {
  cd "$root" || exit 1
  if [ -n "$(git status --porcelain)" ]; then echo "the working tree has uncommitted changes: commit them, then run this again"; exit 1; fi
  work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
  grep -v -e '^_' -e '^#' "$answers" >"$work/answers.yml"
  adopted=false; grep -q '^adopt:[[:space:]]*true' "$answers" && adopted=true

  # The version this project came from: the commit when it was generated between two tags.
  previous="$current"
  case "$recorded" in *-g[0-9a-f]*) previous="${recorded##*-g}" ;; esac
  render "$previous" "$work/base" || { rm -rf "$work/base"; mkdir -p "$work/base"; echo "note: could not render the template at $recorded; files changed on both sides will show as conflicts"; }
  render "$latest" "$work/new" || { echo "could not render the template at $latest"; exit 1; }

  # Keep this project's copy of every file merged here, before copier touches it.
  mkdir -p "$work/ours"
  (cd "$work/new" && find . -type f ! -path './.git/*' | sed 's|^\./||' | sort) >"$work/files"
  while IFS= read -r file; do
    if merged_here "$file" && [ -f "$root/$file" ]; then mkdir -p "$work/ours/$(dirname "$file")"; cp -p "$root/$file" "$work/ours/$file"; fi
  done <"$work/files"

  echo "== updating from $recorded to $latest"
  run_copier update --trust --defaults --skip-answered -q --vcs-ref "$latest" --conflict inline || { echo "copier update failed"; exit 1; }

  conflicts=()
  while IFS= read -r file; do
    merged_here "$file" || continue
    git reset -q -- "$file" 2>/dev/null || true
    if [ ! -f "$work/ours/$file" ]; then
      # Not in the project before: new in this version, or removed here on purpose.
      if [ -f "$work/base/$file" ]; then rm -f "$root/$file"; else mkdir -p "$root/$(dirname "$file")"; cp -p "$work/new/$file" "$root/$file"; fi
      continue
    fi
    mkdir -p "$root/$(dirname "$file")"; cp -p "$work/ours/$file" "$root/$file"
    cmp -s "$work/ours/$file" "$work/new/$file" && continue
    base="$work/base/$file"; [ -f "$base" ] || { base="$work/empty"; : >"$base"; }
    git merge-file -L project -L "template $current" -L "template $latest" "$root/$file" "$base" "$work/new/$file" >/dev/null 2>&1; code=$?
    if [ "$code" -gt 127 ]; then cp -p "$work/ours/$file" "$root/$file"; conflicts+=("$file (could not be merged: compare with the template by hand)")
    elif [ "$code" -ne 0 ]; then conflicts+=("$file"); fi
  done <"$work/files"

  while IFS= read -r file; do conflicts+=("$file"); done < <(git diff --name-only --diff-filter=U; git grep -l '^<<<<<<< ' -- . 2>/dev/null)
  if [ ${#conflicts[@]} -eq 0 ]; then echo "== updated to $latest with no conflicts"; exit 0; fi
  echo "== updated to $latest; resolve the conflict markers in:"
  printf '%s\n' "${conflicts[@]}" | sort -u | sed 's/^/  /'
  exit 2
}

main "$@"
exit
