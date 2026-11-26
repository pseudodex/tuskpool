#!/usr/bin/env bash
# Pick the GitHub account for this repository from its remote URL.
#
#   bash scripts/git-account.sh [remote]          set up this repository (default remote: origin)
#   bash scripts/git-account.sh --use <account>   choose the account for this repository explicitly
#   bash scripts/git-account.sh --clear           forget that choice (go back to using the remote's owner)
#   bash scripts/git-account.sh --gh <gh args>    run the GitHub CLI as this repository's account
#
# Which account a repository uses, in order of precedence:
#   1. the account chosen with --use (remembered in this repository's .git/config as account.use)
#   2. the account that owns the remote's owner (user or organisation) in
#      ~/.config/git-accounts.gitconfig (override the path with GIT_ACCOUNTS_FILE)
# --use needs no remote, so it also works for a new project before it has one. Either way the script
# writes into THIS repository's own .git/config only:
#   - user.name and user.email                     who the commits are by
#   - HTTPS remotes: a credential helper that answers with that account's GitHub CLI token
#   - SSH remotes:   core.sshCommand with that account's key
# The globally active GitHub CLI account is never switched, so other repositories are unaffected and
# two repositories on different accounts can be used at the same time.
#
# Logging in is a one-time step per account that this script cannot do for you:
#   HTTPS: gh auth login   (once per account; sign in as that account)
#   SSH:   add the key to that GitHub account
#
# Example mapping file:
#   [account "personal"]
#       owners = alice, alice-labs
#       name = Alice Example
#       email = alice@example.com
#       ghUser = alice
#       sshKey = ~/.ssh/id_ed25519_alice
#   [account "work"]
#       owners = acme
#       name = Alice Example
#       email = alice@acme.example
#       ghUser = alice-acme
#
# Exit codes: 0 ready, 2 no such remote, 3 no account owns the remote, 4 mapping file missing,
#             5 identity set, but pushing over HTTPS would not use this account (no ghUser in the mapping,
#               or that account is not logged in to the GitHub CLI).
set -euo pipefail

mode=auto; use_name=""
case "${1:-}" in
  --gh) mode=gh; shift ;;
  --use) mode=use; use_name="${2:-}"; [ -n "$use_name" ] || { echo "usage: git-account.sh --use <account>" >&2; exit 64; }; shift 2 ;;
  --clear) git config --local --unset account.use 2>/dev/null || true; echo "cleared: this repository follows its remote's owner again"; exit 0 ;;
esac
remote=origin
[ "$mode" != auto ] || remote="${1:-origin}"
file="${GIT_ACCOUNTS_FILE:-$HOME/.config/git-accounts.gitconfig}"
[ -f "$file" ] || { echo "mapping file not found: $file (see the header of this script)" >&2; exit 4; }
url="$(git remote get-url "$remote" 2>/dev/null || true)"

owner_of() { # URL -> owner
  local path
  case "$1" in
    *://*) path="${1#*://}"; path="${path#*/}" ;;   # scheme://[user@]host[:port]/owner/repo
    *@*:*) path="${1#*:}" ;;                         # user@host:owner/repo (also SSH host aliases)
    *) return 1 ;;
  esac
  printf '%s\n' "${path%%/*}" | tr '[:upper:]' '[:lower:]'
}
host_of() { # https URL -> host
  local rest="${1#*://}"; rest="${rest#*@}"; rest="${rest%%/*}"; printf '%s\n' "${rest%%:*}"
}
account=""
if [ "$mode" = use ]; then
  if ! git config -f "$file" --get "account.$use_name.email" >/dev/null 2>&1; then
    echo "no account named '$use_name' in $file. Available:" >&2
    git config -f "$file" --name-only --get-regexp '^account\..*\.email$' | sed 's/^account\.//; s/\.email$//; s/^/  /' >&2
    exit 3
  fi
  account="$use_name"
  git config --local account.use "$account"
else
  account="$(git config --local --get account.use || true)"
fi

owner=""
if [ -z "$account" ]; then
  [ -n "$url" ] || { echo "no remote named '$remote' to read the account from. Choose it now: bash scripts/git-account.sh --use <account>" >&2; exit 2; }
  owner="$(owner_of "$url")" || { echo "cannot read an owner from: $url" >&2; exit 3; }
  while read -r key; do
    for candidate in $(git config -f "$file" --get "$key" | tr ',' ' ' | tr '[:upper:]' '[:lower:]'); do
      if [ "$candidate" = "$owner" ]; then account="${key#account.}"; account="${account%.owners}"; break 2; fi
    done
  done < <(git config -f "$file" --name-only --get-regexp '^account\..*\.owners$' || true)
  if [ -z "$account" ]; then
    echo "no account in $file owns '$owner' (remote: $url)." >&2
    echo "Ask which account to use, then run: bash scripts/git-account.sh --use <account>   (add it to the file first if it is new). Do not guess." >&2
    exit 3
  fi
fi

get() { git config -f "$file" --get "account.$account.$1" || true; }
name="$(get name)"; email="$(get email)"; gh_user="$(get ghUser)"; ssh_key="$(get sshKey)"
[ -n "$name" ] && [ -n "$email" ] || { echo "account '$account' needs name and email" >&2; exit 3; }

https=0; ssh=0; host="github.com"
case "$url" in
  "") https=1; ssh=1 ;;                                  # no remote yet: prepare for either kind
  http://* | https://*) https=1; host="$(host_of "$url")" ;;
  *) ssh=1 ;;
esac

if [ "$mode" = gh ]; then
  [ -n "$gh_user" ] || { echo "account '$account' has no ghUser" >&2; exit 3; }
  token="$(gh auth token --hostname "$host" --user "$gh_user" 2>/dev/null)" \
    || { echo "'$gh_user' is not logged in to the GitHub CLI: run  gh auth login --hostname $host" >&2; exit 5; }
  GH_TOKEN="$token" exec gh "$@"
fi

git config --local user.name "$name"
git config --local user.email "$email"
status=0
if [ "$https" = 1 ]; then
  if [ -z "$gh_user" ] && [ "$ssh" = 1 ]; then
    : # no remote yet and no ghUser: fine for SSH; nothing to prepare for HTTPS
  elif [ -z "$gh_user" ]; then
    echo "account '$account' has no ghUser, so HTTPS pushes would use whichever GitHub CLI account is active: add ghUser to the mapping file" >&2
    status=5
  elif gh auth token --hostname "$host" --user "$gh_user" >/dev/null 2>&1; then
    # An empty value clears any helper configured globally for this host; ours then answers alone.
    git config --local --replace-all "credential.https://$host.helper" ""
    git config --local --add "credential.https://$host.helper" \
      "!f() { test \"\$1\" = get && echo username=$gh_user && echo password=\$(gh auth token --hostname $host --user $gh_user); }; f"
  else
    echo "'$gh_user' is not logged in to the GitHub CLI: run  gh auth login --hostname $host  and sign in as $gh_user" >&2
    status=5
  fi
fi
if [ "$ssh" = 1 ] && [ -n "$ssh_key" ]; then
  git config --local core.sshCommand "ssh -i ${ssh_key/#\~/$HOME} -o IdentitiesOnly=yes"
fi
echo "account '$account'${owner:+ for owner '$owner'}: $name <$email>${gh_user:+, GitHub user $gh_user}${ssh_key:+, SSH key $ssh_key}"
exit "$status"
