#!/usr/bin/env bash
# 04_deploy.sh [minor ...]  — THE IRREVERSIBLE STEP: publish via the repo's deploy.sh.
#
# A published jar can never be changed. This script therefore refuses to run unless
# 03_build_verify.sh has already passed for the EXACT commit currently checked out,
# and it requires an explicit confirmation. It then runs the repo's `deploy.sh` per
# version, which does `mvn clean deploy -Prelease` (GPG-signs and publishes to
# Maven Central, blocking until each version is live).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"
REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"

pom_is_snapshot "$REPO_ROOT" && die "pom is still a -SNAPSHOT — you are not on the release tag"
SRFC="$(pom_srfc_version "$REPO_ROOT")"
EXPECTED_COMMIT="$(git rev-parse HEAD)"

# Gate 1: stage 03 must have verified THIS commit.
MARKER="$REPO_ROOT/.release/verified-$SRFC.commit"
[ -f "$MARKER" ] || die "no verification marker — run 03_build_verify.sh first (it must pass)"
marked="$(head -1 "$MARKER")"
[ "$marked" = "$EXPECTED_COMMIT" ] || die "marker commit $marked != current $EXPECTED_COMMIT — re-run 03_build_verify.sh for this exact commit"
# The commit alone isn't enough: deploy.sh rebuilds from the worktree, and git-commit-id stamps
# HEAD, so a dirty tree would publish uncommitted bytes that still report the tag commit (05 can't
# catch it). Require a clean tree so the published bytes equal what 03 verified.
[ -z "$(git status --porcelain)" ] \
  || die "working tree not clean (uncommitted or untracked files) — deploy must build the exact verified bytes; commit/stash/clean and re-run 03"
pass "verification marker matches current commit, and the working tree is clean"

mapfile -t VERSIONS < <(resolve_versions "$REPO_ROOT" "$@")
[ "${#VERSIONS[@]}" -gt 0 ] || die "no Flink versions resolved — is common.sh updated to support 'supported-minor-versions'?"

# Gate 1b: every version we are about to deploy must have passed 03 — the marker records exactly
# which versions were verified (its "versions:" line). 03 may have run on a subset, so checking the
# commit alone is not enough; otherwise `03_build_verify.sh 1.20` then `04_deploy.sh` would publish
# 1.16–1.19 unverified.
verified="$(sed -n 's/^versions: //p' "$MARKER")"
for m in "${VERSIONS[@]}"; do
  case " $verified " in
    *" $m "*) : ;;
    *) die "flink $m was not verified by 03 (verified: ${verified:-none}) — run 03_build_verify.sh $m first";;
  esac
done
pass "all requested versions ($(IFS=,; echo "${VERSIONS[*]}")) were verified by 03"

# Gate 1c: deploy.sh rebuilds and shades whatever stream-load SDK is in the local repo. That SDK
# could have changed since 03 (another checkout reinstalled it, or Maven refreshed the snapshot) —
# the commit/marker wouldn't catch it. Re-check the installed SDK still matches the tag before the
# irreversible deploy.
verify_installed_sdk "$REPO_ROOT" "$EXPECTED_COMMIT"
pass "the locally-installed SDK still matches the tag"

# Gate 2: explicit human confirmation, because this cannot be undone.
echo
warn "About to PUBLISH to Maven Central (cannot be undone):"
for m in "${VERSIONS[@]}"; do printf '     com.starrocks:flink-connector-starrocks:%s_flink-%s\n' "$SRFC" "$m"; done
if [ -t 0 ]; then
  printf 'Type the version "%s" to confirm: ' "$SRFC"; read -r ans
  [ "$ans" = "$SRFC" ] || die "confirmation mismatch — aborted"
else
  [ "${CONFIRM_DEPLOY:-}" = "$SRFC" ] || die "non-interactive: set CONFIRM_DEPLOY=$SRFC to proceed"
fi

# Suppress SNAPSHOT updates so deploy.sh shades the locally-verified SDK, not a refreshed remote one.
pin_local_snapshots

declare -a RESULT; overall=0
for m in "${VERSIONS[@]}"; do
  info "──────── deploy flink $m (via deploy.sh) ────────"
  if bash deploy.sh "$m"; then
    pass "published flink $m"; RESULT+=("$m PUBLISHED")
  else
    fail "deploy.sh FAILED for flink $m"; RESULT+=("$m FAILED"); overall=1
    warn "stopping. Check https://central.sonatype.com Deployments; re-run only the failed version: scripts/04_deploy.sh $m"
    break
  fi
done

echo
info "── deploy summary ──"; for r in "${RESULT[@]}"; do printf '   %s\n' "$r"; done
if [ "$overall" -eq 0 ]; then
  info "${C_GRN}DEPLOY COMPLETE${C_RST}"
  echo  "Next:  git push origin v$SRFC   (if not pushed)   then   scripts/05_verify_central.sh $SRFC"
else
  die "deploy incomplete — see summary above"
fi
