#!/usr/bin/env bash
# 00_preflight.sh  — read-only ENVIRONMENT readiness check. Changes nothing.
#
# Run this first. It fails loudly if the machine cannot produce a correct,
# signed, publishable release (no maven / no gpg key / no Central credentials),
# so you find out before cutting a tag or (worse) publishing a broken jar.
# Repo/version/tag-state checks live in 01_tag.sh.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

REPO_ROOT="$(resolve_repo)"
# CUSTOM_MVN may carry args (the repo's CI uses `mvn -B -ntp`), so treat it as command+args the way
# common.sh does: split into an array whose first element is the executable and the rest are flags.
MVN=(${CUSTOM_MVN:-mvn})

hard=0

info "Preflight (environment readiness) for flink-connector-starrocks  (repo: $REPO_ROOT)"

# 1. maven (check the executable — the first word — exists)
if command -v "${MVN[0]}" >/dev/null 2>&1; then pass "maven found: $(command -v "${MVN[0]}")"
else fail "maven (${MVN[0]}) not found"; hard=$((hard+1)); fi

# 2. java 8 (connector targets 1.8; building on a much newer JDK can surprise you)
jline="$("${MVN[@]}" -v 2>/dev/null | grep -i 'Java version' || true)"
case "$jline" in
  *1.8*) pass "Java 8 ($jline)";;
  "")    warn "could not determine Java version from '${MVN[*]} -v'";;
  *)     warn "Java is not 1.8 — connector targets 1.8: $jline";;
esac

# 3. gpg signing key (release profile signs every artifact)
if command -v gpg >/dev/null 2>&1 && [ -n "$(gpg --list-secret-keys 2>/dev/null)" ]; then
  pass "gpg secret key present"
else
  fail "no gpg secret key — 'mvn ... -Prelease' will fail to sign"; hard=$((hard+1))
fi

# 4. Central Portal credentials in settings.xml (publishingServerId=central)
SETTINGS="${HOME}/.m2/settings.xml"
if [ -f "$SETTINGS" ] && grep -q '<id>central</id>' "$SETTINGS"; then
  pass "settings.xml has a <server><id>central</id> entry"
else
  fail "no <server><id>central</id> in $SETTINGS — deploy will not authenticate to Maven Central"; hard=$((hard+1))
fi

# 5. network to Maven Central mirror (used by 05; warn only)
if command -v curl >/dev/null 2>&1; then
  if curl -fsI --max-time 10 https://repo1.maven.org/maven2/ >/dev/null 2>&1; then pass "reachable: repo1.maven.org"
  else warn "could not reach repo1.maven.org (only needed for stage 05 verification)"; fi
fi

echo
if [ "$hard" -eq 0 ]; then info "${C_GRN}ENVIRONMENT OK${C_RST} — next: scripts/01_tag.sh <version>"; exit 0
else die "$hard blocking problem(s) — fix them before continuing"; fi
