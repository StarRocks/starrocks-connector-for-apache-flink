#!/usr/bin/env bash
# 00_preflight.sh  — read-only ENVIRONMENT readiness check. Changes nothing.
#
# Run this first. It fails loudly if the machine cannot produce a correct,
# signed, publishable release (no maven / not JDK 8 / no gpg key / no Central
# credentials), so you find out before cutting a tag or (worse) publishing a broken jar.
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

# 2. Maven must run on JDK 8 — a hard requirement, not a warning. The pom compiles with
#    -source/-target 1.8, not --release 8, so a newer javac links against its own class library:
#    e.g. ByteBuffer.flip() in the bundled SDK becomes flip()Ljava/nio/ByteBuffer;, which throws
#    NoSuchMethodError on a Java 8 runtime. The build still succeeds (only warnings), and a
#    published jar can never be fixed. Maven compiles on the JDK it runs on (JAVA_HOME), so ask
#    `mvn -v`, not the `java` on PATH. Fail closed when the version cannot be read.
jline="$("${MVN[@]}" -v 2>/dev/null | grep -i 'Java version' || true)"
jver="$(sed -n 's/.*[Jj]ava version: *\([^, ]*\).*/\1/p' <<<"$jline")"
case "$jver" in
  1.8.*) pass "Java 8 ($jline)";;
  "")    fail "could not determine the Java version from '${MVN[*]} -v' — the release must be built on JDK 8"; hard=$((hard+1));;
  *)     fail "Maven runs on Java $jver, not 1.8 — the release must be built on JDK 8; export JAVA_HOME=/path/to/jdk8 and keep it set for every stage"; hard=$((hard+1));;
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
