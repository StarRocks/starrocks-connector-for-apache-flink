# .claude/skills/

Repo-local, agent-agnostic procedures ("skills"). Each subdirectory is one skill: a `SKILL.md`
runbook plus the `scripts/` that do the work. Any coding agent (Claude Code, Codex, Cursor, …)
or a human can use them — open the skill's `SKILL.md`, follow it, and run its scripts. The
scripts locate the repo themselves (`git rev-parse --show-toplevel`, or set `CONNECTOR_REPO`),
so they work from any directory.

> Note: under `.claude/skills/`, Claude Code auto-discovers these as project skills, so a skill
> can be invoked by name (e.g. the `flink-connector-release` skill) or triggered from a matching
> prompt per its `SKILL.md` description. Other agents or humans can still open the skill's
> `SKILL.md` and run its scripts directly.

## Available skills

| Skill | What it does |
| --- | --- |
| [`flink-connector-release`](flink-connector-release/SKILL.md) | Release/publish `flink-connector-starrocks` to Maven Central — tag, build, strictly verify, deploy, and verify on Central. Enforces a hard pre-deploy validation gate because a published jar can never be changed. |

## Adding a skill

Create `.claude/skills/<name>/` with a `SKILL.md` (what it does + the steps) and a `scripts/`
directory, then add a row to the table above. Keep each skill self-contained; if several skills
end up sharing helper code, factor it into `.claude/skills/_lib/` and source it from the scripts.
