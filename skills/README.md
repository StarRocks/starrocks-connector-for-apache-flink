# skills/

Repo-local, agent-agnostic procedures ("skills"). Each subdirectory is one skill: a `SKILL.md`
runbook plus the `scripts/` that do the work. Any coding agent (Claude Code, Codex, Cursor, …)
or a human can use them — open the skill's `SKILL.md`, follow it, and run its scripts. The
scripts locate the repo themselves (`git rev-parse --show-toplevel`, or set `CONNECTOR_REPO`),
so they work from any directory.

> Note: this is a plain folder, **not** an auto-discovered skill location. Agents won't trigger
> these automatically from a vague prompt — invoke a skill explicitly (e.g. "use
> `skills/flink-connector-release`") or point the agent at it. That's intentional: these are
> deliberate, manually-run procedures.

## Available skills

| Skill | What it does |
| --- | --- |
| [`flink-connector-release`](flink-connector-release/SKILL.md) | Release/publish `flink-connector-starrocks` to Maven Central — tag, build, strictly verify, deploy, and verify on Central. Enforces a hard pre-deploy validation gate because a published jar can never be changed. |

## Adding a skill

Create `skills/<name>/` with a `SKILL.md` (what it does + the steps) and a `scripts/` directory,
then add a row to the table above. Keep each skill self-contained; if several skills end up
sharing helper code, factor it into `skills/_lib/` and source it from the scripts.
