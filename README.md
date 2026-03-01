# postgres-cli skill

`postgres-cli` is a reusable agent skill for running PostgreSQL SQL and schema introspection through named project connections.

## Install

```bash
npx skills add dobby/postgres-cli-skill --skill postgres-cli
```

Install telemetry from this command is what gets skills indexed on [skills.sh](https://skills.sh/).

## Repository layout

- `SKILL.md` skill metadata + instructions
- `scripts/postgres-cli` prebuilt macOS arm64 release binary
- `scripts/build-release-binary.sh` maintainer helper to rebuild `scripts/postgres-cli`
- `references/postgres.toml.example` starter config
- `references/SETUP.md` setup and usage guide

## Maintainer release workflow

```bash
scripts/build-release-binary.sh
git add scripts/postgres-cli
git commit -m "Refresh postgres-cli release binary"
git push
```

