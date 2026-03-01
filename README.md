# postgres-cli skill

`postgres-cli` is a reusable agent skill for running PostgreSQL SQL and schema introspection through named project connections.

It also supports schema-cache generation for progressive agent context loading via:

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target <name> --schema-cache update
```

## Schema cache file naming

By default, schema cache table files are tenant-agnostic:

- `tables/<table>.md` (`file_naming = "table"`)

Configure this in your project's `.agent/postgres-cli/postgres.toml`:

```toml
[schema_cache]
file_naming = "table"        # default
# file_naming = "schema_table" # legacy behavior: tables/<schema>.<table>.md
```

When using `file_naming = "table"`, `postgres-cli` fails fast if two selected schemas contain the same
table name. Use `schema_table` mode (or narrow `schema`/`search_path`) when duplicates are expected.

## Install

```bash
npx skills add dobby/postgres-cli-skill --skill postgres-cli
```

Install telemetry from this command is what gets skills indexed on [skills.sh](https://skills.sh/).

## Repository layout

- `skills/postgres-cli/SKILL.md` skill metadata + instructions
- `skills/postgres-cli/scripts/postgres-cli` prebuilt macOS arm64 release binary
- `skills/postgres-cli/scripts/build-release-binary.sh` maintainer helper to rebuild the binary
- `skills/postgres-cli/references/postgres.toml.example` starter config
- `skills/postgres-cli/references/SETUP.md` setup and usage guide

## Maintainer release workflow

```bash
skills/postgres-cli/scripts/build-release-binary.sh
git add skills/postgres-cli/scripts/postgres-cli
git commit -m "Refresh postgres-cli release binary"
git push
```
