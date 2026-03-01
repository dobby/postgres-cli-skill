---
name: postgres-cli
description: Execute PostgreSQL queries against local or remote databases with shell `psql` using named project connections. Use when the user asks to inspect data, run SQL, or introspect schemas/tables/columns/indexes/rowcounts from the agent.
---

# Postgres CLI

Use `postgres-cli` to query PostgreSQL through named connection targets.

## Platform Support

- `scripts/postgres-cli` is a prebuilt release binary for `macOS arm64` only.
- This v1 package does not ship Linux/Windows binaries.
- Maintainers refresh the binary with `scripts/build-release-binary.sh`.

## Available Scripts

- `scripts/postgres-cli` - Executes SQL or introspection against a named target.
- `scripts/build-release-binary.sh` - Rebuilds and refreshes `scripts/postgres-cli` from `cargo build --release`.

## When To Use

- The user asks to inspect data in Postgres.
- The user asks to run SQL against a configured database target.
- The user asks for schema/table/column/index/rowcount introspection.

## Setup

Read [Setup Guide](references/SETUP.md).

## Inputs You Must Provide

- `--project-root` pointing at the repo root (optional if command runs from repo root).
- `--target` with a valid named connection.
- For query mode, exactly one of `--sql "<query>"`, `--sql-file <path>`, or `--introspect <mode>`.
- For schema-cache mode, use `--schema-cache update` (optionally `--all-tables`).

## Command Patterns

Run SQL directly:

```bash
scripts/postgres-cli --project-root /path/to/repo --target webshop-read --sql "SELECT now();"
```

Run SQL from file:

```bash
scripts/postgres-cli --project-root /path/to/repo --target webshop-read --sql-file /path/to/query.sql
```

Run introspection:

```bash
scripts/postgres-cli --project-root /path/to/repo --target webshop-read --introspect tables
```

Update schema cache from configured `important_tables`:

```bash
scripts/postgres-cli --project-root /path/to/repo --target webshop-read --schema-cache update
```

`important_tables` seeds the cache; directly related tables are added automatically.

Schema cache file naming is configured in `.agent/postgres-cli/postgres.toml`:

```toml
[schema_cache]
file_naming = "table"        # default: tables/<table>.md
# file_naming = "schema_table" # legacy: tables/<schema>.<table>.md
```

With `file_naming = "table"`, schema-cache update fails if selected schemas contain duplicate table
names. Use `schema_table` or narrow configured schemas/search_path when duplicates are expected.

Update schema cache for all tables in `current_schemas(false)`:

```bash
scripts/postgres-cli --project-root /path/to/repo --target webshop-read --schema-cache update --all-tables
```

Supported introspection modes:

- `schemas`
- `tables`
- `columns`
- `indexes`
- `rowcounts`
- `rowcounts-exact`

## Agent Guidelines

- Pass a valid named target with `--target`.
- Use either query mode (`--sql`/`--sql-file`/`--introspect`) or schema-cache mode (`--schema-cache update`).
- Prefer read targets unless write operations are explicitly requested.
- Return relevant query output to the user.

## Progressive Schema Loading

When schema context is needed, use this order to avoid overloading context:

1. Read `.agent/postgres-cli/schema/index.json`.
2. Load only required files from `.agent/postgres-cli/schema/tables/`.
3. Read `.agent/postgres-cli/schema/relations.md` only when join/relationship reasoning is needed.
4. If cache is missing or stale, ask to run `--schema-cache update`.
