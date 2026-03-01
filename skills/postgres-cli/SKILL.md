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
- Exactly one of `--sql "<query>"`, `--sql-file <path>`, or `--introspect <mode>`.

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

Supported introspection modes:

- `schemas`
- `tables`
- `columns`
- `indexes`
- `rowcounts`
- `rowcounts-exact`

## Agent Guidelines

- Pass a valid named target with `--target`.
- Use exactly one of `--sql`, `--sql-file`, or `--introspect`.
- Prefer read targets unless write operations are explicitly requested.
- Return relevant query output to the user.
