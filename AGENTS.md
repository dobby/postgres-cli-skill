# Postgres CLI Module Guide

## Purpose
This file explains how the Rust code is split so agent-driven edits can stay focused and token-efficient.

## Module map
- `src/main.rs`
  - Owns CLI surface (`clap` args/subcommands), config loading, output envelopes, command dispatch, and non-schema-cache command handlers (`query`, `explain`, `introspect`, `targets`, `config validate`, `doctor`).
  - Owns shared runtime primitives used across modules (`AppError`, SQL runner, formatting helpers, write guards).
- `src/schema_metadata.rs`
  - Owns schema-cache metadata collection logic.
  - Runs batched table metadata queries (columns, PKs, FKs, indexes).
  - Resolves direct FK relation expansion for selected important tables.
  - Returns normalized `TableSchemaDoc` objects to `main.rs` for artifact writing.

## Why this split
- High-churn schema SQL and cache behavior now live in one focused module.
- CLI behavior and output contracts stay centralized in `main.rs`.
- Future edits can usually target a single file instead of scanning the full CLI implementation.

## Extension rules
- Put new schema-cache data-fetch logic in `src/schema_metadata.rs`.
- Keep user-facing CLI contracts and exit code mapping in `src/main.rs`.
- If another domain grows large (for example config validation or output rendering), create a dedicated `src/<domain>.rs` module and keep interfaces narrow.

## Release and Skill Versioning
- `skills/postgres-cli/SKILL.md` must include a `version` field in the frontmatter.
- Every GitHub push that changes the skill behavior, command examples, setup flow, or safety rules must increment that `version`.
- Every release must create and push a matching Git tag (for example `v2.1.0`) so CI release workflows are triggered from the tag.
- Use semantic versioning (`MAJOR.MINOR.PATCH`):
  - `MAJOR`: breaking changes in workflow/contract.
  - `MINOR`: additive improvements and new documented capabilities.
  - `PATCH`: doc clarifications and non-breaking fixes.
