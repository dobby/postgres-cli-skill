# Postgres CLI Setup (V2)

## 1. Create config directory

```bash
mkdir -p .agents/postgres-cli
```

## 2. Install `psql`

Homebrew (macOS):

```bash
brew install libpq
```

Add `psql` to PATH (Apple Silicon):

```bash
echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

Intel macOS:

```bash
echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

Linux package managers should install `postgresql-client`/`libpq` equivalents.

Verify:

```bash
command -v psql
psql --version
```

## 3. Create `.agents/postgres-cli/postgres.toml`

Assuming the skill is vendored into `.agents/skills/postgres-cli`:

From repository root:

```bash
cp .agents/skills/postgres-cli/references/postgres.toml.example .agents/postgres-cli/postgres.toml
```

The CLI resolves config from `.agents/postgres-cli/postgres.toml`.
Legacy fallback: if missing, it also checks `.agent/postgres-cli/postgres.toml`.

## 4. Configure secrets with env vars

Preferred location:

```bash
cat > .agents/postgres-cli/.env <<'EOFVARS'
PGPASSWORD_APP=your-password
# Optional DSN mode:
# DATABASE_URL_APP=postgres://user:pass@127.0.0.1:5432/app
EOFVARS
```

Secrets policy:

- Allowed: `password_env`, `dsn_env`
- Disallowed: plaintext `password`, plaintext `dsn`

## 5. Validate config and connectivity

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo config validate
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read doctor
```

If the consuming repo adds a repo-root wrapper, agents should use:

```bash
scripts/postgres-cli --project-root /path/to/repo config validate
```

## Agent safety policy

- Agents must run database operations through the installed skill launcher.
- Canonical installed path: `.agents/skills/postgres-cli/scripts/postgres-cli`
- If the repo provides a root wrapper, prefer `scripts/postgres-cli`.
- Agents must not execute `psql` directly.
- Agents must not read `.agents/.agent` `postgres.toml` or `.env` files directly.
- Target resolution for agents:
  - If user supplies a target, pass `--target <name>`.
  - If user does not supply a target, omit `--target` and rely on `default_target`.
  - If command returns `TARGET_MISSING`, ask user for a target name.

## 6. Run queries and introspection

Read query:

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read query --sql "SELECT now();"
```

Write query (explicit):

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-write query --mode write --sql "UPDATE users SET active = true WHERE id = 1;"
```

Introspect tables:

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read introspect --kind tables
```

## 7. Build schema cache for progressive agent context

Configured `important_tables` (+ direct FK neighbors):

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update
```

All tables in `current_schemas(false)`:

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update --all-tables
```

Generate optional markdown too:

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update --with-markdown
```

Generated artifacts (JSON-first):

```text
.agents/postgres-cli/schema/
├── index.json
├── relations.json
└── tables/
    └── <table>.json
```

Optional markdown artifacts when `--with-markdown` is enabled:

```text
.agents/postgres-cli/schema/
├── README.md
├── relations.md
└── tables/
    └── <table>.md
```

## Troubleshooting

- `CONFIG_NOT_FOUND`: create `.agents/postgres-cli/postgres.toml`.
- `PSQL_NOT_FOUND`: install `psql` or set `psql_bin` in config.
- `TARGET_UNKNOWN`: use `targets list` to discover configured targets.
- `TARGET_WRITE_DISABLED`: run on a target with `allow_write = true` for `--mode write`.
- `CONFIG_VALIDATION_FAILED`: run `config validate` and fix failed checks.
