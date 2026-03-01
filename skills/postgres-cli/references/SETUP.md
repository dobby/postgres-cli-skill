# Postgres CLI Setup (V2)

## 1. Create config directory

```bash
mkdir -p .agent/postgres-cli
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

## 3. Create `.agent/postgres-cli/postgres.toml`

From repository root:

```bash
cp skills/postgres-cli/references/postgres.toml.example .agent/postgres-cli/postgres.toml
```

The CLI resolves config from `.agent/postgres-cli/postgres.toml`.

## 4. Configure secrets with env vars

Preferred location:

```bash
cat > .agent/postgres-cli/.env <<'EOFVARS'
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
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo config validate
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read doctor
```

## 6. Run queries and introspection

Read query:

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read query --sql "SELECT now();"
```

Write query (explicit):

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-write query --mode write --sql "UPDATE users SET active = true WHERE id = 1;"
```

Introspect tables:

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read introspect --kind tables
```

## 7. Build schema cache for progressive agent context

Configured `important_tables` (+ direct FK neighbors):

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update
```

All tables in `current_schemas(false)`:

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update --all-tables
```

Generate optional markdown too:

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update --with-markdown
```

Generated artifacts (JSON-first):

```text
.agent/postgres-cli/schema/
├── index.json
├── relations.json
└── tables/
    └── <table>.json
```

Optional markdown artifacts when `--with-markdown` is enabled:

```text
.agent/postgres-cli/schema/
├── README.md
├── relations.md
└── tables/
    └── <table>.md
```

## Troubleshooting

- `CONFIG_NOT_FOUND`: create `.agent/postgres-cli/postgres.toml`.
- `PSQL_NOT_FOUND`: install `psql` or set `psql_bin` in config.
- `TARGET_UNKNOWN`: use `targets list` to discover configured targets.
- `TARGET_WRITE_DISABLED`: run on a target with `allow_write = true` for `--mode write`.
- `CONFIG_VALIDATION_FAILED`: run `config validate` and fix failed checks.
