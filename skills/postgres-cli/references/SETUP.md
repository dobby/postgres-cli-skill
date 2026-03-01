# Postgres CLI Setup

## 1. Create config directory

```bash
mkdir -p .agent/postgres-cli
```

## 2. Install `psql` (Homebrew preferred)

Install:

```bash
brew install libpq
```

Add `psql` to PATH:

```bash
echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

For Intel Macs, use:

```bash
echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

Verify:

```bash
command -v psql
psql --version
```

## 3. Create `.agent/postgres-cli/postgres.toml`

Start from the bundled example:

From this repository root:

```bash
cp skills/postgres-cli/references/postgres.toml.example .agent/postgres-cli/postgres.toml
```

From inside the installed skill directory (`.agents/skills/postgres-cli`):

```bash
cp references/postgres.toml.example /path/to/your-repo/.agent/postgres-cli/postgres.toml
```

`postgres-cli` resolves config from `.agent/postgres-cli/` only. Keep any additional CLI-only config files in this same directory.

Or create it manually:

- Use array form for `schema` to set multiple entries in `search_path`.
- Top-level `schema` applies to all targets by default.
- `connections.<name>.schema` overrides top-level `schema` for that target.

```toml
default_target = "webshop"
schema = ["bellimmo", "public"]
statement_timeout_ms = 30000
connect_timeout_s = 10
# Optional: only set if PATH does not already include psql
# psql_bin = "/absolute/path/to/psql"

[connections.webshop]
host = "127.0.0.1"
port = 5432
database = "webshop"
username = "webshop"
password_env = "PGPASSWORD_WEBSHOP"
application_name = "my-app"
sslmode = "prefer"
schema = ["bellimmo", "public"]
allow_write = false
```

## 4. Configure password environment variable

Option A (shell):

```bash
export PGPASSWORD_WEBSHOP='your-password'
```

Option B (`.agent/.env`):

```bash
cat > .agent/postgres-cli/.env <<'EOF'
PGPASSWORD_WEBSHOP=your-password
EOF
```

## 5. Run queries

From repo root:

```bash
skills/postgres-cli/scripts/postgres-cli --target webshop --sql "SELECT now();"
```

From any directory:

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target webshop --sql "SELECT now();"
```
