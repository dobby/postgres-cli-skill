# postgres-cli (V2)

`postgres-cli` is a Postgres command runner for agent and CI workflows.

V2 is a clean-break release with:

- subcommands (`query`, `explain`, `introspect`, `schema-cache`, `targets`, `config`, `doctor`)
- JSON output by default with a stable envelope
- explicit write safety (`query --mode write` + `allow_write=true`)
- JSON-first schema cache artifacts for progressive context loading
- tri-platform skill launcher support (macOS arm64, Linux x86_64, Windows x86_64)

## Quick usage

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read query --sql "SELECT now();"
```

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read introspect --kind tables
```

```bash
.agents/skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update --all-tables
```

## Agent safety

- Agents must use the installed skill launcher for DB operations.
- Canonical installed path: `.agents/skills/postgres-cli/scripts/postgres-cli`.
- If the consuming repo provides a root wrapper, prefer `scripts/postgres-cli`.
- Agents must not run `psql` directly.
- Agents must not read `.agents/postgres-cli/postgres.toml` or `.env` files directly.

## Target resolution

- If the user provides a connection name, pass `--target <name>`.
- If `--target` is omitted, CLI falls back to `default_target`.
- If no target is provided and no `default_target` exists, CLI returns `TARGET_MISSING`.

## Output formats

Global `--format` supports:

- `json` (default)
- `text`
- `csv`
- `tsv`

`csv`/`tsv` are available for tabular commands only.

Config, dotenv, and schema-cache artifacts are now stored under `.agents/postgres-cli/`.

## Schema cache layout (JSON-first)

```text
.agents/postgres-cli/schema/
├── index.json
├── relations.json
└── tables/
    └── <table>.json (or <schema>.<table>.json)
```

Optional markdown (with `schema-cache update --with-markdown`):

```text
.agents/postgres-cli/schema/
├── README.md
├── relations.md
└── tables/
    └── <table>.md
```

## Repository layout

- `src/main.rs` V2 Rust CLI implementation
- `skills/postgres-cli/SKILL.md` skill instructions for agents
- `skills/postgres-cli/scripts/postgres-cli` platform launcher script
- `skills/postgres-cli/scripts/bin/` prebuilt binaries
- `skills/postgres-cli/scripts/build-release-binary.sh` local maintainer build helper
- `skills/postgres-cli/scripts/refresh-binaries-from-release.sh` maintainer release refresh helper
- `skills/postgres-cli/references/postgres.toml.example` starter config
- `skills/postgres-cli/references/SETUP.md` setup and usage guide
- `.github/workflows/build-release.yml` CI + release pipeline
