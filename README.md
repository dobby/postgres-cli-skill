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
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read query --sql "SELECT now();"
```

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read introspect --kind tables
```

```bash
skills/postgres-cli/scripts/postgres-cli --project-root /path/to/repo --target local-read schema-cache update --all-tables
```

## Output formats

Global `--format` supports:

- `json` (default)
- `text`
- `csv`
- `tsv`

`csv`/`tsv` are available for tabular commands only.

## Schema cache layout (JSON-first)

```text
.agent/postgres-cli/schema/
├── index.json
├── relations.json
└── tables/
    └── <table>.json (or <schema>.<table>.json)
```

Optional markdown (with `schema-cache update --with-markdown`):

```text
.agent/postgres-cli/schema/
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
