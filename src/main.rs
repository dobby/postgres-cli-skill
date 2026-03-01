use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

const CONFIG_DIR_NAME: &str = "postgres-cli";
const QUERY_FIELD_DELIM: &str = "\u{1f}";
const HELP_SENTINEL: &str = "__HELP__";

#[derive(Debug, Deserialize)]
struct Config {
    default_target: Option<String>,
    schema: Option<SchemaValue>,
    #[allow(dead_code)]
    schema_path: Option<String>,
    statement_timeout_ms: Option<u64>,
    connect_timeout_s: Option<u64>,
    psql_bin: Option<String>,
    important_tables: Option<Vec<String>>,
    connections: BTreeMap<String, Connection>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SchemaValue {
    Single(String),
    Multi(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct Connection {
    host: Option<String>,
    port: Option<u16>,
    database: String,
    username: String,
    password_env: Option<String>,
    application_name: Option<String>,
    sslmode: Option<String>,
    schema: Option<SchemaValue>,
    allow_write: Option<bool>,
    statement_timeout_ms: Option<u64>,
    connect_timeout_s: Option<u64>,
}

#[derive(Debug)]
struct CliArgs {
    project_root: PathBuf,
    target: Option<String>,
    sql: Option<String>,
    sql_file: Option<PathBuf>,
    introspect: Option<String>,
    schema_cache_action: Option<String>,
    all_tables: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize)]
struct TableRef {
    schema: String,
    table: String,
}

impl TableRef {
    fn fq_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    fn file_name(&self) -> String {
        format!("{}.{}.md", self.schema, self.table)
    }
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    ordinal_position: usize,
    column_name: String,
    data_type: String,
    is_nullable: bool,
    column_default: Option<String>,
}

#[derive(Debug, Clone)]
struct OutboundFk {
    constraint_name: String,
    from_column: String,
    to_schema: String,
    to_table: String,
    to_column: String,
}

#[derive(Debug, Clone)]
struct InboundFk {
    from_schema: String,
    from_table: String,
    constraint_name: String,
    from_column: String,
    to_column: String,
}

#[derive(Debug, Clone)]
struct IndexInfo {
    index_name: String,
    index_def: String,
}

#[derive(Debug, Clone)]
struct TableSchemaDoc {
    table: TableRef,
    columns: Vec<ColumnInfo>,
    primary_key_columns: Vec<String>,
    outbound_fks: Vec<OutboundFk>,
    inbound_fks: Vec<InboundFk>,
    indexes: Vec<IndexInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize)]
struct RelationEdge {
    constraint_name: String,
    from_table: String,
    from_column: String,
    to_table: String,
    to_column: String,
}

#[derive(Debug, Serialize)]
struct SchemaIndex {
    target: String,
    mode: String,
    generated_at: u64,
    table_count: usize,
    relation_count: usize,
    tables: Vec<SchemaIndexTable>,
}

#[derive(Debug, Serialize)]
struct SchemaIndexTable {
    schema: String,
    table: String,
    file: String,
}

fn parse_args() -> Result<CliArgs, String> {
    let mut project_root = PathBuf::from(env::current_dir().map_err(|e| e.to_string())?);
    let mut target = None;
    let mut sql = None;
    let mut sql_file = None;
    let mut introspect = None;
    let mut schema_cache_action = None;
    let mut all_tables = false;

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--project-root" => {
                i += 1;
                let v = args.get(i).ok_or("--project-root requires a value")?;
                project_root = PathBuf::from(v);
            }
            "--target" => {
                i += 1;
                target = Some(args.get(i).ok_or("--target requires a value")?.clone());
            }
            "--sql" => {
                i += 1;
                sql = Some(args.get(i).ok_or("--sql requires a value")?.clone());
            }
            "--sql-file" => {
                i += 1;
                sql_file = Some(PathBuf::from(
                    args.get(i).ok_or("--sql-file requires a value")?,
                ));
            }
            "--introspect" => {
                i += 1;
                introspect = Some(args.get(i).ok_or("--introspect requires a value")?.clone());
            }
            "--schema-cache" => {
                i += 1;
                schema_cache_action = Some(
                    args.get(i)
                        .ok_or("--schema-cache requires a value")?
                        .clone(),
                );
            }
            "--all-tables" => {
                all_tables = true;
            }
            "-h" | "--help" => {
                print_help();
                return Err(HELP_SENTINEL.to_string());
            }
            other => {
                return Err(format!("Unknown argument: {other}"));
            }
        }
        i += 1;
    }

    Ok(CliArgs {
        project_root,
        target,
        sql,
        sql_file,
        introspect,
        schema_cache_action,
        all_tables,
    })
}

fn print_help() {
    println!(
        "postgres-cli\n\
Usage:\n\
  postgres-cli --project-root <repo> --target <name> --sql \"SELECT 1;\"\n\
  postgres-cli --project-root <repo> --target <name> --introspect tables\n\
  postgres-cli --project-root <repo> --target <name> --schema-cache update\n\
  postgres-cli --project-root <repo> --target <name> --schema-cache update --all-tables\n\
\nFlags:\n\
  --project-root <path>   Project root containing .agent/postgres-cli/postgres.toml (default: cwd)\n\
  --target <name>         Named connection in config (optional if default_target set)\n\
  --sql <statement>       SQL to execute\n\
  --sql-file <file>       SQL file to execute\n\
  --introspect <name>     One of: schemas,tables,columns,indexes,rowcounts,rowcounts-exact\n\
  --schema-cache <name>   Schema cache action. Supported: update\n\
  --all-tables            With --schema-cache update, include all tables in current_schemas(false)"
    );
}

fn query_mode_count(args: &CliArgs) -> usize {
    usize::from(args.sql.is_some())
        + usize::from(args.sql_file.is_some())
        + usize::from(args.introspect.is_some())
}

fn validate_cli_args(args: &CliArgs) -> Result<(), String> {
    let query_modes = query_mode_count(args);

    if let Some(action) = args.schema_cache_action.as_deref() {
        if query_modes > 0 {
            return Err(
                "--schema-cache is mutually exclusive with --sql, --sql-file, and --introspect"
                    .to_string(),
            );
        }
        if action != "update" {
            return Err("Invalid --schema-cache value. Use update".to_string());
        }
        return Ok(());
    }

    if args.all_tables {
        return Err("--all-tables requires --schema-cache update".to_string());
    }

    if query_modes != 1 {
        return Err("Provide exactly one of --sql, --sql-file, or --introspect".to_string());
    }

    Ok(())
}

fn preferred_config_dir(project_root: &Path) -> PathBuf {
    project_root.join(".agent").join(CONFIG_DIR_NAME)
}

fn preferred_config_path(project_root: &Path) -> PathBuf {
    preferred_config_dir(project_root).join("postgres.toml")
}

fn load_config(project_root: &Path) -> Result<Config, String> {
    let config_path = preferred_config_path(project_root);
    let content = fs::read_to_string(&config_path)
        .map_err(|_| format!("Missing or unreadable config: {}", config_path.display()))?;
    toml::from_str::<Config>(&content).map_err(|e| format!("Invalid TOML config: {e}"))
}

fn load_dotenv(project_root: &Path) -> Result<(), String> {
    let preferred_env = preferred_config_dir(project_root).join(".env");
    let root_env = project_root.join(".env");

    // Load `.agent/postgres-cli/.env` first so postgres-cli-specific values are preferred.
    if preferred_env.exists() {
        load_dotenv_file(&preferred_env)?;
    }
    if root_env.exists() {
        load_dotenv_file(&root_env)?;
    }

    Ok(())
}

fn load_dotenv_file(path: &Path) -> Result<(), String> {
    let content = fs::read_to_string(path)
        .map_err(|_| format!("Unreadable .env file: {}", path.display()))?;

    for (idx, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let assignment = if let Some(rest) = line.strip_prefix("export ") {
            rest.trim_start()
        } else {
            line
        };

        let Some((key_raw, value_raw)) = assignment.split_once('=') else {
            return Err(format!(
                "Invalid .env entry at {}:{} (expected KEY=VALUE)",
                path.display(),
                idx + 1
            ));
        };

        let key = key_raw.trim();
        if !is_valid_env_key(key) {
            return Err(format!(
                "Invalid .env key '{}' at {}:{}",
                key,
                path.display(),
                idx + 1
            ));
        }

        if env::var_os(key).is_some() {
            continue;
        }

        let value = parse_env_value(value_raw.trim());
        env::set_var(key, value);
    }

    Ok(())
}

fn is_valid_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if first != '_' && !first.is_ascii_alphabetic() {
        return false;
    }

    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn parse_env_value(raw: &str) -> String {
    if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
        return unescape_double_quoted(&raw[1..raw.len() - 1]);
    }

    if raw.len() >= 2 && raw.starts_with('\'') && raw.ends_with('\'') {
        return raw[1..raw.len() - 1].to_string();
    }

    raw.to_string()
}

fn unescape_double_quoted(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut escaped = false;

    for c in raw.chars() {
        if escaped {
            match c {
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                '\\' => out.push('\\'),
                '"' => out.push('"'),
                '$' => out.push('$'),
                other => out.push(other),
            }
            escaped = false;
            continue;
        }

        if c == '\\' {
            escaped = true;
        } else {
            out.push(c);
        }
    }

    if escaped {
        out.push('\\');
    }

    out
}

fn resolve_psql(config: &Config) -> Result<String, String> {
    if let Some(configured) = &config.psql_bin {
        let p = Path::new(configured);
        if p.exists() {
            return Ok(configured.clone());
        }
    }

    if let Some(path) = find_in_path("psql") {
        return Ok(path);
    }

    let known_locations = [
        "/opt/homebrew/bin/psql",
        "/opt/homebrew/Cellar/libpq/18.3/bin/psql",
        "/opt/homebrew/Cellar/postgresql@18/18.3/bin/psql",
        "/usr/local/bin/psql",
        "/Applications/Postgres.app/Contents/Versions/latest/bin/psql",
    ];

    for location in known_locations {
        if Path::new(location).exists() {
            return Ok(location.to_string());
        }
    }

    Err(
        "psql not found; install libpq/postgresql or set psql_bin in .agent/postgres-cli/postgres.toml"
            .to_string(),
    )
}

fn find_in_path(bin: &str) -> Option<String> {
    let paths = env::var_os("PATH")?;
    for p in env::split_paths(&paths) {
        let candidate = p.join(bin);
        if candidate.exists() {
            return Some(candidate.to_string_lossy().to_string());
        }
    }
    None
}

fn normalize_search_path(schema: Option<&SchemaValue>) -> Result<Option<String>, String> {
    let Some(schema) = schema else {
        return Ok(None);
    };

    let parts: Vec<String> = match schema {
        SchemaValue::Single(v) => v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        SchemaValue::Multi(items) => items
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    };

    if parts.is_empty() {
        return Ok(None);
    }

    if parts.iter().any(|p| p.contains('\0')) {
        return Err("schema contains invalid null byte".to_string());
    }

    let escaped: Vec<String> = parts
        .iter()
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect();

    Ok(Some(escaped.join(", ")))
}

fn read_sql(args: &CliArgs) -> Result<String, String> {
    let count = query_mode_count(args);
    if count != 1 {
        return Err("Provide exactly one of --sql, --sql-file, or --introspect".to_string());
    }

    if let Some(sql) = &args.sql {
        return Ok(sql.clone());
    }

    if let Some(file) = &args.sql_file {
        return fs::read_to_string(file)
            .map_err(|_| format!("SQL file not found or unreadable: {}", file.display()));
    }

    let intro = args.introspect.as_ref().expect("validated");
    match intro.as_str() {
        "schemas" => Ok(
            "SELECT nspname AS schema_name\nFROM pg_namespace\nWHERE nspname NOT IN ('pg_catalog', 'information_schema')\nORDER BY 1;".to_string(),
        ),
        "tables" => Ok(
            "SELECT table_schema, table_name\nFROM information_schema.tables\nWHERE table_schema = ANY(current_schemas(false))\n  AND table_type = 'BASE TABLE'\nORDER BY table_schema, table_name;".to_string(),
        ),
        "columns" => Ok(
            "SELECT table_schema, table_name, ordinal_position, column_name, data_type\nFROM information_schema.columns\nWHERE table_schema = ANY(current_schemas(false))\nORDER BY table_schema, table_name, ordinal_position;".to_string(),
        ),
        "indexes" => Ok(
            "SELECT schemaname, tablename, indexname, indexdef\nFROM pg_indexes\nWHERE schemaname = ANY(current_schemas(false))\nORDER BY schemaname, tablename, indexname;".to_string(),
        ),
        "rowcounts" => Ok(
            "SELECT n.nspname AS schema_name,\n       c.relname AS table_name,\n       c.reltuples::bigint AS estimated_rows\nFROM pg_class c\nJOIN pg_namespace n ON n.oid = c.relnamespace\nWHERE c.relkind = 'r'\n  AND n.nspname = ANY(current_schemas(false))\nORDER BY n.nspname, c.relname;".to_string(),
        ),
        "rowcounts-exact" => Ok("__ROWCOUNT_EXACT__".to_string()),
        _ => Err("Invalid --introspect value. Use schemas|tables|columns|indexes|rowcounts|rowcounts-exact".to_string()),
    }
}

fn is_write_sql(sql: &str) -> bool {
    let trimmed = strip_leading_comments(sql);
    let re = Regex::new(
        r"(?i)\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COMMENT|VACUUM|ANALYZE|REINDEX|REFRESH|CALL|DO|COPY\s+[^\(])\b",
    )
    .expect("valid regex");
    re.is_match(trimmed)
}

fn strip_leading_comments(sql: &str) -> &str {
    let mut s = sql;
    loop {
        let trimmed = s.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            if let Some(pos) = rest.find('\n') {
                s = &rest[pos + 1..];
                continue;
            }
            return "";
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            if let Some(pos) = rest.find("*/") {
                s = &rest[pos + 2..];
                continue;
            }
            return "";
        }
        return trimmed;
    }
}

fn apply_connection_env(
    command: &mut Command,
    conn: &Connection,
    config: &Config,
) -> Result<(), String> {
    if let Some(password_env) = &conn.password_env {
        let Some(password) = env::var_os(password_env) else {
            return Err(format!(
                "Missing required env var for password: {password_env}"
            ));
        };
        command.env("PGPASSWORD", password);
    }

    if let Some(appname) = &conn.application_name {
        command.env("PGAPPNAME", appname);
    }

    if let Some(sslmode) = &conn.sslmode {
        command.env("PGSSLMODE", sslmode);
    }

    let connect_timeout = conn
        .connect_timeout_s
        .or(config.connect_timeout_s)
        .unwrap_or(10);
    command.env("PGCONNECT_TIMEOUT", connect_timeout.to_string());

    let statement_timeout = conn
        .statement_timeout_ms
        .or(config.statement_timeout_ms)
        .unwrap_or(30_000);

    let existing_pgoptions = env::var("PGOPTIONS").unwrap_or_default();
    let timeout_opt = format!("-c statement_timeout={statement_timeout}");
    let final_pgoptions = if existing_pgoptions.trim().is_empty() {
        timeout_opt
    } else {
        format!("{} {}", existing_pgoptions.trim(), timeout_opt)
    };
    command.env("PGOPTIONS", final_pgoptions);

    Ok(())
}

fn psql_base_command(psql_bin: &str, conn: &Connection) -> Command {
    let mut cmd = Command::new(psql_bin);
    cmd.arg("-X")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-P")
        .arg("pager=off")
        .arg("-U")
        .arg(&conn.username)
        .arg("-d")
        .arg(&conn.database);

    if let Some(host) = &conn.host {
        cmd.arg("-h").arg(host);
    }
    if let Some(port) = conn.port {
        cmd.arg("-p").arg(port.to_string());
    }

    cmd
}

fn apply_search_path(search_path: Option<&str>, sql: &str) -> String {
    match search_path {
        Some(sp) => format!("SET search_path TO {sp};\n{sql}"),
        None => sql.to_string(),
    }
}

fn run_query_rows(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let final_sql = apply_search_path(search_path, sql);
    let mut cmd = psql_base_command(psql_bin, conn);
    apply_connection_env(&mut cmd, conn, config)?;

    let out = cmd
        .arg("-A")
        .arg("-t")
        .arg("-F")
        .arg(QUERY_FIELD_DELIM)
        .arg("-c")
        .arg(final_sql)
        .output()
        .map_err(|e| e.to_string())?;

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "psql query failed (code={}): {}",
            out.status.code().unwrap_or(1),
            stderr.trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut rows = Vec::new();
    for raw in stdout.lines() {
        let line = raw.trim_end();
        if line.is_empty() {
            continue;
        }
        let cols = line
            .split(QUERY_FIELD_DELIM)
            .map(|c| c.to_string())
            .collect::<Vec<_>>();
        rows.push(cols);
    }

    Ok(rows)
}

fn run_sql(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    sql: &str,
) -> Result<i32, String> {
    let final_sql = apply_search_path(search_path, sql);

    let mut cmd = psql_base_command(psql_bin, conn);
    apply_connection_env(&mut cmd, conn, config)?;
    let status = cmd
        .arg("-c")
        .arg(final_sql)
        .status()
        .map_err(|e| e.to_string())?;
    Ok(status.code().unwrap_or(1))
}

fn run_exact_rowcounts(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
) -> Result<i32, String> {
    let base_list_sql = "SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)\nFROM information_schema.tables\nWHERE table_schema = ANY(current_schemas(false))\n  AND table_type = 'BASE TABLE'\nORDER BY table_schema, table_name;";
    let list_sql = apply_search_path(search_path, base_list_sql);

    let mut list_cmd = psql_base_command(psql_bin, conn);
    apply_connection_env(&mut list_cmd, conn, config)?;

    let out = list_cmd
        .arg("-A")
        .arg("-t")
        .arg("-c")
        .arg(list_sql)
        .output()
        .map_err(|e| e.to_string())?;

    if !out.status.success() {
        eprint!("{}", String::from_utf8_lossy(&out.stderr));
        return Ok(out.status.code().unwrap_or(1));
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    let tables: Vec<String> = stdout
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
        .collect();

    if tables.is_empty() {
        println!("No tables found in current search_path.");
        return Ok(0);
    }

    println!("table_name|exact_rows");
    for table in tables {
        let count_sql =
            format!("SELECT '{table}' AS table_name, COUNT(*)::bigint AS exact_rows FROM {table};");
        let mut count_cmd = psql_base_command(psql_bin, conn);
        apply_connection_env(&mut count_cmd, conn, config)?;

        let out = count_cmd
            .arg("-A")
            .arg("-t")
            .arg("-F")
            .arg("|")
            .arg("-c")
            .arg(count_sql)
            .output()
            .map_err(|e| e.to_string())?;

        if !out.status.success() {
            eprint!("{}", String::from_utf8_lossy(&out.stderr));
            return Ok(out.status.code().unwrap_or(1));
        }

        print!("{}", String::from_utf8_lossy(&out.stdout));
    }

    Ok(0)
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn list_available_tables(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
) -> Result<Vec<TableRef>, String> {
    let sql = "SELECT table_schema, table_name\nFROM information_schema.tables\nWHERE table_schema = ANY(current_schemas(false))\n  AND table_type = 'BASE TABLE'\nORDER BY table_schema, table_name;";

    let rows = run_query_rows(psql_bin, conn, config, search_path, sql)?;
    let mut out = Vec::new();
    for row in rows {
        if row.len() < 2 {
            continue;
        }
        out.push(TableRef {
            schema: row[0].trim().to_string(),
            table: row[1].trim().to_string(),
        });
    }
    Ok(out)
}

fn list_direct_table_relations(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
) -> Result<Vec<(TableRef, TableRef)>, String> {
    let sql = "SELECT tc.table_schema, tc.table_name, ccu.table_schema, ccu.table_name\n\
FROM information_schema.table_constraints tc\n\
JOIN information_schema.constraint_column_usage ccu\n\
  ON ccu.constraint_name = tc.constraint_name\n\
 AND ccu.constraint_schema = tc.constraint_schema\n\
WHERE tc.constraint_type = 'FOREIGN KEY'\n\
  AND tc.table_schema = ANY(current_schemas(false))\n\
  AND ccu.table_schema = ANY(current_schemas(false))\n\
ORDER BY tc.table_schema, tc.table_name, ccu.table_schema, ccu.table_name;";

    let rows = run_query_rows(psql_bin, conn, config, search_path, sql)?;
    let mut out = Vec::new();
    for row in rows {
        if row.len() < 4 {
            continue;
        }
        out.push((
            TableRef {
                schema: row[0].trim().to_string(),
                table: row[1].trim().to_string(),
            },
            TableRef {
                schema: row[2].trim().to_string(),
                table: row[3].trim().to_string(),
            },
        ));
    }
    Ok(out)
}

fn expand_with_directly_related_tables(
    selected_tables: Vec<TableRef>,
    relation_pairs: &[(TableRef, TableRef)],
) -> Vec<TableRef> {
    let mut selected = selected_tables.into_iter().collect::<BTreeSet<_>>();
    let snapshot = selected.clone();

    for (from_table, to_table) in relation_pairs {
        if snapshot.contains(from_table) || snapshot.contains(to_table) {
            selected.insert(from_table.clone());
            selected.insert(to_table.clone());
        }
    }

    selected.into_iter().collect()
}

fn resolve_selected_tables(
    available_tables: &[TableRef],
    config: &Config,
    all_tables: bool,
) -> Result<Vec<TableRef>, String> {
    if all_tables {
        return Ok(available_tables.to_vec());
    }

    let requested = config
        .important_tables
        .as_ref()
        .ok_or("Missing top-level important_tables in .agent/postgres-cli/postgres.toml")?;

    if requested.is_empty() {
        return Err(
            "important_tables is empty; add at least one table or use --all-tables".to_string(),
        );
    }

    let available_set: BTreeSet<TableRef> = available_tables.iter().cloned().collect();
    let mut selected = BTreeSet::new();
    let mut errors = Vec::new();

    for raw in requested {
        let name = raw.trim();
        if name.is_empty() {
            continue;
        }

        if let Some((schema, table)) = name.split_once('.') {
            let schema = schema.trim();
            let table = table.trim();
            if schema.is_empty() || table.is_empty() {
                errors.push(format!(
                    "Invalid important_tables entry '{name}'. Use schema.table format."
                ));
                continue;
            }

            let candidate = TableRef {
                schema: schema.to_string(),
                table: table.to_string(),
            };

            if available_set.contains(&candidate) {
                selected.insert(candidate);
            } else {
                errors.push(format!(
                    "important_tables entry '{name}' not found in current_schemas(false)"
                ));
            }
            continue;
        }

        let matches = available_tables
            .iter()
            .filter(|t| t.table == name)
            .cloned()
            .collect::<Vec<_>>();

        if matches.is_empty() {
            errors.push(format!(
                "important_tables entry '{name}' not found. Use schema-qualified names like schema.table"
            ));
        } else if matches.len() > 1 {
            let candidates = matches
                .iter()
                .map(TableRef::fq_name)
                .collect::<Vec<_>>()
                .join(", ");
            errors.push(format!(
                "important_tables entry '{name}' is ambiguous. Candidates: {candidates}"
            ));
        } else {
            selected.insert(matches[0].clone());
        }
    }

    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }

    Ok(selected.into_iter().collect())
}

fn fetch_columns(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    table: &TableRef,
) -> Result<Vec<ColumnInfo>, String> {
    let sql = format!(
        "SELECT ordinal_position, column_name, data_type, is_nullable, COALESCE(column_default, '')\nFROM information_schema.columns\nWHERE table_schema = {}\n  AND table_name = {}\nORDER BY ordinal_position;",
        sql_literal(&table.schema),
        sql_literal(&table.table)
    );

    let rows = run_query_rows(psql_bin, conn, config, search_path, &sql)?;
    let mut cols = Vec::new();
    for row in rows {
        if row.len() < 5 {
            continue;
        }
        let ordinal_position = row[0].parse::<usize>().unwrap_or(0);
        let column_default = if row[4].trim().is_empty() {
            None
        } else {
            Some(row[4].clone())
        };
        cols.push(ColumnInfo {
            ordinal_position,
            column_name: row[1].clone(),
            data_type: row[2].clone(),
            is_nullable: row[3].eq_ignore_ascii_case("YES"),
            column_default,
        });
    }
    Ok(cols)
}

fn fetch_primary_key_columns(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    table: &TableRef,
) -> Result<Vec<String>, String> {
    let sql = format!(
        "SELECT kcu.column_name\nFROM information_schema.table_constraints tc\nJOIN information_schema.key_column_usage kcu\n  ON tc.constraint_name = kcu.constraint_name\n AND tc.constraint_schema = kcu.constraint_schema\n AND tc.table_name = kcu.table_name\nWHERE tc.constraint_type = 'PRIMARY KEY'\n  AND tc.table_schema = {}\n  AND tc.table_name = {}\nORDER BY kcu.ordinal_position;",
        sql_literal(&table.schema),
        sql_literal(&table.table)
    );

    let rows = run_query_rows(psql_bin, conn, config, search_path, &sql)?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.first().cloned())
        .collect::<Vec<_>>())
}

fn fetch_outbound_fks(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    table: &TableRef,
) -> Result<Vec<OutboundFk>, String> {
    let sql = format!(
        "SELECT tc.constraint_name, kcu.column_name, ccu.table_schema, ccu.table_name, ccu.column_name\nFROM information_schema.table_constraints tc\nJOIN information_schema.key_column_usage kcu\n  ON tc.constraint_name = kcu.constraint_name\n AND tc.constraint_schema = kcu.constraint_schema\n AND tc.table_name = kcu.table_name\nJOIN information_schema.constraint_column_usage ccu\n  ON ccu.constraint_name = tc.constraint_name\n AND ccu.constraint_schema = tc.constraint_schema\nWHERE tc.constraint_type = 'FOREIGN KEY'\n  AND tc.table_schema = {}\n  AND tc.table_name = {}\nORDER BY tc.constraint_name, kcu.ordinal_position;",
        sql_literal(&table.schema),
        sql_literal(&table.table)
    );

    let rows = run_query_rows(psql_bin, conn, config, search_path, &sql)?;
    let mut out = Vec::new();
    for row in rows {
        if row.len() < 5 {
            continue;
        }
        out.push(OutboundFk {
            constraint_name: row[0].clone(),
            from_column: row[1].clone(),
            to_schema: row[2].clone(),
            to_table: row[3].clone(),
            to_column: row[4].clone(),
        });
    }
    Ok(out)
}

fn fetch_inbound_fks(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    table: &TableRef,
) -> Result<Vec<InboundFk>, String> {
    let sql = format!(
        "SELECT tc.table_schema, tc.table_name, tc.constraint_name, kcu.column_name, ccu.column_name\nFROM information_schema.table_constraints tc\nJOIN information_schema.key_column_usage kcu\n  ON tc.constraint_name = kcu.constraint_name\n AND tc.constraint_schema = kcu.constraint_schema\n AND tc.table_name = kcu.table_name\nJOIN information_schema.constraint_column_usage ccu\n  ON ccu.constraint_name = tc.constraint_name\n AND ccu.constraint_schema = tc.constraint_schema\nWHERE tc.constraint_type = 'FOREIGN KEY'\n  AND ccu.table_schema = {}\n  AND ccu.table_name = {}\nORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;",
        sql_literal(&table.schema),
        sql_literal(&table.table)
    );

    let rows = run_query_rows(psql_bin, conn, config, search_path, &sql)?;
    let mut out = Vec::new();
    for row in rows {
        if row.len() < 5 {
            continue;
        }
        out.push(InboundFk {
            from_schema: row[0].clone(),
            from_table: row[1].clone(),
            constraint_name: row[2].clone(),
            from_column: row[3].clone(),
            to_column: row[4].clone(),
        });
    }
    Ok(out)
}

fn fetch_indexes(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    table: &TableRef,
) -> Result<Vec<IndexInfo>, String> {
    let sql = format!(
        "SELECT indexname, indexdef\nFROM pg_indexes\nWHERE schemaname = {}\n  AND tablename = {}\nORDER BY indexname;",
        sql_literal(&table.schema),
        sql_literal(&table.table)
    );

    let rows = run_query_rows(psql_bin, conn, config, search_path, &sql)?;
    let mut out = Vec::new();
    for row in rows {
        if row.len() < 2 {
            continue;
        }
        out.push(IndexInfo {
            index_name: row[0].clone(),
            index_def: row[1].clone(),
        });
    }
    Ok(out)
}

fn fetch_table_schema_doc(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    table: &TableRef,
) -> Result<TableSchemaDoc, String> {
    Ok(TableSchemaDoc {
        table: table.clone(),
        columns: fetch_columns(psql_bin, conn, config, search_path, table)?,
        primary_key_columns: fetch_primary_key_columns(psql_bin, conn, config, search_path, table)?,
        outbound_fks: fetch_outbound_fks(psql_bin, conn, config, search_path, table)?,
        inbound_fks: fetch_inbound_fks(psql_bin, conn, config, search_path, table)?,
        indexes: fetch_indexes(psql_bin, conn, config, search_path, table)?,
    })
}

fn markdown_escape(value: &str) -> String {
    value
        .replace('|', "\\|")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn render_table_markdown(doc: &TableSchemaDoc, generated_at: u64, target: &str) -> String {
    let mut out = String::new();
    out.push_str(&format!("# {}\n\n", doc.table.fq_name()));
    out.push_str(&format!("- Target: `{}`\n", target));
    out.push_str(&format!("- Generated at (unix): `{}`\n\n", generated_at));

    out.push_str("## Columns\n\n");
    if doc.columns.is_empty() {
        out.push_str("No columns found.\n\n");
    } else {
        out.push_str("| # | Column | Type | Nullable | Default |\n");
        out.push_str("|---:|---|---|:---:|---|\n");
        for col in &doc.columns {
            let default_val = col
                .column_default
                .as_deref()
                .map(markdown_escape)
                .unwrap_or_else(|| "".to_string());
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                col.ordinal_position,
                markdown_escape(&col.column_name),
                markdown_escape(&col.data_type),
                if col.is_nullable { "YES" } else { "NO" },
                default_val
            ));
        }
        out.push('\n');
    }

    out.push_str("## Primary Key\n\n");
    if doc.primary_key_columns.is_empty() {
        out.push_str("None\n\n");
    } else {
        for c in &doc.primary_key_columns {
            out.push_str(&format!("- `{}`\n", c));
        }
        out.push('\n');
    }

    out.push_str("## Foreign Keys (Outbound)\n\n");
    if doc.outbound_fks.is_empty() {
        out.push_str("None\n\n");
    } else {
        for fk in &doc.outbound_fks {
            out.push_str(&format!(
                "- `{}`: `{}` -> `{}.{}.{}`\n",
                fk.constraint_name, fk.from_column, fk.to_schema, fk.to_table, fk.to_column
            ));
        }
        out.push('\n');
    }

    out.push_str("## Foreign Keys (Inbound)\n\n");
    if doc.inbound_fks.is_empty() {
        out.push_str("None\n\n");
    } else {
        for fk in &doc.inbound_fks {
            out.push_str(&format!(
                "- `{}`: `{}.{}.{}` -> `{}`\n",
                fk.constraint_name, fk.from_schema, fk.from_table, fk.from_column, fk.to_column
            ));
        }
        out.push('\n');
    }

    out.push_str("## Indexes\n\n");
    if doc.indexes.is_empty() {
        out.push_str("None\n");
    } else {
        for idx in &doc.indexes {
            out.push_str(&format!("- `{}`\n", idx.index_name));
            out.push_str("```sql\n");
            out.push_str(&idx.index_def);
            out.push_str("\n```\n\n");
        }
    }

    out
}

fn render_relations_markdown(edges: &[RelationEdge], generated_at: u64, target: &str) -> String {
    let mut out = String::new();
    out.push_str("# Relations\n\n");
    out.push_str(&format!("- Target: `{}`\n", target));
    out.push_str(&format!("- Generated at (unix): `{}`\n", generated_at));
    out.push_str(&format!("- Relation count: `{}`\n\n", edges.len()));

    if edges.is_empty() {
        out.push_str("No direct foreign-key relations found.\n");
        return out;
    }

    for edge in edges {
        out.push_str(&format!(
            "- `{}`: `{}.{}` -> `{}.{}`\n",
            edge.constraint_name, edge.from_table, edge.from_column, edge.to_table, edge.to_column
        ));
    }

    out
}

fn render_schema_readme(index: &SchemaIndex) -> String {
    let mut out = String::new();
    out.push_str("# postgres-cli schema cache\n\n");
    out.push_str(&format!("- Target: `{}`\n", index.target));
    out.push_str(&format!("- Mode: `{}`\n", index.mode));
    out.push_str(&format!(
        "- Generated at (unix): `{}`\n",
        index.generated_at
    ));
    out.push_str(&format!("- Tables: `{}`\n", index.table_count));
    out.push_str(&format!("- Relations: `{}`\n\n", index.relation_count));

    out.push_str("## Progressive loading\n\n");
    out.push_str("1. Read `index.json` first for a compact overview.\n");
    out.push_str("2. Load only needed files under `tables/`.\n");
    out.push_str("3. Read `relations.md` only when join paths are needed.\n\n");

    if index.tables.is_empty() {
        out.push_str("No tables were selected for this snapshot.\n");
    }

    out
}

fn write_text(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::write(path, content).map_err(|e| e.to_string())
}

fn write_schema_snapshot(
    project_root: &Path,
    index: &SchemaIndex,
    docs: &[TableSchemaDoc],
    edges: &[RelationEdge],
) -> Result<(), String> {
    let config_dir = preferred_config_dir(project_root);
    fs::create_dir_all(&config_dir).map_err(|e| e.to_string())?;

    let schema_root = config_dir.join("schema");
    let tmp_dir = config_dir.join(format!(
        "schema.tmp-{}-{}",
        index.generated_at,
        std::process::id()
    ));

    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir).map_err(|e| e.to_string())?;
    }

    fs::create_dir_all(tmp_dir.join("tables")).map_err(|e| e.to_string())?;

    let index_json = serde_json::to_string_pretty(index).map_err(|e| e.to_string())?;
    write_text(&tmp_dir.join("index.json"), &index_json)?;
    write_text(&tmp_dir.join("README.md"), &render_schema_readme(index))?;
    write_text(
        &tmp_dir.join("relations.md"),
        &render_relations_markdown(edges, index.generated_at, &index.target),
    )?;

    for doc in docs {
        let table_path = tmp_dir.join("tables").join(doc.table.file_name());
        let content = render_table_markdown(doc, index.generated_at, &index.target);
        write_text(&table_path, &content)?;
    }

    if schema_root.exists() {
        fs::remove_dir_all(&schema_root).map_err(|e| e.to_string())?;
    }
    fs::rename(&tmp_dir, &schema_root).map_err(|e| e.to_string())?;

    Ok(())
}

fn run_schema_cache_update(
    args: &CliArgs,
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    target: &str,
) -> Result<i32, String> {
    if !args.all_tables {
        let important = config
            .important_tables
            .as_ref()
            .ok_or("Missing top-level important_tables in .agent/postgres-cli/postgres.toml")?;
        if important.is_empty() {
            return Err(
                "important_tables is empty; add at least one table or use --all-tables".to_string(),
            );
        }
    }

    let available_tables = list_available_tables(psql_bin, conn, config, search_path)?;
    let relation_pairs = list_direct_table_relations(psql_bin, conn, config, search_path)?;

    let mut selected_tables = resolve_selected_tables(&available_tables, config, args.all_tables)?;
    if !args.all_tables {
        selected_tables = expand_with_directly_related_tables(selected_tables, &relation_pairs);
    }

    let mut docs = Vec::new();
    for table in &selected_tables {
        docs.push(fetch_table_schema_doc(
            psql_bin,
            conn,
            config,
            search_path,
            table,
        )?);
    }

    let mut edge_set = BTreeSet::new();
    for doc in &docs {
        for fk in &doc.outbound_fks {
            edge_set.insert(RelationEdge {
                constraint_name: fk.constraint_name.clone(),
                from_table: doc.table.fq_name(),
                from_column: fk.from_column.clone(),
                to_table: format!("{}.{}", fk.to_schema, fk.to_table),
                to_column: fk.to_column.clone(),
            });
        }
    }
    let edges: Vec<RelationEdge> = edge_set.into_iter().collect();

    let generated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs();

    let index_tables = selected_tables
        .iter()
        .map(|t| SchemaIndexTable {
            schema: t.schema.clone(),
            table: t.table.clone(),
            file: format!("tables/{}", t.file_name()),
        })
        .collect::<Vec<_>>();

    let mode = if args.all_tables {
        "all_tables".to_string()
    } else {
        "important".to_string()
    };

    let index = SchemaIndex {
        target: target.to_string(),
        mode,
        generated_at,
        table_count: docs.len(),
        relation_count: edges.len(),
        tables: index_tables,
    };

    write_schema_snapshot(&args.project_root, &index, &docs, &edges)?;

    println!(
        "Schema cache updated: target={}, mode={}, tables={}, relations={}",
        index.target, index.mode, index.table_count, index.relation_count
    );
    println!(
        "Wrote snapshot to {}",
        preferred_config_dir(&args.project_root)
            .join("schema")
            .display()
    );

    Ok(0)
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code as u8),
        Err(e) => {
            if !e.is_empty() {
                eprintln!("Error: {e}");
            }
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<i32, String> {
    let args = match parse_args() {
        Ok(args) => args,
        Err(e) if e == HELP_SENTINEL => return Ok(0),
        Err(e) => return Err(e),
    };
    validate_cli_args(&args)?;

    load_dotenv(&args.project_root)?;
    let config = load_config(&args.project_root)?;
    let psql_bin = resolve_psql(&config)?;

    let target = args
        .target
        .clone()
        .or_else(|| config.default_target.clone())
        .ok_or("No target specified and no default_target configured")?;

    let conn = config.connections.get(&target).ok_or_else(|| {
        let names = config
            .connections
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        format!("Unknown target '{target}'. Available: {names}")
    })?;

    let search_path = normalize_search_path(conn.schema.as_ref().or(config.schema.as_ref()))?;

    if args.schema_cache_action.as_deref() == Some("update") {
        return run_schema_cache_update(
            &args,
            &psql_bin,
            conn,
            &config,
            search_path.as_deref(),
            &target,
        );
    }

    let sql = read_sql(&args)?;
    let write_intent = sql != "__ROWCOUNT_EXACT__" && is_write_sql(&sql);
    if write_intent && !conn.allow_write.unwrap_or(false) {
        return Err(format!(
            "Target '{target}' is read-only (allow_write=false). Use a write-enabled configured target for DDL/DML statements."
        ));
    }

    let code = if sql == "__ROWCOUNT_EXACT__" {
        run_exact_rowcounts(&psql_bin, conn, &config, search_path.as_deref())?
    } else {
        run_sql(&psql_bin, conn, &config, search_path.as_deref(), &sql)?
    };

    if code == 0 {
        println!("\nSummary: target={target}, query_executed=ok");
    }

    Ok(code)
}
