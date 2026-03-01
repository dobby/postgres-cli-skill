use regex::Regex;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

#[derive(Debug, Deserialize)]
struct Config {
    default_target: Option<String>,
    schema: Option<SchemaValue>,
    #[allow(dead_code)]
    schema_path: Option<String>,
    statement_timeout_ms: Option<u64>,
    connect_timeout_s: Option<u64>,
    psql_bin: Option<String>,
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
}

fn parse_args() -> Result<CliArgs, String> {
    let mut project_root = PathBuf::from(env::current_dir().map_err(|e| e.to_string())?);
    let mut target = None;
    let mut sql = None;
    let mut sql_file = None;
    let mut introspect = None;

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
                sql_file = Some(PathBuf::from(args.get(i).ok_or("--sql-file requires a value")?));
            }
            "--introspect" => {
                i += 1;
                introspect = Some(args.get(i).ok_or("--introspect requires a value")?.clone());
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
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
    })
}

fn print_help() {
    println!(
        "postgres-cli\n\
Usage:\n\
  postgres-cli --project-root <repo> --target <name> --sql \"SELECT 1;\"\n\
  postgres-cli --project-root <repo> --target <name> --introspect tables\n\
\nFlags:\n\
  --project-root <path>  Project root containing .agent/postgres.toml (default: cwd)\n\
  --target <name>        Named connection in config (optional if default_target set)\n\
  --sql <statement>      SQL to execute\n\
  --sql-file <file>      SQL file to execute\n\
  --introspect <name>    One of: schemas,tables,columns,indexes,rowcounts,rowcounts-exact"
    );
}

fn load_config(project_root: &Path) -> Result<Config, String> {
    let config_path = project_root.join(".agent").join("postgres.toml");
    let content = fs::read_to_string(&config_path)
        .map_err(|_| format!("Missing or unreadable config: {}", config_path.display()))?;
    toml::from_str::<Config>(&content).map_err(|e| format!("Invalid TOML config: {e}"))
}

fn load_dotenv(project_root: &Path) -> Result<(), String> {
    let agent_env = project_root.join(".agent").join(".env");
    let root_env = project_root.join(".env");

    // Load `.agent/.env` first so values near postgres.toml are preferred.
    if agent_env.exists() {
        load_dotenv_file(&agent_env)?;
    }
    if root_env.exists() {
        load_dotenv_file(&root_env)?;
    }

    Ok(())
}

fn load_dotenv_file(path: &Path) -> Result<(), String> {
    let content =
        fs::read_to_string(path).map_err(|_| format!("Unreadable .env file: {}", path.display()))?;

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
    if raw.len() >= 2 && raw.starts_with('\"') && raw.ends_with('\"') {
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

    Err("psql not found; install libpq/postgresql or set psql_bin in .agent/postgres.toml".to_string())
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

fn normalize_search_path(schema: &Option<SchemaValue>) -> Result<Option<String>, String> {
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
    let mut count = 0u8;
    if args.sql.is_some() {
        count += 1;
    }
    if args.sql_file.is_some() {
        count += 1;
    }
    if args.introspect.is_some() {
        count += 1;
    }
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

    let intro = args.introspect.as_ref().expect("checked above");
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

fn apply_connection_env(command: &mut Command, conn: &Connection, config: &Config) -> Result<(), String> {
    if let Some(password_env) = &conn.password_env {
        let Some(password) = env::var_os(password_env) else {
            return Err(format!("Missing required env var for password: {password_env}"));
        };
        command.env("PGPASSWORD", password);
    }

    if let Some(appname) = &conn.application_name {
        command.env("PGAPPNAME", appname);
    }

    if let Some(sslmode) = &conn.sslmode {
        command.env("PGSSLMODE", sslmode);
    }

    let connect_timeout = conn.connect_timeout_s.or(config.connect_timeout_s).unwrap_or(10);
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

fn run_sql(psql_bin: &str, conn: &Connection, config: &Config, search_path: Option<&str>, sql: &str) -> Result<i32, String> {
    let final_sql = match search_path {
        Some(sp) => format!("SET search_path TO {sp};\n{sql}"),
        None => sql.to_string(),
    };

    let mut cmd = psql_base_command(psql_bin, conn);
    apply_connection_env(&mut cmd, conn, config)?;
    let status = cmd.arg("-c").arg(final_sql).status().map_err(|e| e.to_string())?;
    Ok(status.code().unwrap_or(1))
}

fn run_exact_rowcounts(psql_bin: &str, conn: &Connection, config: &Config, search_path: Option<&str>) -> Result<i32, String> {
    let base_list_sql = "SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)\nFROM information_schema.tables\nWHERE table_schema = ANY(current_schemas(false))\n  AND table_type = 'BASE TABLE'\nORDER BY table_schema, table_name;";
    let list_sql = match search_path {
        Some(sp) => format!("SET search_path TO {sp};\n{base_list_sql}"),
        None => base_list_sql.to_string(),
    };

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
        let count_sql = format!("SELECT '{table}' AS table_name, COUNT(*)::bigint AS exact_rows FROM {table};");
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
    let args = parse_args()?;
    load_dotenv(&args.project_root)?;
    let config = load_config(&args.project_root)?;
    let psql_bin = resolve_psql(&config)?;

    let target = args
        .target
        .clone()
        .or_else(|| config.default_target.clone())
        .ok_or("No target specified and no default_target configured")?;

    let conn = config
        .connections
        .get(&target)
        .ok_or_else(|| {
            let names = config.connections.keys().cloned().collect::<Vec<_>>().join(", ");
            format!("Unknown target '{target}'. Available: {names}")
        })?;

    let sql = read_sql(&args)?;
    let write_intent = sql != "__ROWCOUNT_EXACT__" && is_write_sql(&sql);
    if write_intent && !conn.allow_write.unwrap_or(false) {
        return Err(format!(
            "Target '{target}' is read-only (allow_write=false). Use a write-enabled configured target for DDL/DML statements."
        ));
    }

    let search_path = normalize_search_path(&config.schema)?;

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
