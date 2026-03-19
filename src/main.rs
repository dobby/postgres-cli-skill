use clap::{Args, Parser, Subcommand, ValueEnum};
use csv::{ReaderBuilder, WriterBuilder};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

mod schema_metadata;

const CONFIG_DIR_NAME: &str = "postgres-cli";
const PREFERRED_AGENT_DIR_NAME: &str = ".agents";
const VERSION: &str = "2.3.1";

#[derive(Debug, Clone, Copy)]
enum ExitKind {
    Cli = 2,
    Policy = 3,
    Db = 4,
    Runtime = 5,
}

#[derive(Debug)]
struct AppError {
    exit: ExitKind,
    code: &'static str,
    message: String,
    details: Value,
}

impl AppError {
    fn cli(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            exit: ExitKind::Cli,
            code,
            message: message.into(),
            details: json!({}),
        }
    }

    fn policy(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            exit: ExitKind::Policy,
            code,
            message: message.into(),
            details: json!({}),
        }
    }

    fn db(message: impl Into<String>) -> Self {
        Self {
            exit: ExitKind::Db,
            code: "DB_EXECUTION_FAILED",
            message: message.into(),
            details: json!({}),
        }
    }

    fn runtime(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            exit: ExitKind::Runtime,
            code,
            message: message.into(),
            details: json!({}),
        }
    }

    fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    config_version: Option<u32>,
    default_target: Option<String>,
    schema: Option<SchemaValue>,
    #[allow(dead_code)]
    schema_path: Option<String>,
    schema_cache: Option<SchemaCacheConfig>,
    statement_timeout_ms: Option<u64>,
    connect_timeout_s: Option<u64>,
    psql_bin: Option<String>,
    important_tables: Option<Vec<String>>,
    connections: BTreeMap<String, Connection>,
}

#[derive(Debug, Deserialize)]
struct SchemaCacheConfig {
    file_naming: Option<SchemaCacheFileNaming>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SchemaCacheFileNaming {
    Table,
    SchemaTable,
}

#[derive(Debug, Clone, ValueEnum)]
enum TableFileNamingArg {
    Table,
    SchemaTable,
}

impl From<TableFileNamingArg> for SchemaCacheFileNaming {
    fn from(value: TableFileNamingArg) -> Self {
        match value {
            TableFileNamingArg::Table => SchemaCacheFileNaming::Table,
            TableFileNamingArg::SchemaTable => SchemaCacheFileNaming::SchemaTable,
        }
    }
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
    database: Option<String>,
    username: Option<String>,
    password_env: Option<String>,
    dsn_env: Option<String>,
    password: Option<String>,
    dsn: Option<String>,
    application_name: Option<String>,
    sslmode: Option<String>,
    schema: Option<SchemaValue>,
    allow_write: Option<bool>,
    statement_timeout_ms: Option<u64>,
    connect_timeout_s: Option<u64>,
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

    fn display_name(&self, file_naming: SchemaCacheFileNaming) -> String {
        match file_naming {
            SchemaCacheFileNaming::Table => self.table.clone(),
            SchemaCacheFileNaming::SchemaTable => self.fq_name(),
        }
    }

    fn file_stem(&self, file_naming: SchemaCacheFileNaming) -> String {
        match file_naming {
            SchemaCacheFileNaming::Table => self.table.clone(),
            SchemaCacheFileNaming::SchemaTable => format!("{}.{}", self.schema, self.table),
        }
    }

    fn json_file(&self, file_naming: SchemaCacheFileNaming) -> String {
        format!("{}.json", self.file_stem(file_naming))
    }

    fn markdown_file(&self, file_naming: SchemaCacheFileNaming) -> String {
        format!("{}.md", self.file_stem(file_naming))
    }
}

#[derive(Debug, Clone, Serialize)]
struct ColumnInfo {
    ordinal_position: usize,
    column_name: String,
    data_type: String,
    is_nullable: bool,
    column_default: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct OutboundFk {
    constraint_name: String,
    from_column: String,
    to_schema: String,
    to_table: String,
    to_column: String,
}

#[derive(Debug, Clone, Serialize)]
struct InboundFk {
    from_schema: String,
    from_table: String,
    constraint_name: String,
    from_column: String,
    to_column: String,
}

#[derive(Debug, Clone, Serialize)]
struct IndexInfo {
    index_name: String,
    index_def: String,
}

#[derive(Debug, Clone, Serialize)]
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
    from_schema: String,
    from_table: String,
    from_column: String,
    to_schema: String,
    to_table: String,
    to_column: String,
}

#[derive(Debug, Serialize)]
struct SchemaIndex {
    version: &'static str,
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
    json_file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    markdown_file: Option<String>,
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    version: &'static str,
    ok: bool,
    command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    error: ErrorPayload,
}

#[derive(Debug, Serialize)]
struct ErrorPayload {
    code: String,
    message: String,
    details: Value,
}

#[derive(Debug, Serialize)]
struct SuccessEnvelope {
    version: &'static str,
    ok: bool,
    command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    data: Value,
    meta: ResponseMeta,
}

#[derive(Debug, Default, Serialize)]
struct ResponseMeta {
    duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct TableData {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Debug)]
struct CommandOutcome {
    command: String,
    target: Option<String>,
    data: Value,
    table: Option<TableData>,
    meta: ResponseMeta,
    summary: Vec<String>,
}

#[derive(Debug)]
struct GlobalContext {
    format: OutputFormat,
    output: Option<PathBuf>,
    no_summary: bool,
}

#[derive(Debug)]
struct QueryExecution {
    table: Option<TableData>,
    raw_stdout: String,
    statement_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    Json,
    Text,
    Csv,
    Tsv,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "lower")]
enum QueryMode {
    Read,
    Write,
}

#[derive(Debug, Clone, ValueEnum)]
enum IntrospectKind {
    Schemas,
    Tables,
    Columns,
    Indexes,
    Constraints,
    Views,
    MaterializedViews,
    Functions,
    Triggers,
    Enums,
    Rowcounts,
    #[value(alias = "rowcounts-exact", alias = "rowcounts_exact")]
    RowcountsExact,
}

#[derive(Debug, Parser)]
#[command(
    name = "postgres-cli",
    version,
    about = "Postgres CLI V2 for agent and CI workflows"
)]
struct Cli {
    #[arg(long, global = true)]
    project_root: Option<PathBuf>,
    #[arg(long, global = true)]
    target: Option<String>,
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,
    #[arg(long, global = true)]
    output: Option<PathBuf>,
    #[arg(long, global = true, default_value_t = false)]
    no_summary: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Query(QueryArgs),
    Explain(ExplainArgs),
    Introspect(IntrospectArgs),
    SchemaCache {
        #[command(subcommand)]
        command: SchemaCacheCommand,
    },
    Targets {
        #[command(subcommand)]
        command: TargetsCommand,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Doctor(DoctorArgs),
}

#[derive(Debug, Args)]
struct SqlInput {
    #[arg(long)]
    sql: Option<String>,
    #[arg(long)]
    sql_file: Option<PathBuf>,
    #[arg(long = "stdin", default_value_t = false)]
    from_stdin: bool,
}

impl SqlInput {
    fn read_sql(&self) -> Result<String, AppError> {
        let modes = usize::from(self.sql.is_some())
            + usize::from(self.sql_file.is_some())
            + usize::from(self.from_stdin);

        if modes != 1 {
            return Err(AppError::cli(
                "INVALID_SQL_INPUT",
                "Provide exactly one of --sql, --sql-file, or --stdin",
            ));
        }

        if let Some(sql) = &self.sql {
            return Ok(sql.clone());
        }

        if let Some(path) = &self.sql_file {
            return fs::read_to_string(path).map_err(|_| {
                AppError::cli(
                    "SQL_FILE_UNREADABLE",
                    format!("SQL file not found or unreadable: {}", path.display()),
                )
            });
        }

        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| AppError::runtime("STDIN_READ_FAILED", e.to_string()))?;
        Ok(buf)
    }
}

#[derive(Debug, Args)]
struct QueryArgs {
    #[command(flatten)]
    input: SqlInput,
    #[arg(long, value_enum, default_value_t = QueryMode::Read)]
    mode: QueryMode,
    #[arg(long)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Args)]
struct ExplainArgs {
    #[command(flatten)]
    input: SqlInput,
    #[arg(long, default_value_t = false)]
    analyze: bool,
    #[arg(long, default_value_t = false)]
    verbose: bool,
    #[arg(long, default_value_t = false)]
    buffers: bool,
    #[arg(long, default_value_t = false)]
    settings: bool,
    #[arg(long, default_value_t = false)]
    wal: bool,
    #[arg(long)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Args)]
struct IntrospectArgs {
    #[arg(long, value_enum)]
    kind: IntrospectKind,
    #[arg(long)]
    schema: Vec<String>,
    #[arg(long)]
    table: Vec<String>,
    #[arg(long)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Subcommand)]
enum SchemaCacheCommand {
    Update(SchemaCacheUpdateArgs),
}

#[derive(Debug, Args)]
struct SchemaCacheUpdateArgs {
    #[arg(long, default_value_t = false)]
    all_tables: bool,
    #[arg(long, default_value_t = false)]
    with_markdown: bool,
    #[arg(long, value_enum)]
    table_file_naming: Option<TableFileNamingArg>,
    #[arg(long)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Subcommand)]
enum TargetsCommand {
    List,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    Validate,
}

#[derive(Debug, Args)]
struct DoctorArgs {
    #[arg(long)]
    timeout_ms: Option<u64>,
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            use clap::error::ErrorKind;
            let should_exit_zero = matches!(
                err.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            );
            if should_exit_zero {
                print!("{err}");
                std::process::exit(0);
            }
            eprintln!("{err}");
            std::process::exit(ExitKind::Cli as i32);
        }
    };

    let global = GlobalContext {
        format: cli.format,
        output: cli.output.clone(),
        no_summary: cli.no_summary,
    };

    match run(&cli) {
        Ok(outcome) => {
            if let Err(err) = emit_success(&outcome, &global) {
                eprintln!("Error: {}", err.message);
                std::process::exit(err.exit as i32);
            }
            std::process::exit(0);
        }
        Err(err) => {
            if let Err(output_err) = emit_error(&cli, &err, &global) {
                eprintln!("Error: {}", output_err.message);
                std::process::exit(output_err.exit as i32);
            }
            std::process::exit(err.exit as i32);
        }
    }
}

fn run(cli: &Cli) -> Result<CommandOutcome, AppError> {
    let start = Instant::now();
    let project_root = cli.project_root.clone().unwrap_or(
        env::current_dir().map_err(|e| AppError::runtime("CWD_RESOLVE_FAILED", e.to_string()))?,
    );

    load_dotenv(&project_root)?;

    let config = load_config(&project_root)?;

    let mut outcome = match &cli.command {
        Commands::Query(args) => run_query(&project_root, &config, cli.target.as_deref(), args)?,
        Commands::Explain(args) => {
            run_explain(&project_root, &config, cli.target.as_deref(), args)?
        }
        Commands::Introspect(args) => {
            run_introspect(&project_root, &config, cli.target.as_deref(), args)?
        }
        Commands::SchemaCache {
            command: SchemaCacheCommand::Update(args),
        } => run_schema_cache_update(&project_root, &config, cli.target.as_deref(), args)?,
        Commands::Targets {
            command: TargetsCommand::List,
        } => run_targets_list(&config)?,
        Commands::Config {
            command: ConfigCommand::Validate,
        } => run_config_validate(&config, cli.target.as_deref())?,
        Commands::Doctor(args) => run_doctor(&project_root, &config, cli.target.as_deref(), args)?,
    };

    outcome.meta.duration_ms = start.elapsed().as_millis();
    Ok(outcome)
}

fn emit_success(outcome: &CommandOutcome, global: &GlobalContext) -> Result<(), AppError> {
    let body = match global.format {
        OutputFormat::Json => serde_json::to_string_pretty(&SuccessEnvelope {
            version: VERSION,
            ok: true,
            command: outcome.command.clone(),
            target: outcome.target.clone(),
            data: outcome.data.clone(),
            meta: ResponseMeta {
                duration_ms: outcome.meta.duration_ms,
                row_count: outcome.meta.row_count,
                statement_timeout_ms: outcome.meta.statement_timeout_ms,
            },
        })
        .map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))?,
        OutputFormat::Text => render_text_output(outcome, global.no_summary),
        OutputFormat::Csv => render_delimited_output(outcome, b',')?,
        OutputFormat::Tsv => render_delimited_output(outcome, b'\t')?,
    };

    write_output(&body, global.output.as_ref())
}

fn emit_error(cli: &Cli, err: &AppError, global: &GlobalContext) -> Result<(), AppError> {
    if matches!(global.format, OutputFormat::Json) {
        let command = match &cli.command {
            Commands::Query(_) => "query",
            Commands::Explain(_) => "explain",
            Commands::Introspect(_) => "introspect",
            Commands::SchemaCache { .. } => "schema-cache",
            Commands::Targets { .. } => "targets",
            Commands::Config { .. } => "config",
            Commands::Doctor(_) => "doctor",
        }
        .to_string();

        let envelope = ErrorEnvelope {
            version: VERSION,
            ok: false,
            command,
            target: cli.target.clone(),
            error: ErrorPayload {
                code: err.code.to_string(),
                message: err.message.clone(),
                details: err.details.clone(),
            },
        };

        let body = serde_json::to_string_pretty(&envelope)
            .map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))?;
        return write_output(&body, global.output.as_ref());
    }

    let mut body = format!("Error [{}]: {}", err.code, err.message);
    if err.details != json!({}) {
        let details = serde_json::to_string_pretty(&err.details)
            .map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))?;
        body.push_str("\n");
        body.push_str(&details);
    }
    write_output(&body, global.output.as_ref())
}

fn write_output(content: &str, output_path: Option<&PathBuf>) -> Result<(), AppError> {
    if let Some(path) = output_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| AppError::runtime("OUTPUT_WRITE_FAILED", e.to_string()))?;
        }
        fs::write(path, format!("{}\n", content))
            .map_err(|e| AppError::runtime("OUTPUT_WRITE_FAILED", e.to_string()))?;
        return Ok(());
    }

    let mut stdout = io::stdout();
    stdout
        .write_all(content.as_bytes())
        .map_err(|e| AppError::runtime("OUTPUT_WRITE_FAILED", e.to_string()))?;
    stdout
        .write_all(b"\n")
        .map_err(|e| AppError::runtime("OUTPUT_WRITE_FAILED", e.to_string()))?;
    Ok(())
}

fn render_text_output(outcome: &CommandOutcome, no_summary: bool) -> String {
    let mut out = String::new();

    if let Some(table) = &outcome.table {
        out.push_str(&render_table_text(table));
    } else {
        out.push_str(
            &serde_json::to_string_pretty(&outcome.data).unwrap_or_else(|_| "{}".to_string()),
        );
    }

    if !no_summary && !outcome.summary.is_empty() {
        out.push_str("\n\nSummary:\n");
        for line in &outcome.summary {
            out.push_str("- ");
            out.push_str(line);
            out.push('\n');
        }
    }

    out
}

fn render_delimited_output(outcome: &CommandOutcome, delimiter: u8) -> Result<String, AppError> {
    let Some(table) = &outcome.table else {
        return Err(AppError::cli(
            "FORMAT_NOT_SUPPORTED",
            "CSV/TSV output is only supported for tabular command results",
        ));
    };

    let mut writer = WriterBuilder::new()
        .delimiter(delimiter)
        .from_writer(vec![]);

    writer
        .write_record(&table.columns)
        .map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))?;
    for row in &table.rows {
        writer
            .write_record(row)
            .map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))?;
    }

    let bytes = writer
        .into_inner()
        .map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))?;
    String::from_utf8(bytes).map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))
}

fn render_table_text(table: &TableData) -> String {
    if table.columns.is_empty() {
        return "(no columns)".to_string();
    }

    let mut out = String::new();
    out.push_str(&table.columns.join(" | "));
    out.push('\n');
    let separator = table
        .columns
        .iter()
        .map(|_| "---")
        .collect::<Vec<_>>()
        .join(" | ");
    out.push_str(&separator);
    out.push('\n');

    for row in &table.rows {
        out.push_str(&row.join(" | "));
        out.push('\n');
    }

    out.trim_end().to_string()
}

fn preferred_config_dir(project_root: &Path) -> PathBuf {
    project_root
        .join(PREFERRED_AGENT_DIR_NAME)
        .join(CONFIG_DIR_NAME)
}

fn preferred_config_path(project_root: &Path) -> PathBuf {
    preferred_config_dir(project_root).join("postgres.toml")
}

fn load_config(project_root: &Path) -> Result<Config, AppError> {
    let preferred_path = preferred_config_path(project_root);
    let content = fs::read_to_string(&preferred_path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            AppError::cli(
                "CONFIG_NOT_FOUND",
                format!("Missing config. Checked {}", preferred_path.display()),
            )
        } else {
            AppError::cli(
                "CONFIG_NOT_FOUND",
                format!("Missing or unreadable config: {}", preferred_path.display()),
            )
        }
    })?;

    toml::from_str::<Config>(&content)
        .map_err(|e| AppError::cli("CONFIG_INVALID_TOML", format!("Invalid TOML config: {e}")))
}

fn load_dotenv(project_root: &Path) -> Result<(), AppError> {
    let preferred_env = preferred_config_dir(project_root).join(".env");
    let root_env = project_root.join(".env");

    if preferred_env.exists() {
        load_dotenv_file(&preferred_env)?;
    }
    if root_env.exists() {
        load_dotenv_file(&root_env)?;
    }

    Ok(())
}

fn load_dotenv_file(path: &Path) -> Result<(), AppError> {
    let content = fs::read_to_string(path).map_err(|_| {
        AppError::runtime(
            "DOTENV_UNREADABLE",
            format!("Unreadable .env file: {}", path.display()),
        )
    })?;

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
            return Err(AppError::runtime(
                "DOTENV_INVALID",
                format!(
                    "Invalid .env entry at {}:{} (expected KEY=VALUE)",
                    path.display(),
                    idx + 1
                ),
            ));
        };

        let key = key_raw.trim();
        if !is_valid_env_key(key) {
            return Err(AppError::runtime(
                "DOTENV_INVALID_KEY",
                format!(
                    "Invalid .env key '{}' at {}:{}",
                    key,
                    path.display(),
                    idx + 1
                ),
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

fn resolve_psql(config: &Config) -> Result<String, AppError> {
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
        "/usr/local/bin/psql",
        "/Applications/Postgres.app/Contents/Versions/latest/bin/psql",
        "C:\\Program Files\\PostgreSQL\\16\\bin\\psql.exe",
    ];

    for location in known_locations {
        if Path::new(location).exists() {
            return Ok(location.to_string());
        }
    }

    Err(AppError::runtime(
        "PSQL_NOT_FOUND",
        "psql not found; install libpq/postgresql or set psql_bin in .agents/postgres-cli/postgres.toml",
    ))
}

fn find_in_path(bin: &str) -> Option<String> {
    let paths = env::var_os("PATH")?;
    for p in env::split_paths(&paths) {
        let candidate = p.join(bin);
        if candidate.exists() {
            return Some(candidate.to_string_lossy().to_string());
        }

        #[cfg(target_os = "windows")]
        {
            let candidate_exe = p.join(format!("{bin}.exe"));
            if candidate_exe.exists() {
                return Some(candidate_exe.to_string_lossy().to_string());
            }
        }
    }
    None
}

fn resolve_target<'a>(
    config: &'a Config,
    target: Option<&str>,
) -> Result<(String, &'a Connection), AppError> {
    let target_name = target
        .map(|t| t.to_string())
        .or_else(|| config.default_target.clone())
        .ok_or_else(|| {
            AppError::cli(
                "TARGET_MISSING",
                "No target specified and no default_target configured",
            )
        })?;

    let conn = config.connections.get(&target_name).ok_or_else(|| {
        let names = config
            .connections
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        AppError::cli(
            "TARGET_UNKNOWN",
            format!("Unknown target '{target_name}'. Available: {names}"),
        )
    })?;

    Ok((target_name, conn))
}

fn normalize_search_path(schema: Option<&SchemaValue>) -> Result<Option<String>, AppError> {
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
        return Err(AppError::cli(
            "SEARCH_PATH_INVALID",
            "schema contains invalid null byte",
        ));
    }

    let escaped: Vec<String> = parts
        .iter()
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect();

    Ok(Some(escaped.join(", ")))
}

fn resolve_connection_dsn(conn: &Connection) -> Result<Option<String>, AppError> {
    if conn.dsn.is_some() {
        return Err(AppError::cli(
            "CONFIG_SECRET_POLICY",
            "Plaintext dsn is not allowed. Use dsn_env instead.",
        ));
    }

    if let Some(dsn_env) = &conn.dsn_env {
        let dsn = env::var(dsn_env).map_err(|_| {
            AppError::runtime(
                "MISSING_DSN_ENV",
                format!("Missing required env var for DSN: {dsn_env}"),
            )
        })?;
        if dsn.trim().is_empty() {
            return Err(AppError::runtime(
                "MISSING_DSN_ENV",
                format!("DSN env var {dsn_env} is empty"),
            ));
        }
        return Ok(Some(dsn));
    }

    Ok(None)
}

fn apply_connection_env(
    command: &mut Command,
    conn: &Connection,
    config: &Config,
    timeout_ms_override: Option<u64>,
) -> Result<u64, AppError> {
    if conn.password.is_some() {
        return Err(AppError::cli(
            "CONFIG_SECRET_POLICY",
            "Plaintext password is not allowed. Use password_env instead.",
        ));
    }

    if let Some(password_env) = &conn.password_env {
        let password = env::var_os(password_env).ok_or_else(|| {
            AppError::runtime(
                "MISSING_PASSWORD_ENV",
                format!("Missing required env var for password: {password_env}"),
            )
        })?;
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

    let statement_timeout = timeout_ms_override
        .or(conn.statement_timeout_ms)
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

    Ok(statement_timeout)
}

fn psql_base_command(
    psql_bin: &str,
    conn: &Connection,
    dsn: Option<&str>,
) -> Result<Command, AppError> {
    let mut cmd = Command::new(psql_bin);
    cmd.arg("-X")
        .arg("-q")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-P")
        .arg("pager=off");

    if let Some(dsn_value) = dsn {
        cmd.arg(dsn_value);
        return Ok(cmd);
    }

    let username = conn.username.as_deref().ok_or_else(|| {
        AppError::cli(
            "CONFIG_INVALID_CONNECTION",
            "Connection is missing username",
        )
    })?;
    let database = conn.database.as_deref().ok_or_else(|| {
        AppError::cli(
            "CONFIG_INVALID_CONNECTION",
            "Connection is missing database",
        )
    })?;

    cmd.arg("-U").arg(username).arg("-d").arg(database);

    if let Some(host) = &conn.host {
        cmd.arg("-h").arg(host);
    }

    if let Some(port) = conn.port {
        cmd.arg("-p").arg(port.to_string());
    }

    Ok(cmd)
}

fn apply_search_path(search_path: Option<&str>, sql: &str) -> String {
    match search_path {
        Some(sp) => format!("SET search_path TO {sp};\n{sql}"),
        None => sql.to_string(),
    }
}

fn run_sql_capture_table(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    sql: &str,
    timeout_ms_override: Option<u64>,
) -> Result<QueryExecution, AppError> {
    let final_sql = apply_search_path(search_path, sql);
    let dsn = resolve_connection_dsn(conn)?;

    let mut cmd = psql_base_command(psql_bin, conn, dsn.as_deref())?;
    let statement_timeout_ms = apply_connection_env(&mut cmd, conn, config, timeout_ms_override)?;

    let output = cmd
        .arg("--csv")
        .arg("-c")
        .arg(final_sql)
        .output()
        .map_err(|e| AppError::runtime("PSQL_EXEC_FAILED", e.to_string()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let msg = if stderr.is_empty() {
            format!(
                "psql execution failed with code {}",
                output.status.code().unwrap_or(1)
            )
        } else {
            format!(
                "psql execution failed (code={}): {}",
                output.status.code().unwrap_or(1),
                stderr
            )
        };
        return Err(AppError::db(msg));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let table = parse_csv_table(&stdout);

    Ok(QueryExecution {
        table,
        raw_stdout: stdout,
        statement_timeout_ms,
    })
}

fn parse_csv_table(stdout: &str) -> Option<TableData> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(trimmed.as_bytes());

    let headers = reader
        .headers()
        .ok()?
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>();
    if headers.is_empty() {
        return None;
    }

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.ok()?;
        rows.push(record.iter().map(|v| v.to_string()).collect::<Vec<_>>());
    }

    if rows.is_empty()
        && headers.len() == 1
        && headers[0].contains(' ')
        && headers[0]
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == ' ')
    {
        return None;
    }

    Some(TableData {
        columns: headers,
        rows,
    })
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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

fn ensure_query_mode_allowed(
    mode: QueryMode,
    conn: &Connection,
    sql: &str,
) -> Result<(), AppError> {
    let allow_write = conn.allow_write.unwrap_or(false);
    match mode {
        QueryMode::Read => {
            if is_write_sql(sql) {
                return Err(AppError::policy(
                    "READ_MODE_BLOCKED",
                    "Mutating SQL is blocked in --mode read. Re-run with --mode write on a write-enabled target.",
                ));
            }
        }
        QueryMode::Write => {
            if !allow_write {
                return Err(AppError::policy(
                    "TARGET_WRITE_DISABLED",
                    "Target is not write-enabled (allow_write=false).",
                ));
            }
        }
    }

    Ok(())
}

fn run_query(
    _project_root: &Path,
    config: &Config,
    target: Option<&str>,
    args: &QueryArgs,
) -> Result<CommandOutcome, AppError> {
    let psql_bin = resolve_psql(config)?;
    let (target_name, conn) = resolve_target(config, target)?;
    let search_path = normalize_search_path(conn.schema.as_ref().or(config.schema.as_ref()))?;

    let sql = args.input.read_sql()?;
    ensure_query_mode_allowed(args.mode, conn, &sql)?;

    let execution = run_sql_capture_table(
        &psql_bin,
        conn,
        config,
        search_path.as_deref(),
        &sql,
        args.timeout_ms,
    )?;

    let row_count = execution.table.as_ref().map(|t| t.rows.len());
    let data = if let Some(table) = &execution.table {
        json!({
            "columns": table.columns,
            "rows": table.rows,
        })
    } else {
        json!({
            "stdout": execution.raw_stdout.trim(),
        })
    };

    Ok(CommandOutcome {
        command: "query".to_string(),
        target: Some(target_name.to_string()),
        data,
        table: execution.table,
        meta: ResponseMeta {
            duration_ms: 0,
            row_count,
            statement_timeout_ms: Some(execution.statement_timeout_ms),
        },
        summary: vec![
            format!("target={target_name}"),
            "query_executed=ok".to_string(),
            format!(
                "mode={}",
                match args.mode {
                    QueryMode::Read => "read",
                    QueryMode::Write => "write",
                }
            ),
        ],
    })
}

fn build_explain_sql(args: &ExplainArgs, sql: &str) -> String {
    let mut options = Vec::new();
    if args.analyze {
        options.push("ANALYZE true".to_string());
    }
    if args.verbose {
        options.push("VERBOSE true".to_string());
    }
    if args.buffers {
        options.push("BUFFERS true".to_string());
    }
    if args.settings {
        options.push("SETTINGS true".to_string());
    }
    if args.wal {
        options.push("WAL true".to_string());
    }

    if options.is_empty() {
        format!("EXPLAIN {sql}")
    } else {
        format!("EXPLAIN ({}) {sql}", options.join(", "))
    }
}

fn run_explain(
    _project_root: &Path,
    config: &Config,
    target: Option<&str>,
    args: &ExplainArgs,
) -> Result<CommandOutcome, AppError> {
    let psql_bin = resolve_psql(config)?;
    let (target_name, conn) = resolve_target(config, target)?;
    let search_path = normalize_search_path(conn.schema.as_ref().or(config.schema.as_ref()))?;

    let sql = args.input.read_sql()?;
    if args.analyze && is_write_sql(&sql) && !conn.allow_write.unwrap_or(false) {
        return Err(AppError::policy(
            "EXPLAIN_ANALYZE_BLOCKED",
            "EXPLAIN --analyze on mutating SQL requires a write-enabled target.",
        ));
    }

    let explain_sql = build_explain_sql(args, &sql);
    let execution = run_sql_capture_table(
        &psql_bin,
        conn,
        config,
        search_path.as_deref(),
        &explain_sql,
        args.timeout_ms,
    )?;

    let row_count = execution.table.as_ref().map(|t| t.rows.len());
    let data = if let Some(table) = &execution.table {
        json!({
            "columns": table.columns,
            "rows": table.rows,
            "analyze": args.analyze,
        })
    } else {
        json!({
            "stdout": execution.raw_stdout.trim(),
            "analyze": args.analyze,
        })
    };

    Ok(CommandOutcome {
        command: "explain".to_string(),
        target: Some(target_name.to_string()),
        data,
        table: execution.table,
        meta: ResponseMeta {
            duration_ms: 0,
            row_count,
            statement_timeout_ms: Some(execution.statement_timeout_ms),
        },
        summary: vec![
            format!("target={target_name}"),
            format!("analyze={}", args.analyze),
            "explain_executed=ok".to_string(),
        ],
    })
}

fn parse_table_filters(raw: &[String]) -> Result<Vec<TableRef>, AppError> {
    let mut out = Vec::new();
    for entry in raw {
        let value = entry.trim();
        let Some((schema, table)) = value.split_once('.') else {
            return Err(AppError::cli(
                "INVALID_TABLE_FILTER",
                format!("Invalid --table value '{value}'. Expected schema.table"),
            ));
        };

        let schema = schema.trim();
        let table = table.trim();
        if schema.is_empty() || table.is_empty() {
            return Err(AppError::cli(
                "INVALID_TABLE_FILTER",
                format!("Invalid --table value '{value}'. Expected schema.table"),
            ));
        }

        out.push(TableRef {
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }

    Ok(out)
}

fn build_schema_table_filter(
    schema_col: &str,
    table_col: Option<&str>,
    schema_filters: &[String],
    table_filters: &[TableRef],
    default_current_schemas: bool,
) -> String {
    let mut clauses = Vec::new();

    if !table_filters.is_empty() {
        if let Some(table_col_name) = table_col {
            let table_clauses = table_filters
                .iter()
                .map(|t| {
                    format!(
                        "({schema_col} = {} AND {table_col_name} = {})",
                        sql_literal(&t.schema),
                        sql_literal(&t.table)
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            clauses.push(format!("({table_clauses})"));
        }
    } else if !schema_filters.is_empty() {
        let schemas = schema_filters
            .iter()
            .map(|s| sql_literal(s.trim()))
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!("{schema_col} IN ({schemas})"));
    } else if default_current_schemas {
        clauses.push(format!("{schema_col} = ANY(current_schemas(false))"));
    }

    if clauses.is_empty() {
        "TRUE".to_string()
    } else {
        clauses.join(" AND ")
    }
}

fn build_introspect_sql(
    kind: &IntrospectKind,
    schema_filters: &[String],
    table_filters: &[TableRef],
) -> Result<String, AppError> {
    let sql = match kind {
        IntrospectKind::Schemas => {
            if !table_filters.is_empty() {
                return Err(AppError::cli(
                    "INVALID_FILTER",
                    "--table filter is not supported for kind=schemas",
                ));
            }
            let mut where_clauses =
                vec!["nspname NOT IN ('pg_catalog', 'information_schema')".to_string()];
            if !schema_filters.is_empty() {
                let schemas = schema_filters
                    .iter()
                    .map(|s| sql_literal(s.trim()))
                    .collect::<Vec<_>>()
                    .join(", ");
                where_clauses.push(format!("nspname IN ({schemas})"));
            }
            format!(
                "SELECT nspname AS schema_name FROM pg_namespace WHERE {} ORDER BY 1;",
                where_clauses.join(" AND ")
            )
        }
        IntrospectKind::Tables => {
            let filter = build_schema_table_filter(
                "table_schema",
                Some("table_name"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT table_schema, table_name, table_type\nFROM information_schema.tables\nWHERE {filter}\n  AND table_type = 'BASE TABLE'\nORDER BY table_schema, table_name;"
            )
        }
        IntrospectKind::Columns => {
            let filter = build_schema_table_filter(
                "table_schema",
                Some("table_name"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT table_schema, table_name, ordinal_position, column_name, data_type, is_nullable, COALESCE(column_default, '') AS column_default\nFROM information_schema.columns\nWHERE {filter}\nORDER BY table_schema, table_name, ordinal_position;"
            )
        }
        IntrospectKind::Indexes => {
            let filter = build_schema_table_filter(
                "schemaname",
                Some("tablename"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT schemaname AS table_schema, tablename AS table_name, indexname, indexdef\nFROM pg_indexes\nWHERE {filter}\nORDER BY schemaname, tablename, indexname;"
            )
        }
        IntrospectKind::Constraints => {
            let filter = build_schema_table_filter(
                "table_schema",
                Some("table_name"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT table_schema, table_name, constraint_name, constraint_type\nFROM information_schema.table_constraints\nWHERE {filter}\nORDER BY table_schema, table_name, constraint_name;"
            )
        }
        IntrospectKind::Views => {
            let filter = build_schema_table_filter(
                "table_schema",
                Some("table_name"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT table_schema, table_name, view_definition\nFROM information_schema.views\nWHERE {filter}\nORDER BY table_schema, table_name;"
            )
        }
        IntrospectKind::MaterializedViews => {
            let filter = build_schema_table_filter(
                "schemaname",
                Some("matviewname"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT schemaname AS table_schema, matviewname AS table_name, definition\nFROM pg_matviews\nWHERE {filter}\nORDER BY schemaname, matviewname;"
            )
        }
        IntrospectKind::Functions => {
            if !table_filters.is_empty() {
                return Err(AppError::cli(
                    "INVALID_FILTER",
                    "--table filter is not supported for kind=functions",
                ));
            }
            let filter = build_schema_table_filter("n.nspname", None, schema_filters, &[], true);
            format!(
                "SELECT n.nspname AS schema_name, p.proname AS function_name, pg_get_function_identity_arguments(p.oid) AS arguments, pg_get_function_result(p.oid) AS return_type\nFROM pg_proc p\nJOIN pg_namespace n ON n.oid = p.pronamespace\nWHERE {filter}\nORDER BY n.nspname, p.proname;"
            )
        }
        IntrospectKind::Triggers => {
            let filter = build_schema_table_filter(
                "event_object_schema",
                Some("event_object_table"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT trigger_schema, trigger_name, event_object_schema AS table_schema, event_object_table AS table_name, event_manipulation, action_timing\nFROM information_schema.triggers\nWHERE {filter}\nORDER BY trigger_schema, trigger_name;"
            )
        }
        IntrospectKind::Enums => {
            if !table_filters.is_empty() {
                return Err(AppError::cli(
                    "INVALID_FILTER",
                    "--table filter is not supported for kind=enums",
                ));
            }
            let filter = build_schema_table_filter("n.nspname", None, schema_filters, &[], true);
            format!(
                "SELECT n.nspname AS schema_name, t.typname AS enum_name, e.enumlabel AS enum_value, e.enumsortorder\nFROM pg_type t\nJOIN pg_enum e ON t.oid = e.enumtypid\nJOIN pg_namespace n ON n.oid = t.typnamespace\nWHERE {filter}\nORDER BY n.nspname, t.typname, e.enumsortorder;"
            )
        }
        IntrospectKind::Rowcounts => {
            let filter = build_schema_table_filter(
                "n.nspname",
                Some("c.relname"),
                schema_filters,
                table_filters,
                true,
            );
            format!(
                "SELECT n.nspname AS table_schema, c.relname AS table_name, c.reltuples::bigint AS estimated_rows\nFROM pg_class c\nJOIN pg_namespace n ON n.oid = c.relnamespace\nWHERE c.relkind = 'r'\n  AND {filter}\nORDER BY n.nspname, c.relname;"
            )
        }
        IntrospectKind::RowcountsExact => {
            return Err(AppError::cli(
                "ROWCOUNTS_EXACT_INTERNAL",
                "rowcounts_exact should be executed via dedicated handler",
            ));
        }
    };

    Ok(sql)
}

fn run_introspect(
    _project_root: &Path,
    config: &Config,
    target: Option<&str>,
    args: &IntrospectArgs,
) -> Result<CommandOutcome, AppError> {
    let psql_bin = resolve_psql(config)?;
    let (target_name, conn) = resolve_target(config, target)?;
    let search_path = normalize_search_path(conn.schema.as_ref().or(config.schema.as_ref()))?;
    let table_filters = parse_table_filters(&args.table)?;

    let execution = if matches!(args.kind, IntrospectKind::RowcountsExact) {
        run_exact_rowcounts(
            &psql_bin,
            conn,
            config,
            search_path.as_deref(),
            &args.schema,
            &table_filters,
            args.timeout_ms,
        )?
    } else {
        let sql = build_introspect_sql(&args.kind, &args.schema, &table_filters)?;
        run_sql_capture_table(
            &psql_bin,
            conn,
            config,
            search_path.as_deref(),
            &sql,
            args.timeout_ms,
        )?
    };

    let row_count = execution.table.as_ref().map(|t| t.rows.len());
    let kind_name = introspect_kind_name(&args.kind);
    let data = if let Some(table) = &execution.table {
        json!({
            "kind": kind_name,
            "columns": table.columns,
            "rows": table.rows,
        })
    } else {
        json!({
            "kind": kind_name,
            "stdout": execution.raw_stdout.trim(),
        })
    };

    Ok(CommandOutcome {
        command: "introspect".to_string(),
        target: Some(target_name.to_string()),
        data,
        table: execution.table,
        meta: ResponseMeta {
            duration_ms: 0,
            row_count,
            statement_timeout_ms: Some(execution.statement_timeout_ms),
        },
        summary: vec![
            format!("target={target_name}"),
            format!("kind={kind_name}"),
            "introspection_executed=ok".to_string(),
        ],
    })
}

fn introspect_kind_name(kind: &IntrospectKind) -> &'static str {
    match kind {
        IntrospectKind::Schemas => "schemas",
        IntrospectKind::Tables => "tables",
        IntrospectKind::Columns => "columns",
        IntrospectKind::Indexes => "indexes",
        IntrospectKind::Constraints => "constraints",
        IntrospectKind::Views => "views",
        IntrospectKind::MaterializedViews => "materialized_views",
        IntrospectKind::Functions => "functions",
        IntrospectKind::Triggers => "triggers",
        IntrospectKind::Enums => "enums",
        IntrospectKind::Rowcounts => "rowcounts",
        IntrospectKind::RowcountsExact => "rowcounts_exact",
    }
}

fn run_exact_rowcounts(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    schema_filters: &[String],
    table_filters: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<QueryExecution, AppError> {
    let filter = build_schema_table_filter(
        "table_schema",
        Some("table_name"),
        schema_filters,
        table_filters,
        true,
    );

    let list_sql = format!(
        "SELECT quote_ident(table_schema) || '.' || quote_ident(table_name) AS fq_table, table_schema, table_name\nFROM information_schema.tables\nWHERE table_type = 'BASE TABLE'\n  AND {filter}\nORDER BY table_schema, table_name;"
    );

    let listing = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        &list_sql,
        timeout_ms_override,
    )?;

    let mut result = TableData {
        columns: vec![
            "table_schema".to_string(),
            "table_name".to_string(),
            "exact_rows".to_string(),
        ],
        rows: Vec::new(),
    };

    let Some(list_table) = listing.table else {
        return Ok(QueryExecution {
            table: Some(result),
            raw_stdout: String::new(),
            statement_timeout_ms: listing.statement_timeout_ms,
        });
    };

    for row in &list_table.rows {
        if row.len() < 3 {
            continue;
        }
        let fq = row[0].clone();
        let schema = row[1].clone();
        let table = row[2].clone();
        let sql = format!("SELECT COUNT(*)::bigint AS exact_rows FROM {fq};");
        let count = run_sql_capture_table(
            psql_bin,
            conn,
            config,
            search_path,
            &sql,
            timeout_ms_override,
        )?;

        let exact = count
            .table
            .and_then(|t| t.rows.first().cloned())
            .and_then(|r| r.first().cloned())
            .unwrap_or_else(|| "0".to_string());

        result.rows.push(vec![schema, table, exact]);
    }

    Ok(QueryExecution {
        table: Some(result),
        raw_stdout: String::new(),
        statement_timeout_ms: listing.statement_timeout_ms,
    })
}

fn list_available_tables(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    timeout_ms_override: Option<u64>,
) -> Result<Vec<TableRef>, AppError> {
    let sql = "SELECT table_schema, table_name\nFROM information_schema.tables\nWHERE table_schema = ANY(current_schemas(false))\n  AND table_type = 'BASE TABLE'\nORDER BY table_schema, table_name;";

    let rows = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        sql,
        timeout_ms_override,
    )?;

    let table = rows.table.unwrap_or(TableData {
        columns: vec![],
        rows: vec![],
    });

    let mut out = Vec::new();
    for row in table.rows {
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
) -> Result<Vec<TableRef>, AppError> {
    if all_tables {
        return Ok(available_tables.to_vec());
    }

    let requested = config.important_tables.as_ref().ok_or_else(|| {
        AppError::cli(
            "IMPORTANT_TABLES_MISSING",
            "Missing top-level important_tables in .agents/postgres-cli/postgres.toml",
        )
    })?;

    if requested.is_empty() {
        return Err(AppError::cli(
            "IMPORTANT_TABLES_EMPTY",
            "important_tables is empty; add at least one table or use --all-tables",
        ));
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
        return Err(AppError::cli("IMPORTANT_TABLES_INVALID", errors.join("\n")));
    }

    Ok(selected.into_iter().collect())
}

fn markdown_escape(value: &str) -> String {
    value
        .replace('|', "\\|")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn render_table_markdown(
    doc: &TableSchemaDoc,
    generated_at: u64,
    target: &str,
    file_naming: SchemaCacheFileNaming,
) -> String {
    let mut out = String::new();
    out.push_str(&format!("# {}\n\n", doc.table.display_name(file_naming)));
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

fn relation_table_label(schema: &str, table: &str, file_naming: SchemaCacheFileNaming) -> String {
    match file_naming {
        SchemaCacheFileNaming::Table => table.to_string(),
        SchemaCacheFileNaming::SchemaTable => format!("{schema}.{table}"),
    }
}

fn render_relations_markdown(
    edges: &[RelationEdge],
    generated_at: u64,
    target: &str,
    file_naming: SchemaCacheFileNaming,
) -> String {
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
        let from_table = relation_table_label(&edge.from_schema, &edge.from_table, file_naming);
        let to_table = relation_table_label(&edge.to_schema, &edge.to_table, file_naming);
        out.push_str(&format!(
            "- `{}`: `{}.{}` -> `{}.{}`\n",
            edge.constraint_name, from_table, edge.from_column, to_table, edge.to_column
        ));
    }

    out
}

fn render_schema_readme(index: &SchemaIndex) -> String {
    let mut out = String::new();
    out.push_str("# postgres-cli schema cache\n\n");
    out.push_str(&format!("- Version: `{}`\n", index.version));
    out.push_str(&format!("- Target: `{}`\n", index.target));
    out.push_str(&format!("- Mode: `{}`\n", index.mode));
    out.push_str(&format!(
        "- Generated at (unix): `{}`\n",
        index.generated_at
    ));
    out.push_str(&format!("- Tables: `{}`\n", index.table_count));
    out.push_str(&format!("- Relations: `{}`\n\n", index.relation_count));
    out.push_str("This snapshot is JSON-first. Load `index.json` then required table files from `tables/`.\n");
    out
}

fn write_text(path: &Path, content: &str) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| AppError::runtime("SCHEMA_CACHE_WRITE_FAILED", e.to_string()))?;
    }
    fs::write(path, content)
        .map_err(|e| AppError::runtime("SCHEMA_CACHE_WRITE_FAILED", e.to_string()))
}

fn write_json<T: Serialize>(path: &Path, data: &T) -> Result<(), AppError> {
    let content = serde_json::to_string_pretty(data)
        .map_err(|e| AppError::runtime("SERIALIZE_FAILED", e.to_string()))?;
    write_text(path, &content)
}

fn validate_table_file_name_collisions(
    selected_tables: &[TableRef],
    file_naming: SchemaCacheFileNaming,
) -> Result<(), AppError> {
    if file_naming != SchemaCacheFileNaming::Table {
        return Ok(());
    }

    let mut table_to_schemas: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for table in selected_tables {
        table_to_schemas
            .entry(table.table.clone())
            .or_default()
            .insert(table.schema.clone());
    }

    let mut collisions = Vec::new();
    for (table_name, schemas) in table_to_schemas {
        if schemas.len() <= 1 {
            continue;
        }
        collisions.push(format!(
            "- table '{}' exists in schemas: {}",
            table_name,
            schemas.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }

    if collisions.is_empty() {
        return Ok(());
    }

    Err(AppError::cli(
        "SCHEMA_CACHE_COLLISION",
        format!(
            "Schema cache file naming collision for [schema_cache].file_naming = \"table\":\n{}\nUse [schema_cache] file_naming = \"schema_table\" or pass --table-file-naming schema-table.",
            collisions.join("\n")
        ),
    ))
}

fn build_schema_index_tables(
    selected_tables: &[TableRef],
    file_naming: SchemaCacheFileNaming,
    with_markdown: bool,
) -> Vec<SchemaIndexTable> {
    selected_tables
        .iter()
        .map(|t| SchemaIndexTable {
            schema: t.schema.clone(),
            table: t.table.clone(),
            json_file: format!("tables/{}", t.json_file(file_naming)),
            markdown_file: if with_markdown {
                Some(format!("tables/{}", t.markdown_file(file_naming)))
            } else {
                None
            },
        })
        .collect()
}

fn write_schema_snapshot(
    project_root: &Path,
    index: &SchemaIndex,
    docs: &[TableSchemaDoc],
    edges: &[RelationEdge],
    file_naming: SchemaCacheFileNaming,
    with_markdown: bool,
) -> Result<(), AppError> {
    let config_dir = preferred_config_dir(project_root);
    fs::create_dir_all(&config_dir)
        .map_err(|e| AppError::runtime("SCHEMA_CACHE_WRITE_FAILED", e.to_string()))?;

    let schema_root = config_dir.join("schema");
    let tmp_dir = config_dir.join(format!(
        "schema.tmp-{}-{}",
        index.generated_at,
        std::process::id()
    ));

    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)
            .map_err(|e| AppError::runtime("SCHEMA_CACHE_WRITE_FAILED", e.to_string()))?;
    }

    fs::create_dir_all(tmp_dir.join("tables"))
        .map_err(|e| AppError::runtime("SCHEMA_CACHE_WRITE_FAILED", e.to_string()))?;

    write_json(&tmp_dir.join("index.json"), index)?;
    write_json(&tmp_dir.join("relations.json"), &edges)?;

    if with_markdown {
        write_text(&tmp_dir.join("README.md"), &render_schema_readme(index))?;
        write_text(
            &tmp_dir.join("relations.md"),
            &render_relations_markdown(edges, index.generated_at, &index.target, file_naming),
        )?;
    }

    for doc in docs {
        let json_path = tmp_dir
            .join("tables")
            .join(doc.table.json_file(file_naming));
        write_json(&json_path, doc)?;

        if with_markdown {
            let md_path = tmp_dir
                .join("tables")
                .join(doc.table.markdown_file(file_naming));
            let content =
                render_table_markdown(doc, index.generated_at, &index.target, file_naming);
            write_text(&md_path, &content)?;
        }
    }

    if schema_root.exists() {
        fs::remove_dir_all(&schema_root)
            .map_err(|e| AppError::runtime("SCHEMA_CACHE_WRITE_FAILED", e.to_string()))?;
    }
    fs::rename(&tmp_dir, &schema_root)
        .map_err(|e| AppError::runtime("SCHEMA_CACHE_WRITE_FAILED", e.to_string()))?;

    Ok(())
}

fn schema_cache_file_naming(config: &Config) -> SchemaCacheFileNaming {
    config
        .schema_cache
        .as_ref()
        .and_then(|cfg| cfg.file_naming)
        .unwrap_or(SchemaCacheFileNaming::Table)
}

fn run_schema_cache_update(
    project_root: &Path,
    config: &Config,
    target: Option<&str>,
    args: &SchemaCacheUpdateArgs,
) -> Result<CommandOutcome, AppError> {
    let psql_bin = resolve_psql(config)?;
    let (target_name, conn) = resolve_target(config, target)?;
    let search_path = normalize_search_path(conn.schema.as_ref().or(config.schema.as_ref()))?;

    if !args.all_tables {
        let important = config.important_tables.as_ref().ok_or_else(|| {
            AppError::cli(
                "IMPORTANT_TABLES_MISSING",
                "Missing top-level important_tables in .agents/postgres-cli/postgres.toml",
            )
        })?;
        if important.is_empty() {
            return Err(AppError::cli(
                "IMPORTANT_TABLES_EMPTY",
                "important_tables is empty; add at least one table or use --all-tables",
            ));
        }
    }

    let file_naming = args
        .table_file_naming
        .clone()
        .map(SchemaCacheFileNaming::from)
        .unwrap_or_else(|| schema_cache_file_naming(config));

    let available_tables = list_available_tables(
        &psql_bin,
        conn,
        config,
        search_path.as_deref(),
        args.timeout_ms,
    )?;

    let mut selected_tables = resolve_selected_tables(&available_tables, config, args.all_tables)?;
    if !args.all_tables {
        let relation_pairs = schema_metadata::list_direct_table_relations(
            &psql_bin,
            conn,
            config,
            search_path.as_deref(),
            &selected_tables,
            args.timeout_ms,
        )?;
        selected_tables = expand_with_directly_related_tables(selected_tables, &relation_pairs);
    }
    validate_table_file_name_collisions(&selected_tables, file_naming)?;

    let docs = schema_metadata::fetch_table_schema_docs_batch(
        &psql_bin,
        conn,
        config,
        search_path.as_deref(),
        &selected_tables,
        args.timeout_ms,
    )?;

    let mut edge_set = BTreeSet::new();
    for doc in &docs {
        for fk in &doc.outbound_fks {
            edge_set.insert(RelationEdge {
                constraint_name: fk.constraint_name.clone(),
                from_schema: doc.table.schema.clone(),
                from_table: doc.table.table.clone(),
                from_column: fk.from_column.clone(),
                to_schema: fk.to_schema.clone(),
                to_table: fk.to_table.clone(),
                to_column: fk.to_column.clone(),
            });
        }
    }
    let edges: Vec<RelationEdge> = edge_set.into_iter().collect();

    let generated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| AppError::runtime("CLOCK_FAILED", e.to_string()))?
        .as_secs();

    let index_tables = build_schema_index_tables(&selected_tables, file_naming, args.with_markdown);

    let mode = if args.all_tables {
        "all_tables".to_string()
    } else {
        "important".to_string()
    };

    let index = SchemaIndex {
        version: VERSION,
        target: target_name.to_string(),
        mode,
        generated_at,
        table_count: docs.len(),
        relation_count: edges.len(),
        tables: index_tables,
    };

    write_schema_snapshot(
        project_root,
        &index,
        &docs,
        &edges,
        file_naming,
        args.with_markdown,
    )?;

    let data = json!({
        "target": index.target,
        "mode": index.mode,
        "generated_at": index.generated_at,
        "table_count": index.table_count,
        "relation_count": index.relation_count,
        "schema_path": preferred_config_dir(project_root).join("schema").display().to_string(),
        "with_markdown": args.with_markdown,
        "table_file_naming": match file_naming {
            SchemaCacheFileNaming::Table => "table",
            SchemaCacheFileNaming::SchemaTable => "schema_table",
        }
    });

    Ok(CommandOutcome {
        command: "schema-cache".to_string(),
        target: Some(target_name.to_string()),
        data,
        table: None,
        meta: ResponseMeta {
            duration_ms: 0,
            row_count: Some(docs.len()),
            statement_timeout_ms: None,
        },
        summary: vec![
            format!("target={target_name}"),
            format!(
                "mode={}",
                if args.all_tables {
                    "all_tables"
                } else {
                    "important"
                }
            ),
            format!("tables={}", docs.len()),
            format!("relations={}", edges.len()),
        ],
    })
}

fn run_targets_list(config: &Config) -> Result<CommandOutcome, AppError> {
    let mut rows = Vec::new();
    for (name, conn) in &config.connections {
        let is_default = config.default_target.as_deref() == Some(name.as_str());
        let connection_mode = if conn.dsn_env.is_some() {
            "dsn_env"
        } else {
            "host"
        };

        rows.push(vec![
            name.clone(),
            is_default.to_string(),
            conn.allow_write.unwrap_or(false).to_string(),
            connection_mode.to_string(),
            conn.database.clone().unwrap_or_default(),
            conn.username.clone().unwrap_or_default(),
            conn.host.clone().unwrap_or_default(),
            conn.port.map(|p| p.to_string()).unwrap_or_default(),
            conn.dsn_env.clone().unwrap_or_default(),
        ]);
    }

    let table = TableData {
        columns: vec![
            "target".to_string(),
            "is_default".to_string(),
            "allow_write".to_string(),
            "connection_mode".to_string(),
            "database".to_string(),
            "username".to_string(),
            "host".to_string(),
            "port".to_string(),
            "dsn_env".to_string(),
        ],
        rows,
    };

    Ok(CommandOutcome {
        command: "targets".to_string(),
        target: None,
        data: json!({
            "default_target": config.default_target,
            "columns": table.columns,
            "rows": table.rows,
        }),
        table: Some(table.clone()),
        meta: ResponseMeta {
            duration_ms: 0,
            row_count: Some(table.rows.len()),
            statement_timeout_ms: None,
        },
        summary: vec![format!("targets={}", table.rows.len())],
    })
}

#[derive(Debug, Clone, Serialize)]
struct ValidationCheck {
    name: String,
    status: String,
    message: String,
}

fn run_config_validate(config: &Config, target: Option<&str>) -> Result<CommandOutcome, AppError> {
    let mut checks = Vec::new();

    if config.config_version == Some(2) {
        checks.push(ValidationCheck {
            name: "config_version".to_string(),
            status: "pass".to_string(),
            message: "config_version=2".to_string(),
        });
    } else {
        checks.push(ValidationCheck {
            name: "config_version".to_string(),
            status: "fail".to_string(),
            message: "config_version must be set to 2".to_string(),
        });
    }

    if let Some(default_target) = &config.default_target {
        if config.connections.contains_key(default_target) {
            checks.push(ValidationCheck {
                name: "default_target".to_string(),
                status: "pass".to_string(),
                message: format!("default_target '{default_target}' exists"),
            });
        } else {
            checks.push(ValidationCheck {
                name: "default_target".to_string(),
                status: "fail".to_string(),
                message: format!("default_target '{default_target}' not found in [connections]"),
            });
        }
    } else {
        checks.push(ValidationCheck {
            name: "default_target".to_string(),
            status: "warn".to_string(),
            message: "No default_target set; --target will be required".to_string(),
        });
    }

    for (name, conn) in &config.connections {
        if conn.password.is_some() {
            checks.push(ValidationCheck {
                name: format!("connection:{name}:password"),
                status: "fail".to_string(),
                message: "Plaintext password is not allowed; use password_env".to_string(),
            });
        }

        if conn.dsn.is_some() {
            checks.push(ValidationCheck {
                name: format!("connection:{name}:dsn"),
                status: "fail".to_string(),
                message: "Plaintext dsn is not allowed; use dsn_env".to_string(),
            });
        }

        if let Some(password_env) = &conn.password_env {
            if !is_valid_env_key(password_env) {
                checks.push(ValidationCheck {
                    name: format!("connection:{name}:password_env"),
                    status: "fail".to_string(),
                    message: format!("Invalid env var name: {password_env}"),
                });
            } else if env::var_os(password_env).is_none() {
                checks.push(ValidationCheck {
                    name: format!("connection:{name}:password_env"),
                    status: "warn".to_string(),
                    message: format!("Env var {password_env} is not currently set"),
                });
            }
        }

        if let Some(dsn_env) = &conn.dsn_env {
            if !is_valid_env_key(dsn_env) {
                checks.push(ValidationCheck {
                    name: format!("connection:{name}:dsn_env"),
                    status: "fail".to_string(),
                    message: format!("Invalid env var name: {dsn_env}"),
                });
            } else if env::var_os(dsn_env).is_none() {
                checks.push(ValidationCheck {
                    name: format!("connection:{name}:dsn_env"),
                    status: "warn".to_string(),
                    message: format!("Env var {dsn_env} is not currently set"),
                });
            }
        }

        if conn.dsn_env.is_some() {
            checks.push(ValidationCheck {
                name: format!("connection:{name}:mode"),
                status: "pass".to_string(),
                message: "Connection uses dsn_env".to_string(),
            });
        } else {
            let mut missing = Vec::new();
            if conn
                .username
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                missing.push("username");
            }
            if conn
                .database
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                missing.push("database");
            }

            if missing.is_empty() {
                checks.push(ValidationCheck {
                    name: format!("connection:{name}:mode"),
                    status: "pass".to_string(),
                    message: "Connection uses host/database/username fields".to_string(),
                });
            } else {
                checks.push(ValidationCheck {
                    name: format!("connection:{name}:mode"),
                    status: "fail".to_string(),
                    message: format!("Missing required fields: {}", missing.join(", ")),
                });
            }
        }
    }

    if let Some(requested_target) = target {
        if config.connections.contains_key(requested_target) {
            checks.push(ValidationCheck {
                name: "requested_target".to_string(),
                status: "pass".to_string(),
                message: format!("target '{requested_target}' exists"),
            });
        } else {
            checks.push(ValidationCheck {
                name: "requested_target".to_string(),
                status: "fail".to_string(),
                message: format!("target '{requested_target}' does not exist"),
            });
        }
    }

    let has_fail = checks.iter().any(|c| c.status == "fail");

    let table = TableData {
        columns: vec![
            "name".to_string(),
            "status".to_string(),
            "message".to_string(),
        ],
        rows: checks
            .iter()
            .map(|c| vec![c.name.clone(), c.status.clone(), c.message.clone()])
            .collect(),
    };

    if has_fail {
        return Err(
            AppError::cli("CONFIG_VALIDATION_FAILED", "Config validation failed").with_details(
                json!({
                    "checks": checks,
                }),
            ),
        );
    }

    Ok(CommandOutcome {
        command: "config".to_string(),
        target: target.map(|s| s.to_string()),
        data: json!({
            "checks": checks,
        }),
        table: Some(table.clone()),
        meta: ResponseMeta {
            duration_ms: 0,
            row_count: Some(table.rows.len()),
            statement_timeout_ms: None,
        },
        summary: vec!["config_validation=ok".to_string()],
    })
}

fn run_doctor(
    _project_root: &Path,
    config: &Config,
    target: Option<&str>,
    args: &DoctorArgs,
) -> Result<CommandOutcome, AppError> {
    let mut checks = Vec::<ValidationCheck>::new();

    let psql_bin = match resolve_psql(config) {
        Ok(bin) => {
            checks.push(ValidationCheck {
                name: "psql".to_string(),
                status: "pass".to_string(),
                message: format!("resolved psql at {bin}"),
            });
            bin
        }
        Err(e) => {
            checks.push(ValidationCheck {
                name: "psql".to_string(),
                status: "fail".to_string(),
                message: e.message.clone(),
            });
            return Err(AppError::runtime("DOCTOR_FAILED", "Doctor checks failed")
                .with_details(json!({ "checks": checks })));
        }
    };

    let (target_name, conn) = resolve_target(config, target)?;
    checks.push(ValidationCheck {
        name: "target".to_string(),
        status: "pass".to_string(),
        message: format!("using target '{target_name}'"),
    });

    let search_path = normalize_search_path(conn.schema.as_ref().or(config.schema.as_ref()))?;

    match run_sql_capture_table(
        &psql_bin,
        conn,
        config,
        search_path.as_deref(),
        "SELECT 1 AS ok;",
        args.timeout_ms,
    ) {
        Ok(_) => checks.push(ValidationCheck {
            name: "connectivity".to_string(),
            status: "pass".to_string(),
            message: "SELECT 1 succeeded".to_string(),
        }),
        Err(e) => {
            checks.push(ValidationCheck {
                name: "connectivity".to_string(),
                status: "fail".to_string(),
                message: e.message,
            });
            return Err(AppError::runtime("DOCTOR_FAILED", "Doctor checks failed")
                .with_details(json!({ "checks": checks })));
        }
    }

    checks.push(ValidationCheck {
        name: "write_mode".to_string(),
        status: "pass".to_string(),
        message: if conn.allow_write.unwrap_or(false) {
            "target is write-enabled".to_string()
        } else {
            "target is read-only by config".to_string()
        },
    });

    let table = TableData {
        columns: vec![
            "name".to_string(),
            "status".to_string(),
            "message".to_string(),
        ],
        rows: checks
            .iter()
            .map(|c| vec![c.name.clone(), c.status.clone(), c.message.clone()])
            .collect(),
    };

    Ok(CommandOutcome {
        command: "doctor".to_string(),
        target: Some(target_name.to_string()),
        data: json!({ "checks": checks }),
        table: Some(table.clone()),
        meta: ResponseMeta {
            duration_ms: 0,
            row_count: Some(table.rows.len()),
            statement_timeout_ms: None,
        },
        summary: vec![format!("target={target_name}"), "doctor=ok".to_string()],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn config_minimal_toml(extra: &str) -> String {
        format!(
            "{}\nconfig_version = 2\n[connections.dev]\ndatabase = \"app\"\nusername = \"postgres\"\n",
            extra
        )
    }

    fn unique_test_temp_dir() -> PathBuf {
        let suffix = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "postgres_cli_test_{}_{}_{}",
            std::process::id(),
            nanos,
            suffix
        ));
        fs::create_dir_all(&dir).expect("test temp dir should be creatable");
        dir
    }

    fn unique_env_key(prefix: &str) -> String {
        let suffix = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}_{}", prefix, std::process::id(), suffix)
    }

    #[test]
    fn clap_commands_compile() {
        Cli::command().debug_assert();
    }

    #[test]
    fn schema_cache_file_naming_defaults_to_table() {
        let toml = config_minimal_toml("");
        let config: Config = toml::from_str(&toml).expect("config should parse");
        assert_eq!(
            schema_cache_file_naming(&config),
            SchemaCacheFileNaming::Table
        );
    }

    #[test]
    fn schema_cache_file_naming_parses_schema_table() {
        let toml = config_minimal_toml("[schema_cache]\nfile_naming = \"schema_table\"");
        let config: Config = toml::from_str(&toml).expect("config should parse");
        assert_eq!(
            schema_cache_file_naming(&config),
            SchemaCacheFileNaming::SchemaTable
        );
    }

    #[test]
    fn schema_cache_file_naming_rejects_invalid_value() {
        let toml = config_minimal_toml("[schema_cache]\nfile_naming = \"bogus\"");
        assert!(toml::from_str::<Config>(&toml).is_err());
    }

    #[test]
    fn write_detection_works() {
        assert!(is_write_sql("INSERT INTO x VALUES (1)"));
        assert!(is_write_sql("/* a */ UPDATE x SET y=1"));
        assert!(!is_write_sql("SELECT * FROM x"));
    }

    #[test]
    fn table_mode_collision_check_fails_for_same_table_in_multiple_schemas() {
        let selected = vec![
            TableRef {
                schema: "tenant_a".to_string(),
                table: "account".to_string(),
            },
            TableRef {
                schema: "public".to_string(),
                table: "account".to_string(),
            },
        ];
        let err = validate_table_file_name_collisions(&selected, SchemaCacheFileNaming::Table)
            .expect_err("collision should fail");
        assert_eq!(err.code, "SCHEMA_CACHE_COLLISION");
    }

    #[test]
    fn schema_table_mode_collision_check_allows_duplicate_table_names() {
        let selected = vec![
            TableRef {
                schema: "tenant_a".to_string(),
                table: "account".to_string(),
            },
            TableRef {
                schema: "public".to_string(),
                table: "account".to_string(),
            },
        ];
        validate_table_file_name_collisions(&selected, SchemaCacheFileNaming::SchemaTable)
            .expect("schema_table mode should allow duplicates");
    }

    #[test]
    fn config_validation_rejects_plaintext_secrets() {
        let toml = r#"
config_version = 2
[connections.dev]
database = "app"
username = "postgres"
password = "secret"
"#;
        let cfg: Config = toml::from_str(toml).expect("valid toml parse");
        let err = run_config_validate(&cfg, None).expect_err("should fail");
        assert_eq!(err.code, "CONFIG_VALIDATION_FAILED");
    }

    #[test]
    fn resolve_target_prefers_explicit_target_over_default() {
        let toml = r#"
config_version = 2
default_target = "read"
[connections.read]
database = "app"
username = "postgres"
allow_write = false
[connections.write]
database = "app"
username = "postgres"
allow_write = true
"#;
        let cfg: Config = toml::from_str(toml).expect("valid toml parse");
        let (target_name, conn) =
            resolve_target(&cfg, Some("write")).expect("explicit target should resolve");
        assert_eq!(target_name, "write");
        assert_eq!(conn.allow_write, Some(true));
    }

    #[test]
    fn resolve_target_uses_default_when_target_omitted() {
        let toml = r#"
config_version = 2
default_target = "read"
[connections.read]
database = "app"
username = "postgres"
allow_write = false
[connections.write]
database = "app"
username = "postgres"
allow_write = true
"#;
        let cfg: Config = toml::from_str(toml).expect("valid toml parse");
        let (target_name, conn) =
            resolve_target(&cfg, None).expect("default target should resolve");
        assert_eq!(target_name, "read");
        assert_eq!(conn.allow_write, Some(false));
    }

    #[test]
    fn resolve_target_errors_when_no_target_and_no_default() {
        let toml = r#"
config_version = 2
[connections.read]
database = "app"
username = "postgres"
"#;
        let cfg: Config = toml::from_str(toml).expect("valid toml parse");
        let err = resolve_target(&cfg, None).expect_err("target should be required");
        assert_eq!(err.code, "TARGET_MISSING");
    }

    #[test]
    fn resolve_target_errors_on_unknown_target() {
        let toml = r#"
config_version = 2
default_target = "read"
[connections.read]
database = "app"
username = "postgres"
"#;
        let cfg: Config = toml::from_str(toml).expect("valid toml parse");
        let err = resolve_target(&cfg, Some("missing")).expect_err("unknown target should fail");
        assert_eq!(err.code, "TARGET_UNKNOWN");
        assert!(err.message.contains("Unknown target 'missing'"));
    }

    #[test]
    fn introspect_sql_dispatch_tables() {
        let sql = build_introspect_sql(&IntrospectKind::Tables, &[], &[]).expect("sql");
        assert!(sql.contains("information_schema.tables"));
    }

    #[test]
    fn explain_sql_builder_includes_flags() {
        let args = ExplainArgs {
            input: SqlInput {
                sql: Some("SELECT 1".to_string()),
                sql_file: None,
                from_stdin: false,
            },
            analyze: true,
            verbose: true,
            buffers: false,
            settings: false,
            wal: false,
            timeout_ms: None,
        };

        let sql = build_explain_sql(&args, "SELECT 1");
        assert!(sql.contains("ANALYZE true"));
        assert!(sql.contains("VERBOSE true"));
    }

    #[test]
    fn load_dotenv_prefers_agents_dir_for_same_key() {
        let project_root = unique_test_temp_dir();
        let key = unique_env_key("POSTGRES_CLI_ENV_PRIORITY");
        let preferred_path = preferred_config_dir(&project_root).join(".env");

        fs::create_dir_all(preferred_path.parent().expect("preferred parent")).expect("mkdir");
        fs::write(&preferred_path, format!("{key}=preferred\n")).expect("write preferred env");

        env::remove_var(&key);
        load_dotenv(&project_root).expect("dotenv should load");
        assert_eq!(
            env::var(&key).expect("env key should be set"),
            "preferred".to_string()
        );

        env::remove_var(&key);
        fs::remove_dir_all(&project_root).expect("cleanup temp dir");
    }

    #[test]
    fn load_dotenv_prefers_agent_dir_over_root_when_both_set() {
        let project_root = unique_test_temp_dir();
        let key = unique_env_key("POSTGRES_CLI_ENV_PRIORITY");
        let preferred_path = preferred_config_dir(&project_root).join(".env");
        let root_path = project_root.join(".env");

        fs::create_dir_all(preferred_path.parent().expect("preferred parent")).expect("mkdir");
        fs::write(&preferred_path, format!("{key}=preferred\n")).expect("write preferred env");
        fs::write(&root_path, format!("{key}=root\n")).expect("write root env");

        env::remove_var(&key);
        load_dotenv(&project_root).expect("dotenv should load");
        assert_eq!(env::var(&key).expect("env key should be set"), "preferred");

        env::remove_var(&key);
        fs::remove_dir_all(&project_root).expect("cleanup temp dir");
    }

    #[test]
    fn load_config_checks_only_preferred_agent_dir() {
        let project_root = unique_test_temp_dir();
        let unrelated_legacy_like_path = project_root
            .join(".legacy-config")
            .join("postgres-cli")
            .join("postgres.toml");
        fs::create_dir_all(
            unrelated_legacy_like_path
                .parent()
                .expect("legacy-like parent"),
        )
        .expect("mkdir");
        fs::write(&unrelated_legacy_like_path, "config_version = 2\n").expect("write legacy test config");

        let preferred_path = preferred_config_path(&project_root);
        let err = load_config(&project_root).expect_err("missing preferred config should fail");
        assert_eq!(err.code, "CONFIG_NOT_FOUND");
        assert_eq!(err.message, format!("Missing config. Checked {}", preferred_path.display()));

        fs::remove_dir_all(&project_root).expect("cleanup temp dir");
    }
}
