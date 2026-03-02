use crate::{
    run_sql_capture_table, sql_literal, AppError, ColumnInfo, Config, Connection, InboundFk,
    IndexInfo, OutboundFk, TableData, TableRef, TableSchemaDoc,
};
use std::collections::BTreeMap;

fn build_selected_tables_values_sql(selected_tables: &[TableRef]) -> Option<String> {
    if selected_tables.is_empty() {
        return None;
    }

    Some(
        selected_tables
            .iter()
            .map(|table| {
                format!(
                    "({}, {})",
                    sql_literal(&table.schema),
                    sql_literal(&table.table)
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

pub(crate) fn list_direct_table_relations(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    selected_tables: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<Vec<(TableRef, TableRef)>, AppError> {
    let Some(values_sql) = build_selected_tables_values_sql(selected_tables) else {
        return Ok(Vec::new());
    };
    let sql = format!(
        "WITH selected(table_schema, table_name) AS (VALUES {values_sql})\nSELECT src_ns.nspname AS from_schema,\n       src.relname AS from_table,\n       dst_ns.nspname AS to_schema,\n       dst.relname AS to_table\nFROM pg_constraint con\nJOIN pg_class src ON src.oid = con.conrelid\nJOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace\nJOIN pg_class dst ON dst.oid = con.confrelid\nJOIN pg_namespace dst_ns ON dst_ns.oid = dst.relnamespace\nWHERE con.contype = 'f'\n  AND src_ns.nspname = ANY(current_schemas(false))\n  AND dst_ns.nspname = ANY(current_schemas(false))\n  AND (\n    (src_ns.nspname, src.relname) IN (SELECT table_schema, table_name FROM selected)\n    OR (dst_ns.nspname, dst.relname) IN (SELECT table_schema, table_name FROM selected)\n  )\nORDER BY src_ns.nspname, src.relname, dst_ns.nspname, dst.relname;"
    );

    let rows = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        &sql,
        timeout_ms_override,
    )?;

    let table = rows.table.unwrap_or(TableData {
        columns: vec![],
        rows: vec![],
    });

    let mut out = Vec::new();
    for row in table.rows {
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

fn fetch_columns_batch(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    selected_tables: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<BTreeMap<TableRef, Vec<ColumnInfo>>, AppError> {
    let mut by_table = BTreeMap::new();
    let Some(values_sql) = build_selected_tables_values_sql(selected_tables) else {
        return Ok(by_table);
    };

    let sql = format!(
        "WITH selected(table_schema, table_name) AS (VALUES {values_sql})\nSELECT n.nspname AS table_schema,\n       cls.relname AS table_name,\n       att.attnum AS ordinal_position,\n       att.attname AS column_name,\n       pg_catalog.format_type(att.atttypid, att.atttypmod) AS data_type,\n       CASE WHEN att.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,\n       COALESCE(pg_get_expr(def.adbin, def.adrelid), '') AS column_default\nFROM pg_class cls\nJOIN pg_namespace n ON n.oid = cls.relnamespace\nJOIN selected s\n  ON s.table_schema = n.nspname\n AND s.table_name = cls.relname\nJOIN pg_attribute att ON att.attrelid = cls.oid\nLEFT JOIN pg_attrdef def\n  ON def.adrelid = cls.oid\n AND def.adnum = att.attnum\nWHERE cls.relkind IN ('r', 'p')\n  AND att.attnum > 0\n  AND NOT att.attisdropped\nORDER BY n.nspname, cls.relname, att.attnum;"
    );

    let rows = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        &sql,
        timeout_ms_override,
    )?;

    for row in rows
        .table
        .unwrap_or(TableData {
            columns: vec![],
            rows: vec![],
        })
        .rows
    {
        if row.len() < 7 {
            continue;
        }
        let table = TableRef {
            schema: row[0].trim().to_string(),
            table: row[1].trim().to_string(),
        };
        let column_default = if row[6].trim().is_empty() {
            None
        } else {
            Some(row[6].clone())
        };
        by_table.entry(table).or_default().push(ColumnInfo {
            ordinal_position: row[2].parse::<usize>().unwrap_or(0),
            column_name: row[3].clone(),
            data_type: row[4].clone(),
            is_nullable: row[5].eq_ignore_ascii_case("YES"),
            column_default,
        });
    }

    Ok(by_table)
}

fn fetch_primary_key_columns_batch(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    selected_tables: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<BTreeMap<TableRef, Vec<String>>, AppError> {
    let mut by_table = BTreeMap::new();
    let Some(values_sql) = build_selected_tables_values_sql(selected_tables) else {
        return Ok(by_table);
    };

    let sql = format!(
        "WITH selected(table_schema, table_name) AS (VALUES {values_sql})\nSELECT n.nspname AS table_schema,\n       cls.relname AS table_name,\n       att.attname AS column_name\nFROM pg_constraint con\nJOIN pg_class cls ON cls.oid = con.conrelid\nJOIN pg_namespace n ON n.oid = cls.relnamespace\nJOIN selected s\n  ON s.table_schema = n.nspname\n AND s.table_name = cls.relname\nJOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_col(attnum, ord) ON true\nJOIN pg_attribute att\n  ON att.attrelid = cls.oid\n AND att.attnum = key_col.attnum\nWHERE con.contype = 'p'\nORDER BY n.nspname, cls.relname, key_col.ord;"
    );

    let rows = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        &sql,
        timeout_ms_override,
    )?;

    for row in rows
        .table
        .unwrap_or(TableData {
            columns: vec![],
            rows: vec![],
        })
        .rows
    {
        if row.len() < 3 {
            continue;
        }
        let table = TableRef {
            schema: row[0].trim().to_string(),
            table: row[1].trim().to_string(),
        };
        by_table.entry(table).or_default().push(row[2].clone());
    }

    Ok(by_table)
}

fn fetch_outbound_fks_batch(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    selected_tables: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<BTreeMap<TableRef, Vec<OutboundFk>>, AppError> {
    let mut by_table = BTreeMap::new();
    let Some(values_sql) = build_selected_tables_values_sql(selected_tables) else {
        return Ok(by_table);
    };

    let sql = format!(
        "WITH selected(table_schema, table_name) AS (VALUES {values_sql})\nSELECT src_ns.nspname AS table_schema,\n       src.relname AS table_name,\n       con.conname AS constraint_name,\n       src_att.attname AS from_column,\n       dst_ns.nspname AS to_schema,\n       dst.relname AS to_table,\n       dst_att.attname AS to_column\nFROM pg_constraint con\nJOIN pg_class src ON src.oid = con.conrelid\nJOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace\nJOIN selected s\n  ON s.table_schema = src_ns.nspname\n AND s.table_name = src.relname\nJOIN pg_class dst ON dst.oid = con.confrelid\nJOIN pg_namespace dst_ns ON dst_ns.oid = dst.relnamespace\nJOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS cols(src_attnum, dst_attnum, ord) ON true\nJOIN pg_attribute src_att\n  ON src_att.attrelid = src.oid\n AND src_att.attnum = cols.src_attnum\nJOIN pg_attribute dst_att\n  ON dst_att.attrelid = dst.oid\n AND dst_att.attnum = cols.dst_attnum\nWHERE con.contype = 'f'\nORDER BY src_ns.nspname, src.relname, con.conname, cols.ord;"
    );

    let rows = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        &sql,
        timeout_ms_override,
    )?;

    for row in rows
        .table
        .unwrap_or(TableData {
            columns: vec![],
            rows: vec![],
        })
        .rows
    {
        if row.len() < 7 {
            continue;
        }
        let table = TableRef {
            schema: row[0].trim().to_string(),
            table: row[1].trim().to_string(),
        };
        by_table.entry(table).or_default().push(OutboundFk {
            constraint_name: row[2].clone(),
            from_column: row[3].clone(),
            to_schema: row[4].clone(),
            to_table: row[5].clone(),
            to_column: row[6].clone(),
        });
    }

    Ok(by_table)
}

fn fetch_inbound_fks_batch(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    selected_tables: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<BTreeMap<TableRef, Vec<InboundFk>>, AppError> {
    let mut by_table = BTreeMap::new();
    let Some(values_sql) = build_selected_tables_values_sql(selected_tables) else {
        return Ok(by_table);
    };

    let sql = format!(
        "WITH selected(table_schema, table_name) AS (VALUES {values_sql})\nSELECT dst_ns.nspname AS table_schema,\n       dst.relname AS table_name,\n       src_ns.nspname AS from_schema,\n       src.relname AS from_table,\n       con.conname AS constraint_name,\n       src_att.attname AS from_column,\n       dst_att.attname AS to_column\nFROM pg_constraint con\nJOIN pg_class src ON src.oid = con.conrelid\nJOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace\nJOIN pg_class dst ON dst.oid = con.confrelid\nJOIN pg_namespace dst_ns ON dst_ns.oid = dst.relnamespace\nJOIN selected s\n  ON s.table_schema = dst_ns.nspname\n AND s.table_name = dst.relname\nJOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS cols(src_attnum, dst_attnum, ord) ON true\nJOIN pg_attribute src_att\n  ON src_att.attrelid = src.oid\n AND src_att.attnum = cols.src_attnum\nJOIN pg_attribute dst_att\n  ON dst_att.attrelid = dst.oid\n AND dst_att.attnum = cols.dst_attnum\nWHERE con.contype = 'f'\nORDER BY dst_ns.nspname, dst.relname, src_ns.nspname, src.relname, con.conname, cols.ord;"
    );

    let rows = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        &sql,
        timeout_ms_override,
    )?;

    for row in rows
        .table
        .unwrap_or(TableData {
            columns: vec![],
            rows: vec![],
        })
        .rows
    {
        if row.len() < 7 {
            continue;
        }
        let table = TableRef {
            schema: row[0].trim().to_string(),
            table: row[1].trim().to_string(),
        };
        by_table.entry(table).or_default().push(InboundFk {
            from_schema: row[2].clone(),
            from_table: row[3].clone(),
            constraint_name: row[4].clone(),
            from_column: row[5].clone(),
            to_column: row[6].clone(),
        });
    }

    Ok(by_table)
}

fn fetch_indexes_batch(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    selected_tables: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<BTreeMap<TableRef, Vec<IndexInfo>>, AppError> {
    let mut by_table = BTreeMap::new();
    let Some(values_sql) = build_selected_tables_values_sql(selected_tables) else {
        return Ok(by_table);
    };

    let sql = format!(
        "WITH selected(table_schema, table_name) AS (VALUES {values_sql})\nSELECT n.nspname AS table_schema,\n       cls.relname AS table_name,\n       idx_cls.relname AS index_name,\n       pg_get_indexdef(idx.indexrelid) AS index_def\nFROM pg_index idx\nJOIN pg_class cls ON cls.oid = idx.indrelid\nJOIN pg_namespace n ON n.oid = cls.relnamespace\nJOIN pg_class idx_cls ON idx_cls.oid = idx.indexrelid\nJOIN selected s\n  ON s.table_schema = n.nspname\n AND s.table_name = cls.relname\nORDER BY n.nspname, cls.relname, idx_cls.relname;"
    );

    let rows = run_sql_capture_table(
        psql_bin,
        conn,
        config,
        search_path,
        &sql,
        timeout_ms_override,
    )?;

    for row in rows
        .table
        .unwrap_or(TableData {
            columns: vec![],
            rows: vec![],
        })
        .rows
    {
        if row.len() < 4 {
            continue;
        }
        let table = TableRef {
            schema: row[0].trim().to_string(),
            table: row[1].trim().to_string(),
        };
        by_table.entry(table).or_default().push(IndexInfo {
            index_name: row[2].clone(),
            index_def: row[3].clone(),
        });
    }

    Ok(by_table)
}

pub(crate) fn fetch_table_schema_docs_batch(
    psql_bin: &str,
    conn: &Connection,
    config: &Config,
    search_path: Option<&str>,
    selected_tables: &[TableRef],
    timeout_ms_override: Option<u64>,
) -> Result<Vec<TableSchemaDoc>, AppError> {
    let columns_by_table = fetch_columns_batch(
        psql_bin,
        conn,
        config,
        search_path,
        selected_tables,
        timeout_ms_override,
    )?;
    let primary_keys_by_table = fetch_primary_key_columns_batch(
        psql_bin,
        conn,
        config,
        search_path,
        selected_tables,
        timeout_ms_override,
    )?;
    let outbound_fks_by_table = fetch_outbound_fks_batch(
        psql_bin,
        conn,
        config,
        search_path,
        selected_tables,
        timeout_ms_override,
    )?;
    let inbound_fks_by_table = fetch_inbound_fks_batch(
        psql_bin,
        conn,
        config,
        search_path,
        selected_tables,
        timeout_ms_override,
    )?;
    let indexes_by_table = fetch_indexes_batch(
        psql_bin,
        conn,
        config,
        search_path,
        selected_tables,
        timeout_ms_override,
    )?;

    let docs = selected_tables
        .iter()
        .map(|table| TableSchemaDoc {
            table: table.clone(),
            columns: columns_by_table.get(table).cloned().unwrap_or_default(),
            primary_key_columns: primary_keys_by_table
                .get(table)
                .cloned()
                .unwrap_or_default(),
            outbound_fks: outbound_fks_by_table
                .get(table)
                .cloned()
                .unwrap_or_default(),
            inbound_fks: inbound_fks_by_table.get(table).cloned().unwrap_or_default(),
            indexes: indexes_by_table.get(table).cloned().unwrap_or_default(),
        })
        .collect();

    Ok(docs)
}
