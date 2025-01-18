use anyhow::{bail, Result};
use page_io::{full_table_scan, index_scan, row_lookup, DbHeader, IndexSchema, Page, SqliteSchema, TableSchema};
use std::fs::File;
use syntax::statement::Statement;

mod typecodes;
mod utils;
use typecodes::SqlValue;
mod syntax;
use syntax::tokenizer::tokenize;

mod page_io;

const TABLESCHEMA_PAGE: u64 = 1;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let dbheader = DbHeader::from_file(&mut file)?;
            let page = Page::from_file(&mut file, TABLESCHEMA_PAGE, &dbheader)?;
            let page_header = &page.header;

            println!("database page size: {}", dbheader.page_size);
            println!("number of tables: {}", page_header.num_cells);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let dbheader = DbHeader::from_file(&mut file)?;
            let page = Page::from_file(&mut file, TABLESCHEMA_PAGE, &dbheader)?;
            let schema = SqliteSchema::from_page(&mut file, &dbheader, &page)?;
            for table in schema.schema_elements {
                println!("{}", table.name);
            }
        }
        ".tokenize" => {
            let mut file = File::open(&args[1])?;
            let input = std::fs::read_to_string(&args[1])?;
            let tokenized = tokenize(&input);
            for token in tokenized {
                println!("{:?}", token);
            }
        }
        ".parse" => {
            let mut file = File::open(&args[1])?;
            let input = std::fs::read_to_string(&args[1])?;
            let stmt = syntax::parse(&input);
            println!("{:?}", stmt);
        }
        select_count_from
            if select_count_from
                .to_uppercase()
                .starts_with("SELECT COUNT(*) FROM") =>
        {
            let mut file = File::open(&args[1])?;
            let dbheader = DbHeader::from_file(&mut file)?;
            let page = Page::from_file(&mut file, TABLESCHEMA_PAGE, &dbheader)?;
            let schema = SqliteSchema::from_page(&mut file, &dbheader, &page)?;
            let tablename = *select_count_from
                .split(" ")
                .collect::<Vec<_>>()
                .last()
                .unwrap();
            let table = schema.schema_elements.iter().find(|t| t.name == tablename);
            let page = Page::from_file(&mut file, table.unwrap().rootpage, &dbheader)?;
            println!("{}", page.header.num_cells);
        }
        select_rows if select_rows.to_uppercase().starts_with("SELECT") => {
            let mut file = File::open(&args[1])?;
            let dbheader = DbHeader::from_file(&mut file)?;
            let page = Page::from_file(&mut file, TABLESCHEMA_PAGE, &dbheader)?;
            let schema = SqliteSchema::from_page(&mut file, &dbheader, &page)?;
            let parsed_select_stmt = syntax::parse(&select_rows);
            let (cols, tablename) = match parsed_select_stmt {
                Statement::Select(ref stmt) => (stmt.columns.clone(), stmt.table.clone()),
                _ => panic!("Expected Select statement"),
            };
            let table = schema.schema_elements.iter().find(|t| t.name == tablename);
            let parsed_table_sql = syntax::parse(&table.unwrap().sql);
            let table_schema = match parsed_table_sql {
                Statement::CreateTable(stmt) => TableSchema::from_ast(&stmt),
                _ => panic!("Expected CreateTable statement"),
            };
            let mut indices_of_selected_cols = Vec::new();
            for selected_col in &cols {
                let index_of_selected_col = table_schema
                    .columns
                    .iter()
                    .position(|c| c.name == *selected_col);
                match index_of_selected_col {
                    Some(ix) => {
                        indices_of_selected_cols.push(ix);
                    }
                    None => panic!("Column not found: {}", selected_col),
                }
            }
            let mut indices_of_where_cols = Vec::new();
            let mut where_clause = Vec::new();
            match parsed_select_stmt {
                Statement::Select(ref stmt) => {
                    for clause in stmt.where_clause.iter() {
                        let index_of_where_col = table_schema
                            .columns
                            .iter()
                            .position(|c| c.name == clause.column);
                        match index_of_where_col {
                            Some(ix) => {
                                indices_of_where_cols.push(ix);
                                where_clause.push(clause);
                            }
                            None => panic!("Column not found: {}", clause.column),
                        }
                    }
                }
                _ => panic!("Expected Select statement"),
            }
            // get all of the schema elements in the sqlschema, and see if any have type index and name tablename
            let index_elements = schema
                .schema_elements
                .iter()
                .filter(|e| e.element_type == "index");
            let mut usable_indexes = Vec::new(); // super advanced query optimition
            for index in index_elements {
                let parsed_index_sql = syntax::parse(&index.sql);
                let index_schema = match parsed_index_sql {
                    Statement::CreateIndex { unique: _, stmt } => IndexSchema {
                        name: stmt.index_name,
                        table_name: stmt.table_name,
                        columns: stmt.columns,
                    },
                    _ => panic!("Expected CreateIndex statement"),
                };
                if index_schema.table_name == tablename {
                    for clause in where_clause.iter() {
                        if index_schema.columns.contains(&clause.column) {
                            usable_indexes.push(index.rootpage);
                        }
                    }
                }
            }
            let mut records;
            if usable_indexes.len() > 0 {
                let use_index_page = usable_indexes[0].clone();
                let mut rowids = Vec::new();
                for clause in &where_clause {
                    let rows_for_where =
                        index_scan(&mut file, &dbheader, use_index_page, &clause.value);
                    rowids.push(rows_for_where);
                }
                let mut rowids_satisfying_all_wheres =
                    rowids.iter().fold(rowids[0].clone(), |acc, rowids| {
                        acc.iter()
                            .filter(|r| rowids.contains(r))
                            .map(|r| *r)
                            .collect()
                    });
                records = Vec::new();
                for rowid in rowids_satisfying_all_wheres {
                    records.extend(row_lookup(
                        &mut file,
                        &dbheader,
                        table.unwrap().rootpage,
                        rowid,
                    ));
                }
            } else {
                records = full_table_scan(&mut file, &dbheader, table.unwrap().rootpage);
            }

            'outer: for record in records {
                for (i, where_idx) in indices_of_where_cols.iter().enumerate() {
                    let where_col_val = &record.values[*where_idx];
                    let where_clause_val = &where_clause
                        .iter()
                        .find(|c| c.column == where_clause[i].column)
                        .unwrap()
                        .value;
                    match where_col_val {
                        SqlValue::Text(ref val) => {
                            if val != where_clause_val {
                                continue 'outer;
                            }
                        }
                        SqlValue::Null => continue 'outer,
                        _ => panic!("Only text values are supported for now"),
                    }
                }
                let mut selected_record_cols = Vec::new();
                if cols.contains(&"id".to_string()) {
                    selected_record_cols.push(record.rowid().unwrap().to_string());
                }
                for ix in &indices_of_selected_cols {
                    match record.values[*ix] {
                        SqlValue::Text(ref val) => {
                            selected_record_cols.push(val.clone());
                        }
                        SqlValue::Null => continue,
                        _ => panic!("Only text values are supported for now"),
                    }
                }
                let cols_as_joined_str = selected_record_cols.join("|");
                println!("{}", cols_as_joined_str);
            }
        }
        _ => panic!("Unknown command: {}", command),
    }

    Ok(())
}
