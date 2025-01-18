use anyhow::{bail, Result};
use std::cell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use syntax::statement::Statement;

mod utils;
use utils::decode_varint;
mod typecodes;
use typecodes::{decode_serial_types, SqlValue};
mod syntax;
use syntax::tokenizer::tokenize;

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
            for table in schema.tables {
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
            let table = schema.tables.iter().find(|t| t.name == tablename);
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
            let table = schema.tables.iter().find(|t| t.name == tablename);
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
            let records = full_table_scan(&mut file, &dbheader, table.unwrap().rootpage);
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
                    selected_record_cols.push(record.rowid.to_string());
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

struct DbHeader {
    page_size: u16,
}

impl DbHeader {
    fn from_file(file: &mut File) -> Result<DbHeader> {
        let mut header = [0; Self::len() as usize];
        file.read_exact(&mut header)?;
        let page_size = u16::from_be_bytes([header[16], header[17]]);
        Ok(DbHeader { page_size })
    }

    const fn len() -> u64 {
        100
    }
}

#[derive(Debug)]
struct Page {
    offset: u64,
    header: PageHeader,
    pointer_array: Vec<u16>,
    data: Vec<u8>,
}

#[derive(Debug)]
struct PageHeader {
    page_type: u8,
    first_freeblock: u16,
    num_cells: u16,
    cell_content_start: u16,
    num_fragments: u8,
    rightmost_pointer: Option<u32>,
}

impl Page {
    fn from_file(file: &mut File, page_offset: u64, dbheader: &DbHeader) -> Result<Page> {
        let mut data = vec![0; dbheader.page_size as usize];
        let start = (page_offset - 1) * (dbheader.page_size as u64);
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut data)?;
        let dbheader_offset = if page_offset == 1 { 100 } else { 0 };
        let (header, headerlen) =
            PageHeader::from_data(&data[dbheader_offset..dbheader_offset + 12])?;
        let mut pointer_array = Vec::new();
        for i in 0..header.num_cells {
            let offset = dbheader_offset + headerlen + (i * 2) as usize;
            pointer_array.push(u16::from_be_bytes([data[offset], data[offset + 1]]));
        }
        Ok(Page {
            offset: page_offset,
            header,
            pointer_array,
            data: data,
        })
    }
}

struct OverflowPage {
    data: Vec<u8>,
}

impl OverflowPage {
    fn from_file(file: &mut File, page_offset: u64, dbheader: &DbHeader) -> Result<OverflowPage> {
        let mut data = vec![0; dbheader.page_size as usize];
        let start = (page_offset - 1) * (dbheader.page_size as u64);
        file.seek(SeekFrom::Start(start))?;
        file.read_to_end(&mut data).unwrap();
        Ok(OverflowPage { data })
    }
}

impl PageHeader {
    fn from_data(data: &[u8]) -> Result<(PageHeader, usize)> {
        let page_type = data[0];
        let first_freeblock = u16::from_be_bytes([data[1], data[2]]);
        let num_cells = u16::from_be_bytes([data[3], data[4]]);
        let cell_content_start = u16::from_be_bytes([data[5], data[6]]);
        let num_fragments = data[7];
        let reserved_region = match page_type {
            0x02 | 0x05 => Some(u32::from_be_bytes([data[8], data[9], data[10], data[11]])),
            _ => None,
        };
        Ok((
            PageHeader {
                page_type,
                first_freeblock,
                num_cells,
                cell_content_start,
                num_fragments,
                rightmost_pointer: reserved_region,
            },
            8 + match reserved_region {
                Some(_) => 4,
                None => 0,
            },
        ))
    }

    fn len(&self) -> usize {
        8 + match self.rightmost_pointer {
            Some(_) => 4,
            None => 0,
        }
    }

    fn is_interior(&self) -> bool {
        self.page_type == 0x02 || self.page_type == 0x05
    }
}

#[derive(Debug)]
struct SqliteSchema {
    tables: Vec<Table>,
}

impl SqliteSchema {
    fn from_page(file: &mut File, dbheader: &DbHeader, page: &Page) -> Result<SqliteSchema> {
        let mut tables = Vec::new();
        for i in 0..page.header.num_cells {
            let record = read_record(file, dbheader, page, i as usize)?;
            tables.push(Table::from_row(record)?);
        }
        Ok(SqliteSchema { tables })
    }
}

#[derive(Debug)]
struct TableSchema {
    name: String,
    columns: Vec<Column>,
}

impl TableSchema {
    fn from_ast(ast: &syntax::create_table::CreateTableStmt) -> TableSchema {
        let name = ast.table_name.clone();
        let columns = ast.cols().iter().map(Column::from_ast).collect();
        TableSchema { name, columns }
    }

    fn primary_key_index(&self) -> Option<usize> {
        let mut i = 0;
        let mut pkeys = Vec::new();
        for col in &self.columns {
            if col.is_primary_key {
                pkeys.push(i);
            }
            i += 1;
        }
        return match pkeys.len() {
            0 => None,
            1 => Some(pkeys[0]),
            _ => panic!("Multiple primary keys not supported"),
        };
    }
}

#[derive(Debug)]
struct Table {
    tabletype: String,
    name: String,
    tbl_name: String,
    rootpage: u64,
    sql: String,
}

#[derive(Debug)]
struct Column {
    name: String,
    is_primary_key: bool,
}

impl Column {
    fn from_ast(ast: &syntax::create_table::ColumnDef) -> Column {
        let is_primary_key = ast.constraints.iter().any(|c| c.is_primary_key());
        Column {
            name: ast.name.clone(),
            is_primary_key,
        }
    }
}

impl Table {
    fn from_row(mut row: Record) -> Result<Table> {
        let sql = match row.values.pop().unwrap() {
            SqlValue::Text(strval) => strval,
            _ => panic!("Expected a string value, got"),
        };
        let rootpage = match row.values.pop().unwrap() {
            SqlValue::I8(val) => val as u64,
            SqlValue::I16(val) => val as u64,
            SqlValue::I24(val) => val as u64,
            SqlValue::I32(val) => val as u64,
            SqlValue::I48(val) => val as u64,
            SqlValue::I64(val) => val as u64,
            _ => panic!("Expected an integer value, got"),
        };
        let tbl_name = match row.values.pop().unwrap() {
            SqlValue::Text(strval) => strval,
            _ => panic!("Expected a string value, got"),
        };
        let name = match row.values.pop().unwrap() {
            SqlValue::Text(strval) => strval,
            _ => panic!("Expected a string value, got"),
        };
        let tabletype = match row.values.pop().unwrap() {
            SqlValue::Text(strval) => strval,
            _ => panic!("Expected a string value, got"),
        };
        Ok(Table {
            tabletype,
            name,
            tbl_name,
            rootpage,
            sql,
        })
    }
}

#[derive(Debug)]
struct Record {
    rowid: u64,
    payload_size: u64,
    values: Vec<SqlValue>,
}

fn btree_pagenum_search(
    file: &mut File,
    dbheader: &DbHeader,
    page_number: u64,
    search_key: u64,
) -> u64 {
    let page = Page::from_file(file, page_number as u64, &dbheader).unwrap();
    if !page.header.is_interior() {
        return page_number;
    }
    let mut i = 0;
    while i < page.pointer_array.len() {
        let bytes = &page.data[page.pointer_array[i] as usize..];
        let left_page = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let (key, _) = decode_varint(&bytes[4..12]);
        if search_key < key {
            return btree_pagenum_search(file, dbheader, left_page as u64, search_key);
        }
        i += 1;
    }
    return page.header.rightmost_pointer.unwrap() as u64;
}

fn full_table_scan(file: &mut File, dbheader: &DbHeader, page_number: u64) -> Vec<Record> {
    let mut records = Vec::new();
    let page = Page::from_file(file, page_number, dbheader).unwrap();
    if page.header.is_interior() {
        for i in 0..page.pointer_array.len() {
            let bytes = &page.data[page.pointer_array[i] as usize..];
            let left_page = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
            records.append(&mut full_table_scan(file, dbheader, left_page as u64));
        }
        let right_page = page.header.rightmost_pointer.unwrap();
        records.append(&mut full_table_scan(file, dbheader, right_page as u64));
    } else {
        for i in 0..page.header.num_cells {
            let record = read_record(file, dbheader, &page, i as usize).unwrap();
            records.push(record);
        }
    }
    records
}

fn read_record(
    file: &mut File,
    dbheader: &DbHeader,
    page: &Page,
    cell_index: usize,
) -> Result<Record> {
    let cell_offset = page.pointer_array[cell_index] as usize;
    let (payload_size, payload_size_len) = decode_varint(&page.data[cell_offset..]);
    let (rowid, rowid_len) = decode_varint(&page.data[cell_offset + payload_size_len..]);
    let payload_start = cell_offset + payload_size_len + rowid_len;
    let overflow = page.data.len() - payload_start - payload_size as usize;
    let overflow_page = u32::from_be_bytes(
        page.data[cell_offset + payload_size as usize - 4..cell_offset + payload_size as usize]
            .try_into()
            .unwrap(),
    );
    let mut overflow_data = Vec::new();
    if overflow > 0 {
        overflow_data = read_overflow(file, dbheader, overflow_page as u64, overflow);
    }
    let mut payload = page.data[payload_start..].to_vec();
    payload.append(&mut overflow_data);
    let (record_headerlen, bytes_for_headerlen) = decode_varint(&payload[0..9]);
    let serial_types =
        decode_serial_types(&payload[bytes_for_headerlen..record_headerlen as usize]);
    let mut offset = record_headerlen as usize;
    let mut values: Vec<SqlValue> = Vec::with_capacity(serial_types.len());

    for t in serial_types {
        let size = t.size();
        let data = &payload[offset..offset + size];
        offset += size;
        values.push(t.decode(data));
    }
    Ok(Record {
        rowid,
        payload_size,
        values,
    })
}

fn read_overflow(
    file: &mut File,
    dbheader: &DbHeader,
    page_number: u64,
    num_overflow_bytes: usize,
) -> Vec<u8> {
    let mut overflow = Vec::new();
    let page = OverflowPage::from_file(file, page_number, dbheader).unwrap();
    overflow.append(&mut page.data[4..4 + num_overflow_bytes].to_vec());
    overflow
}
