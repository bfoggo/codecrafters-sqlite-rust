use anyhow::{bail, Result};
use std::any::Any;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use syntax::statement::Statement;

mod utils;
use utils::decode_varint;
mod typecodes;
use typecodes::{decode_serial_types, SqlValue, TypeCode};
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
            let schema = SqliteSchema::from_page(&page)?;
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
            let schema = SqliteSchema::from_page(&page)?;
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
            let schema = SqliteSchema::from_page(&page)?;
            let parsed_select_stmt = syntax::parse(&select_rows);
            let (cols, tablename) = match parsed_select_stmt {
                Statement::Select(stmt) => (stmt.columns, stmt.table),
                _ => panic!("Expected Select statement"),
            };
            let table = schema.tables.iter().find(|t| t.name == tablename);
            let parsed_table_sql = syntax::parse(&table.unwrap().sql);
            let table_schema = match parsed_table_sql {
                Statement::CreateTable(stmt) => TableSchema::from_ast(&stmt),
                _ => panic!("Expected CreateTable statement"),
            };
            let mut indices_of_selected_cols = Vec::new();
            for selected_col in cols {
                let index_of_selected_col = table_schema
                    .columns
                    .iter()
                    .position(|c| c.name == selected_col);
                indices_of_selected_cols.push(index_of_selected_col.unwrap());
            }
            let table_page = Page::from_file(&mut file, table.unwrap().rootpage, &dbheader)?;
            for i in 0..table_page.header.num_cells {
                let record = read_record(&table_page, i as usize)?;
                let mut selected_record_cols = Vec::new();
                for ix in &indices_of_selected_cols {
                    match record.values[*ix] {
                        SqlValue::Text(ref val) => {
                            selected_record_cols.push(val.clone());
                        }
                        _ => panic!("Only text values are supported for now"),
                    }
                }
                let cols_as_joined_str = selected_record_cols.join("|");
                println!("{}", cols_as_joined_str);
            }
        }
        _ => bail!("Unknown command: {}", command),
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
    reserved_region: Option<u32>,
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
                reserved_region,
            },
            8 + match reserved_region {
                Some(_) => 4,
                None => 0,
            },
        ))
    }

    fn len(&self) -> usize {
        8 + match self.reserved_region {
            Some(_) => 4,
            None => 0,
        }
    }
}

#[derive(Debug)]
struct SqliteSchema {
    tables: Vec<Table>,
}

impl SqliteSchema {
    fn from_page(page: &Page) -> Result<SqliteSchema> {
        let mut tables = Vec::new();
        for i in 0..page.header.num_cells {
            let record = read_record(page, i as usize)?;
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
}

impl Column {
    fn from_ast(ast: &syntax::create_table::ColumnDef) -> Column {
        Column {
            name: ast.name.clone(),
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

fn read_record(page: &Page, cell_index: usize) -> Result<Record> {
    let cell_offset = page.pointer_array[cell_index] as usize;
    let (payload_size, payload_size_len) = decode_varint(&page.data[cell_offset..]);
    let (rowid, rowid_len) = decode_varint(&page.data[cell_offset + payload_size_len..]);
    let payload_start = cell_offset + payload_size_len + rowid_len;
    let payload = page.data[payload_start..].to_vec();
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
