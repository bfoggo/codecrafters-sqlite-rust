use crate::syntax;
use crate::typecodes::{decode_serial_types, SqlValue};
use crate::utils::decode_varint;
use anyhow::Result;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

pub struct DbHeader {
    pub page_size: u16,
}

impl DbHeader {
    pub fn from_file(file: &mut File) -> Result<DbHeader> {
        let mut header = [0; Self::len() as usize];
        file.read_exact(&mut header)?;
        let page_size = u16::from_be_bytes([header[16], header[17]]);
        Ok(DbHeader { page_size })
    }

    pub const fn len() -> u64 {
        100
    }
}

#[derive(Debug)]
pub struct Page {
    offset: u64,
    pub header: PageHeader,
    pointer_array: Vec<u16>,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct PageHeader {
    page_type: u8,
    first_freeblock: u16,
    pub num_cells: u16,
    cell_content_start: u16,
    num_fragments: u8,
    rightmost_pointer: Option<u32>,
}

impl Page {
    pub fn from_file(file: &mut File, page_offset: u64, dbheader: &DbHeader) -> Result<Page> {
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

pub struct OverflowPage {
    data: Vec<u8>,
}

impl OverflowPage {
    pub fn from_file(file: &mut File, page_offset: u64, dbheader: &DbHeader) -> Result<OverflowPage> {
        let mut data = vec![0; dbheader.page_size as usize];
        let start = (page_offset - 1) * (dbheader.page_size as u64);
        file.seek(SeekFrom::Start(start))?;
        file.read_to_end(&mut data).unwrap();
        Ok(OverflowPage { data })
    }
}

impl PageHeader {
    pub fn from_data(data: &[u8]) -> Result<(PageHeader, usize)> {
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

    pub fn len(&self) -> usize {
        8 + match self.rightmost_pointer {
            Some(_) => 4,
            None => 0,
        }
    }

    pub fn is_interior(&self) -> bool {
        self.page_type == 0x02 || self.page_type == 0x05
    }
}

#[derive(Debug)]
pub struct SqliteSchema {
    pub schema_elements: Vec<SqlSchemaElement>,
}

impl SqliteSchema {
    pub fn from_page(file: &mut File, dbheader: &DbHeader, page: &Page) -> Result<SqliteSchema> {
        let mut schema_elements = Vec::new();
        for i in 0..page.header.num_cells {
            let record = read_record(file, dbheader, page, i as usize, 1)?;
            schema_elements.push(SqlSchemaElement::from_row(record)?);
        }
        Ok(SqliteSchema { schema_elements })
    }
}

#[derive(Debug)]
pub struct IndexSchema {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
}

#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn from_ast(ast: &syntax::create_table::CreateTableStmt) -> TableSchema {
        let name = ast.table_name.clone();
        let columns = ast.cols().iter().map(Column::from_ast).collect();
        TableSchema { name, columns }
    }

    pub fn primary_key_index(&self) -> Option<usize> {
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
pub struct SqlSchemaElement {
    pub element_type: String,
    pub name: String,
    tbl_name: String,
    pub rootpage: u64,
    pub sql: String,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    is_primary_key: bool,
}

impl Column {
    pub fn from_ast(ast: &syntax::create_table::ColumnDef) -> Column {
        let is_primary_key = ast.constraints.iter().any(|c| c.is_primary_key());
        Column {
            name: ast.name.clone(),
            is_primary_key,
        }
    }
}

impl SqlSchemaElement {
    pub fn from_row(mut row: Record) -> Result<SqlSchemaElement> {
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
        Ok(SqlSchemaElement {
            element_type: tabletype,
            name,
            tbl_name,
            rootpage,
            sql,
        })
    }
}

#[derive(Debug)]
pub enum RecordStart {
    None,
    RowId(u64),
    LeftPage(u32),
}

#[derive(Debug)]
pub struct Record {
    record_start: RecordStart,
    payload_size: u64,
    pub values: Vec<SqlValue>,
}

impl Record {
    pub fn rowid(&self) -> Option<u64> {
        match self.record_start {
            RecordStart::RowId(rowid) => Some(rowid),
            _ => None,
        }
    }

    pub fn left_page(&self) -> Option<u32> {
        match self.record_start {
            RecordStart::LeftPage(left_page) => Some(left_page),
            _ => None,
        }
    }
}

pub fn full_table_scan(file: &mut File, dbheader: &DbHeader, page_number: u64) -> Vec<Record> {
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
            let record = read_record(file, dbheader, &page, i as usize, 1).unwrap();
            records.push(record);
        }
    }
    records
}

pub fn index_scan(file: &mut File, dbheader: &DbHeader, page_number: u64, key: &str) -> Vec<u64> {
    let mut row_ids = Vec::new();
    let page = Page::from_file(file, page_number, dbheader).unwrap();
    if page.header.is_interior() {
        let mut found_conclusive_subtree = false;
        for i in 0..page.pointer_array.len() {
            let record = read_record(file, dbheader, &page, i as usize, 2).unwrap();
            let found_key = match record.values[0] {
                SqlValue::Text(ref val) => val,
                SqlValue::Null => continue,
                _ => panic!("Only text values are supported for now"),
            };
            if *found_key == key {
                let left_page = record.left_page().unwrap();
                row_ids.append(&mut index_scan(file, dbheader, left_page as u64, key));
            } else if *found_key > key.to_string() {
                row_ids.append(&mut index_scan(
                    file,
                    dbheader,
                    record.left_page().unwrap() as u64,
                    key,
                ));
                found_conclusive_subtree = true;
                break;
            }
        }
        if !found_conclusive_subtree {
            let right_page = page.header.rightmost_pointer.unwrap();
            row_ids.append(&mut index_scan(file, dbheader, right_page as u64, key));
        }
    } else {
        for i in 0..page.header.num_cells {
            let record = read_record(file, dbheader, &page, i as usize, 0).unwrap();
            let found_key = match record.values[0] {
                SqlValue::Text(ref val) => val,
                _ => panic!("Only text values are supported for now"),
            };
            if *found_key == key {
                let rowid = match record.values[1] {
                    SqlValue::I8(val) => row_ids.push(val as u64),
                    SqlValue::I16(val) => row_ids.push(val as u64),
                    SqlValue::I24(val) => row_ids.push(val as u64),
                    SqlValue::I32(val) => row_ids.push(val as u64),
                    SqlValue::I48(val) => row_ids.push(val as u64),
                    SqlValue::I64(val) => row_ids.push(val as u64),
                    _ => panic!("Found a noninteger rowid."),
                };
            }
        }
    }
    row_ids
}

pub fn row_lookup(file: &mut File, dbheader: &DbHeader, page_number: u64, rowid: u64) -> Vec<Record> {
    let mut records = Vec::new();
    let page = Page::from_file(file, page_number, dbheader).unwrap();
    if page.header.is_interior() {
        for i in 0..page.pointer_array.len() {
            let bytes = &page.data[page.pointer_array[i] as usize..];
            let left_page = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
            let (cmpr_rowid, _) = decode_varint(&bytes[4..]);
            if rowid <= cmpr_rowid as u64 {
                records.append(&mut row_lookup(file, dbheader, left_page as u64, rowid));
                break;
            }
        }
        let right_page = page.header.rightmost_pointer.unwrap();
        records.append(&mut row_lookup(file, dbheader, right_page as u64, rowid));
    } else {
        for i in 0..page.header.num_cells {
            let record = read_record(file, dbheader, &page, i as usize, 1).unwrap();
            if record.rowid().unwrap() == rowid {
                records.push(record);
            }
        }
    }
    records
}

pub fn read_record(
    file: &mut File,
    dbheader: &DbHeader,
    page: &Page,
    cell_index: usize,
    record_start_kind: u8,
) -> Result<Record> {
    let cell_offset = page.pointer_array[cell_index] as usize;
    let left_page: Option<u32>;
    let rowid: Option<u64>;
    let payload_size;
    let mut payload_start;
    if record_start_kind == 0 {
        left_page = None;
        rowid = None;
        let (_payload_size, payload_size_len) = decode_varint(&page.data[cell_offset..]);
        payload_size = _payload_size;
        payload_start = cell_offset + payload_size_len;
    } else if record_start_kind == 1 {
        let (_payload_size, payload_size_len) = decode_varint(&page.data[cell_offset..]);
        let (_rowid, rowid_len) = decode_varint(&page.data[cell_offset + payload_size_len..]);
        payload_size = _payload_size;
        payload_start = cell_offset + payload_size_len + rowid_len;
        rowid = Some(_rowid);
        left_page = None;
    } else if record_start_kind == 2 {
        rowid = None;
        left_page = Some(u32::from_be_bytes(
            page.data[cell_offset..cell_offset + 4].try_into().unwrap(),
        ));
        let (_payload_size, payload_size_len) = decode_varint(&page.data[cell_offset + 4..]);
        payload_size = _payload_size;
        payload_start = cell_offset + payload_size_len + 4;
    } else {
        panic!("Invalid record start kind");
    }
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
    let (record_headerlen, bytes_for_headerlen) = decode_varint(&payload[0..]);
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
        record_start: if rowid.is_some() {
            RecordStart::RowId(rowid.unwrap())
        } else if left_page.is_some() {
            RecordStart::LeftPage(left_page.unwrap())
        } else {
            RecordStart::None
        },
        payload_size,
        values,
    })
}

pub fn read_overflow(
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
