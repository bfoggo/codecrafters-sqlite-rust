use crate::utils::decode_varint;

#[derive(Debug)]
pub enum TypeCode {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Blob(usize),
    Text(usize),
}

impl TypeCode {
    pub fn size(&self) -> usize {
        match self {
            TypeCode::Null => 0,
            TypeCode::I8 => 1,
            TypeCode::I16 => 2,
            TypeCode::I24 => 3,
            TypeCode::I32 => 4,
            TypeCode::I48 => 6,
            TypeCode::I64 => 8,
            TypeCode::F64 => 8,
            TypeCode::Zero => 4,
            TypeCode::One => 4,
            TypeCode::Blob(size) => *size,
            TypeCode::Text(size) => *size,
        }
    }

    pub fn decode(&self, data: &[u8]) -> SqlValue {
        match self {
            TypeCode::Null => SqlValue::Null,
            TypeCode::I8 => SqlValue::I8(data[0] as i8),
            TypeCode::I16 => {
                let val = i16::from_be_bytes([data[0], data[1]]);
                SqlValue::I16(val)
            }
            TypeCode::I24 => {
                let val = i32::from_be_bytes([0, data[0], data[1], data[2]]);
                SqlValue::I24(val)
            }
            TypeCode::I32 => {
                let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                SqlValue::I32(val)
            }
            TypeCode::I48 => {
                let val = i64::from_be_bytes([0, 0, 0, 0, data[0], data[1], data[2], data[3]]);
                SqlValue::I48(val)
            }
            TypeCode::I64 => {
                let val = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                SqlValue::I64(val)
            }
            TypeCode::F64 => {
                let val = f64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                SqlValue::F64(val)
            }
            TypeCode::Zero => {
                // why do we need 4 bytes for this?
                let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                SqlValue::Zero
            }
            TypeCode::One => {
                let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                SqlValue::One
            }
            TypeCode::Blob(size) => {
                let blob = data[0..*size].to_vec();
                SqlValue::Blob(blob)
            }
            TypeCode::Text(size) => {
                let text = String::from_utf8(data[0..*size].to_vec()).unwrap();
                SqlValue::Text(text)
            }
            _ => {
                panic!("Not implemented");
            }
        }
    }
}

#[derive(Debug)]
pub enum SqlValue {
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    F64(f64),
    Zero,
    One,
    Blob(Vec<u8>),
    Text(String),
}

pub fn decode_serial_types(data: &[u8]) -> Vec<(TypeCode)> {
    let mut i = 0;
    let mut serial_types = Vec::new();
    while i < data.len() {
        let (serial_type_code, bytes_for_serial_type) = decode_varint(&data[i..]);
        i += bytes_for_serial_type;
        match serial_type_code {
            0x00 => {
                serial_types.push(TypeCode::Null);
            }
            0x01 => {
                serial_types.push(TypeCode::I8);
            }
            0x02 => {
                serial_types.push(TypeCode::I16);
            }
            0x03 => {
                serial_types.push(TypeCode::I24);
            }
            0x04 => {
                serial_types.push(TypeCode::I32);
            }
            0x06 => {
                serial_types.push(TypeCode::I48);
            }
            0x07 => {
                serial_types.push(TypeCode::I64);
            }
            0x08 => {
                serial_types.push(TypeCode::F64);
            }
            0x09 => {
                serial_types.push(TypeCode::Zero);
            }
            0x0a => {
                serial_types.push(TypeCode::One);
            }
            n if n >= 12 && n % 2 == 0 => {
                let size = (n - 12) / 2;
                serial_types.push(TypeCode::Blob(size as usize));
            }
            n if n >= 13 && n % 2 == 1 => {
                let size = (n - 13) / 2;
                serial_types.push(TypeCode::Text(size as usize));
            }
            _ => {
                panic!("Unknown serial type code: {}", serial_type_code);
            }
        }
    }
    serial_types
}
