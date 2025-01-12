pub fn decode_varint(data: &[u8]) -> (u64, usize) {
    let mut result = 0;
    let mut bytes_read = 0;
    for (i, byte) in data.iter().enumerate() {
        bytes_read += 1;
        if i == 8 {
            result = result << 8 | *byte as i64;
            break;
        }
        result = (result << 7 | (*byte & 0b0111_1111) as i64);
        if *byte < 0b1000_0000 {
            break;
        }
    }
    (result as u64, bytes_read)
}
