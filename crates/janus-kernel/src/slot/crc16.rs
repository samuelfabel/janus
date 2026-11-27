//! Redis Cluster CRC-16 (XMODEM): poly `0x1021`, init `0`.

/// CRC-16 used by Redis Cluster for key hashing.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::crc16;

    #[test]
    fn redis_reference_vector() {
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }
}
