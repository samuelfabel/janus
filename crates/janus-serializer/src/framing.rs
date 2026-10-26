/// Find the position of the first CRLF in the given byte slice.
///
/// # Arguments
/// * `slice` - The byte slice to search for CRLF
///
/// # Returns
/// The position of the first CRLF, or None if not found.
#[inline]
pub fn find_crlf(slice: &[u8]) -> Option<usize> {
    slice.windows(2).position(|w| w == b"\r\n")
}

/// Parse a byte slice representing an integer into a usize.
///
/// # Arguments
/// * `slice` - The byte slice to parse
///
/// # Returns
/// The parsed integer, or an error if the input is invalid.
#[inline]
pub fn parse_integer(slice: &[u8]) -> Result<usize, ()> {
    let mut num = 0;
    for &b in slice {
        if b.is_ascii_digit() {
            num = num * 10 + (b - b'0') as usize;
        } else {
            return Err(());
        }
    }
    Ok(num)
}

/// Count the number of digits in a positive integer.
///
/// # Arguments
/// * `num` - The positive integer to count digits for
///
/// # Returns
/// The number of digits in the integer.
#[inline]
pub fn count_digits(mut num: usize) -> usize {
    if num == 0 {
        return 1;
    }
    let mut digits = 0;
    while num > 0 {
        digits += 1;
        num /= 10;
    }
    digits
}
