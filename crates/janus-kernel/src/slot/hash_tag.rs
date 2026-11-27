//! Redis Cluster hash-tag extraction (`{…}`).

/// Bytes hashed for slot selection.
///
/// Uses the substring between the first `{` and the first following `}`.
/// Empty tags (`{}`) or missing braces hash the whole key.
pub fn hash_tag_bytes(key: &[u8]) -> &[u8] {
    let Some(start) = key.iter().position(|&b| b == b'{') else {
        return key;
    };
    let Some(rel_end) = key[start + 1..].iter().position(|&b| b == b'}') else {
        return key;
    };
    if rel_end == 0 {
        return key;
    }
    &key[start + 1..start + 1 + rel_end]
}

#[cfg(test)]
mod tests {
    use super::hash_tag_bytes;

    #[test]
    fn no_braces_hashes_whole_key() {
        assert_eq!(hash_tag_bytes(b"foo"), b"foo");
    }

    #[test]
    fn extracts_interior_tag() {
        assert_eq!(hash_tag_bytes(b"{user}a"), b"user");
        assert_eq!(hash_tag_bytes(b"a{b}c{d}"), b"b");
    }

    #[test]
    fn empty_tag_hashes_whole_key() {
        assert_eq!(hash_tag_bytes(b"{}x"), b"{}x");
    }

    #[test]
    fn unmatched_open_brace_hashes_whole_key() {
        assert_eq!(hash_tag_bytes(b"{user"), b"{user");
    }
}
