//! The little RLP the trie needs: split a list or string, count items,
//! encode strings, lists and unsigned integers.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Str,
    List,
}

/// Splits the first item off `b`: its kind, its content, and the rest.
pub fn split(b: &[u8]) -> Option<(Kind, &[u8], &[u8])> {
    let &first = b.first()?;
    let (kind, hlen, clen) = match first {
        0..=0x7f => (Kind::Str, 0usize, 1usize),
        0x80..=0xb7 => (Kind::Str, 1, (first - 0x80) as usize),
        0xb8..=0xbf => {
            let n = (first - 0xb7) as usize;
            (Kind::Str, 1 + n, be_len(b.get(1..1 + n)?)?)
        }
        0xc0..=0xf7 => (Kind::List, 1, (first - 0xc0) as usize),
        _ => {
            let n = (first - 0xf7) as usize;
            (Kind::List, 1 + n, be_len(b.get(1..1 + n)?)?)
        }
    };
    if hlen + clen > b.len() {
        return None;
    }
    Some((kind, &b[hlen..hlen + clen], &b[hlen + clen..]))
}

fn be_len(b: &[u8]) -> Option<usize> {
    if b.is_empty() || b.len() > 8 || b[0] == 0 {
        return None;
    }
    Some(b.iter().fold(0usize, |a, &x| a << 8 | x as usize))
}

pub fn split_list(b: &[u8]) -> Option<(&[u8], &[u8])> {
    match split(b)? {
        (Kind::List, c, r) => Some((c, r)),
        _ => None,
    }
}

pub fn split_string(b: &[u8]) -> Option<(&[u8], &[u8])> {
    match split(b)? {
        (Kind::Str, c, r) => Some((c, r)),
        _ => None,
    }
}

pub fn count_values(mut content: &[u8]) -> Option<usize> {
    let mut n = 0;
    while !content.is_empty() {
        content = split(content)?.2;
        n += 1;
    }
    Some(n)
}

/// Appends the header of a string or list whose payload is `len` bytes.
pub fn put_header(out: &mut Vec<u8>, list: bool, len: usize) {
    let base = if list { 0xc0 } else { 0x80 };
    if len < 56 {
        out.push(base + len as u8);
    } else {
        let be = (len as u64).to_be_bytes();
        let skip = be.iter().position(|&x| x != 0).unwrap();
        out.push(base + 0x37 + (8 - skip) as u8);
        out.extend_from_slice(&be[skip..]);
    }
}

pub fn header_len(len: usize) -> usize {
    if len < 56 {
        1
    } else {
        1 + (8 - ((len as u64).leading_zeros() / 8) as usize)
    }
}

/// Appends the RLP string of `s`.
pub fn put_bytes(out: &mut Vec<u8>, s: &[u8]) {
    if s.len() == 1 && s[0] < 0x80 {
        out.push(s[0]);
    } else {
        put_header(out, false, s.len());
        out.extend_from_slice(s);
    }
}

/// Appends the RLP of an unsigned integer: minimal big-endian bytes.
pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    let be = v.to_be_bytes();
    let skip = (v.leading_zeros() / 8) as usize;
    put_bytes(out, &be[skip..]);
}

/// Reads a canonical unsigned integer from a string's content.
pub fn get_u64(content: &[u8]) -> Option<u64> {
    if content.len() > 8 || (!content.is_empty() && content[0] == 0) {
        return None;
    }
    Some(content.iter().fold(0u64, |a, &x| a << 8 | x as u64))
}

/// RLP string of `s` as a fresh Vec.
pub fn bytes(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() + 3);
    put_bytes(&mut out, s);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_and_lists() {
        assert_eq!(bytes(b""), [0x80]);
        assert_eq!(bytes(b"\x00"), [0x00]);
        assert_eq!(bytes(b"\x7f"), [0x7f]);
        assert_eq!(bytes(b"\x80"), [0x81, 0x80]);
        assert_eq!(bytes(&[1u8; 55]).len(), 56);
        let long = bytes(&[1u8; 56]);
        assert_eq!(&long[..2], &[0xb8, 56]);
        let mut out = vec![];
        put_header(&mut out, true, 1024);
        assert_eq!(out, [0xf9, 0x04, 0x00]);
        assert_eq!(header_len(1024), 3);
        assert_eq!(header_len(55), 1);
        assert_eq!(header_len(56), 2);
        let mut v = vec![];
        put_u64(&mut v, 0);
        put_u64(&mut v, 1);
        put_u64(&mut v, 256);
        assert_eq!(v, [0x80, 0x01, 0x82, 0x01, 0x00]);
        let (c, r) = split_string(&v).unwrap();
        assert_eq!(get_u64(c), Some(0));
        let (c, r) = split_string(r).unwrap();
        assert_eq!(get_u64(c), Some(1));
        let (c, _) = split_string(r).unwrap();
        assert_eq!(get_u64(c), Some(256));
        assert_eq!(count_values(&v), Some(3));
        assert!(split(&[0xb8]).is_none());
        assert!(split(&[0x82, 1]).is_none());
    }
}
