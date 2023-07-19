pub const MAX_LENGTH: usize = 268435455;

pub fn read_size(buf: &[u8]) -> (usize, usize) {
    let mut idx = 0;
    let mut lensize: usize = 1;
    let mut len: usize = (buf[idx] as usize) & 0x7F;
    while (buf[idx] & 0x80) == 0x80 {
        idx += 1;
        len |= ((buf[idx] as usize) & 0x7F) << (7 * lensize);
        lensize += 1;
    }

    (len, lensize)
}

pub fn read_size_check(buf: &[u8]) -> Result<Option<(usize, usize)>, ()> {
    let mut idx = 0;
    let mut lensize: usize = 1;
    if buf.len() < idx + 1 {
        return Ok(None);
    }
    let mut len: usize = (buf[idx] as usize) & 0x7F;
    while (buf[idx] & 0x80) == 0x80 {
        idx += 1;
        if buf.len() < idx + 1 {
            return Ok(None);
        }
        len |= ((buf[idx] as usize) & 0x7F) << (7 * lensize);
        lensize += 1;
        if lensize > 4 || (lensize == 4 && (buf[idx] & 0x80 == 0x80)) {
            return Err(());
        }
    }

    Ok(Some((len, lensize)))
}

pub fn encode_length(mut length: usize) -> Vec<u8> {
    let mut encoded: Vec<u8> = Vec::new();

    loop {
        let mut encoded_byte: usize = length % 128;

        length /= 128;

        if length > 0 {
            encoded_byte |= 128;
        }

        encoded.push((encoded_byte & 0xff) as u8);

        if length == 0 {
            break;
        }
    }

    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_one_byte_min() {
        assert_eq!(read_size(&[0x00]), (0, 1));
    }

    #[test]
    fn test_decode_one_byte_max() {
        assert_eq!(read_size(&[0x7f]), (127, 1));
    }

    #[test]
    fn test_decode_two_byte_min() {
        assert_eq!(read_size(&[0x80, 0x01]), (128, 2));
    }

    #[test]
    fn test_decode_two_byte_max() {
        assert_eq!(read_size(&[0xff, 0x7f]), (16383, 2));
    }

    #[test]
    fn test_decode_three_byte_min() {
        assert_eq!(read_size(&[0x80, 0x80, 0x01]), (16384, 3));
    }

    #[test]
    fn test_decode_three_byte_max() {
        assert_eq!(read_size(&[0xff, 0xff, 0x7f]), (2097151, 3));
    }

    #[test]
    fn test_decode_four_byte_min() {
        assert_eq!(read_size(&[0x80, 0x80, 0x80, 0x01]), (2097152, 4));
    }

    #[test]
    fn test_decode_four_byte_max() {
        assert_eq!(read_size(&[0xff, 0xff, 0xff, 0x7f]), (268435455, 4));
    }

    #[test]
    fn test_encode_one_byte_min() {
        assert_eq!(encode_length(0), &[0x00]);
    }

    #[test]
    fn test_encode_one_byte_max() {
        assert_eq!(encode_length(127), &[0x7f]);
    }

    #[test]
    fn test_encode_two_byte_min() {
        assert_eq!(encode_length(128), &[0x80, 0x01]);
    }

    #[test]
    fn test_encode_two_byte_max() {
        assert_eq!(encode_length(16383), &[0xff, 0x7f]);
    }

    #[test]
    fn test_encode_three_byte_min() {
        assert_eq!(encode_length(16384), &[0x80, 0x80, 0x01]);
    }

    #[test]
    fn test_encode_three_byte_max() {
        assert_eq!(encode_length(2097151), &[0xff, 0xff, 0x7f]);
    }

    #[test]
    fn test_encode_four_byte_min() {
        assert_eq!(encode_length(2097152), &[0x80, 0x80, 0x80, 0x01]);
    }

    #[test]
    fn test_encode_four_byte_max() {
        assert_eq!(encode_length(268435455), &[0xff, 0xff, 0xff, 0x7f]);
    }

    #[test]
    fn test_length_check() {
        assert_eq!(read_size_check(&[]), Ok(None));
        assert_eq!(read_size_check(&[0xff]), Ok(None));
        assert_eq!(read_size_check(&[0xff, 0xff]), Ok(None));
        assert_eq!(read_size_check(&[0xff, 0xff, 0xff]), Ok(None));
        assert_eq!(
            read_size_check(&[0xff, 0xff, 0xff, 0x7f]),
            Ok(Some((268435455, 4)))
        );

        assert_eq!(read_size_check(&[0x00]), Ok(Some((0, 1))));
        assert_eq!(read_size_check(&[0x80, 0x01]), Ok(Some((128, 2))));
        assert_eq!(read_size_check(&[0xff, 0x7f]), Ok(Some((16383, 2))));
        assert_eq!(read_size_check(&[0x80, 0x80, 0x01]), Ok(Some((16384, 3))));
        assert_eq!(
            read_size_check(&[0x80, 0x80, 0x80, 0x01]),
            Ok(Some((2097152, 4)))
        );
        assert_eq!(read_size_check(&[0x80, 0x80, 0x80, 0x80, 0x00]), Err(()));
        assert_eq!(read_size_check(&[0x80, 0x80, 0x80, 0x80]), Err(()));
    }
}
