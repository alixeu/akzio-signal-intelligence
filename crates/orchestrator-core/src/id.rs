use md5::{Digest, Md5};

/// First three bytes of MD5, rendered as six lowercase hexadecimal characters.
pub fn md5_3(value: impl AsRef<[u8]>) -> String {
    let digest = Md5::digest(value.as_ref());
    format!("{:02x}{:02x}{:02x}", digest[0], digest[1], digest[2])
}

#[cfg(test)]
mod tests {
    use super::md5_3;

    #[test]
    fn md5_3_is_stable_and_six_hex_characters() {
        assert_eq!(md5_3("akzio"), "d12479");
    }
}
