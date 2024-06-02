pub(crate) use reader::{DataReader, RDBFileReader};
pub(crate) use writer::DataWriter;

use crc_table::CRC64_TAB;

pub(crate) mod reader;
pub(crate) mod writer;

mod crc_table;

pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]

#[macro_export]
macro_rules! write_fmt {
    ($buf:ident, $fmt:literal) => {{
        use std::io::{self, Write as _};
        let mut w = io::Cursor::new(&mut $buf[..]);
        write!(w, $fmt).map(move |_| w.position() as usize)
    }};

    ($buf:ident, $fmt:literal, $($arg:expr),*) => {{
        use std::io::{self, Write as _};
        let mut w = io::Cursor::new(&mut $buf[..]);
        write!(w, $fmt, $($arg),*).map(move |_| w.position() as usize)
    }};
}

fn crc64(crc: u64, data: &[u8]) -> u64 {
    let (data, last) = data.split_at(data.len() - (data.len() % 8));

    let crc = data.chunks(8).fold(crc, |mut crc, chunk| {
        crc ^= u64::from_le_bytes(chunk.try_into().unwrap());
        CRC64_TAB[7][(crc & 0xff) as usize]
            ^ CRC64_TAB[6][((crc >> 8) & 0xff) as usize]
            ^ CRC64_TAB[5][((crc >> 16) & 0xff) as usize]
            ^ CRC64_TAB[4][((crc >> 24) & 0xff) as usize]
            ^ CRC64_TAB[3][((crc >> 32) & 0xff) as usize]
            ^ CRC64_TAB[2][((crc >> 40) & 0xff) as usize]
            ^ CRC64_TAB[1][((crc >> 48) & 0xff) as usize]
            ^ CRC64_TAB[0][(crc >> 56) as usize]
    });

    last.iter().fold(crc, |crc, &b| {
        CRC64_TAB[0][((crc ^ b as u64) & 0xff) as usize] ^ (crc >> 8)
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Checksum(u64);

impl std::io::Write for Checksum {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 = crc64(self.0, buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl From<Checksum> for u64 {
    #[inline]
    fn from(Checksum(cksum): Checksum) -> Self {
        cksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};
    use std::io::Cursor;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;

    use crate::rdb::{self, RDB};
    use crate::store::Database;
    use crate::{DataReader, DataWriter};

    fn assert_rdb_eq(expected: RDB, actual: RDB) {
        assert_eq!(expected.version, actual.version, "version");
        assert_eq!(expected.aux, actual.aux, "aux");

        assert_eq!(expected.dbs.len(), actual.dbs.len(), "store size");
        for (db1, db2) in expected.dbs.into_iter().zip(actual.dbs) {
            assert_eq!(db1.id, db2.id, "indices");

            let mut db1 = db1.kvstore.iter().collect::<Vec<_>>();
            db1.sort_unstable_by_key(|(k, _)| k.to_string());

            let mut db2 = db2.kvstore.iter().collect::<Vec<_>>();
            db2.sort_unstable_by_key(|(k, _)| k.to_string());

            for ((k1, v1), (k2, v2)) in db1.into_iter().zip(db2) {
                assert_eq!(k1, k2, "keys");

                let (v1, e1) = v1.clone().into_inner();
                let (v2, e2) = v2.clone().into_inner();
                assert_eq!(v1, v2, "expiry");
                assert_eq!(e1, e2, "expiry");
            }
        }
    }

    // Rounds millis to `u64` (`u64` is required in the spec and thus written as such)
    macro_rules! exptimems {
        ($t:ident + $d:expr) => {
            UNIX_EPOCH
                + Duration::from_millis(
                    ($t + $d).duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                )
        };
    }

    macro_rules! set_str {
        ([$db:ident]; $($key:expr => $val:expr),+) => {
            $(
                $db.set(
                    Str(Bytes::from_static($key)),
                    rdb::Value::String($val),
                    None,
                );
            )+
        };

        ([$db:ident]; $($key:expr => $val:expr => $t:ident + $d:expr),+) => {
            $(
                $db.set(
                    $key,
                    rdb::Value::String(Str(Bytes::from_static($val))),
                    Some(exptimems!($t + $d)),
                );
            )+
        };
    }

    #[tokio::test]
    async fn write_read_rdb_file() {
        use rdb::String::*;

        const EXPIRED_KEY: Bytes = Bytes::from_static(b"expired");

        let t0 = SystemTime::now();

        let (db, to_expire) = {
            let mut db = Database::builder();
            db.with_id(0);
            db.with_capacity(8);

            // TODO: test efficient storage: write Int32(-10_000) => read Int16(-10_000)
            set_str! { [db];
                b"foo" => Str(Bytes::from_static(b"bar")),
                b"int8" => Int8(42),
                b"int16" => Int16(1_000),
                b"int32" => Int32(100_000),
                b"neg32" => Int32(-100_000)
            }

            db.set(
                Int8(42),
                rdb::Value::String(Str(Bytes::from_static(b"baz"))),
                None,
            );

            set_str! { [db];
                Str(Bytes::from_static(b"tmp")) => b"puf" => t0 + Duration::from_secs(3600),
                Str(EXPIRED_KEY) => b"nothing" => t0 + Duration::from_millis(50)
            }

            // NOTE: String only adjusts internal ref count, but does not change key hash
            #[allow(clippy::mutable_key_type)]
            let mut to_expire = HashSet::new();
            to_expire.insert(Str(EXPIRED_KEY));

            (db.build().expect("test db"), to_expire)
        };

        let dbid = db.id;

        let mut expired = HashMap::new();
        expired.insert(db.id, to_expire);

        let mut dbs = Vec::from_iter((0..16).map(Database::new));
        dbs[dbid] = db;

        let rdb = RDB {
            version: 11.try_into().unwrap(),
            aux: rdb::Aux {
                redis_ver: Some(Str(Bytes::from_static(b"7.2.4"))),
                redis_bits: Some(Int8(64)),
                ctime: Some(Int32(1714503219)),
                used_mem: Some(Int32(915576)),
                repl_stream_db: None,
                repl_id: None,
                repl_offset: None,
                aof_base: Some(Int8(0)),
            },
            dbs,
        };

        let input = rdb.clone().remove(expired);

        let buf = Vec::with_capacity(1024);
        let mut writer = DataWriter::new(buf);

        let bytes_written = writer.write_rdb_file(rdb).await.expect("write RDB file");
        writer.flush().await.expect("flush RDB file");

        // let some keys expire between write and read
        tokio::time::sleep(Duration::from_millis(100)).await;

        let rdb = writer.into_inner();
        let mut reader = DataReader::new(Cursor::new(rdb));

        let (output, bytes_read) = reader.read_rdb_file().await.expect("read RDB file");

        assert_eq!(bytes_written, bytes_read, "bytes written/read");
        assert_rdb_eq(input, output);
    }

    #[test]
    fn crc64_works() {
        assert_eq!(0xe9c6d914c4b8d9ca, crc64(0, "123456789".as_bytes()));

        let step1 = "12345".as_bytes();
        let step2 = "6789".as_bytes();

        let value1 = 17326901458626182669;
        let value2 = 16845390139448941002;

        assert_eq!(value1, crc64(0, step1));
        assert_eq!(value2, crc64(value1, step2));
    }
}
