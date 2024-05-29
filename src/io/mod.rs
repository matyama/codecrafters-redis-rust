pub(crate) use reader::{DataReader, RDBFileReader};
pub(crate) use writer::DataWriter;

pub(crate) mod reader;
pub(crate) mod writer;

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

#[cfg(test)]
mod tests {
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

            let mut db1 = db1.iter().collect::<Vec<_>>();
            db1.sort_unstable_by_key(|(k, _)| k.to_string());

            let mut db2 = db2.iter().collect::<Vec<_>>();
            db2.sort_unstable_by_key(|(k, _)| k.to_string());

            for ((k1, v1), (k2, v2)) in db1.into_iter().zip(db2) {
                assert_eq!(k1, k2, "keys");

                let (v1, e1) = v1.clone().into_inner();
                let (v2, e2) = v2.clone().into_inner();
                assert_eq!(v1, v2, "expiry");
                assert_eq!(e1, e2, "expiry");
            }
        }

        assert_eq!(expected.checksum, actual.checksum, "checksum");
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
            checksum: Some(Bytes::from_static(b"\xba\x98\x18\x82\xd2\x98\x96\xbc")),
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
}
