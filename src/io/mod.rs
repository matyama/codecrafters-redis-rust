pub(crate) use reader::{DataReader, RDBFileReader};
pub(crate) use writer::DataWriter;

pub(crate) mod reader;
pub(crate) mod writer;

pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]
