use crate::decode::Decode;
use crate::encode::Encode;
use crate::io::Buf;
use crate::postgres::protocol::TypeId;
use crate::postgres::{PgTypeInfo, PgValue, Postgres};
use crate::types::Type;
use byteorder::BigEndian;
use std::convert::TryInto;

impl Type<Postgres> for (bool, i32, i64, f64, &'_ str) {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo {
            id: TypeId(32925),
            name: Some("RECORD".into()),
        }
    }
}

pub struct PgRecordEncoder<'a> {
    buf: &'a mut Vec<u8>,
    beg: usize,
    num: u32,
}

impl<'a> PgRecordEncoder<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> Self {
        // reserve space for a field count
        buf.extend_from_slice(&(0_u32).to_be_bytes());

        Self {
            beg: buf.len(),
            buf,
            num: 0,
        }
    }

    pub fn finish(&mut self) {
        // replaces zeros with actual length
        self.buf[self.beg - 4..self.beg].copy_from_slice(&self.num.to_be_bytes());
    }

    pub fn encode<T>(&mut self, value: T) -> &mut Self
    where
        T: Type<Postgres> + Encode<Postgres>,
    {
        // write oid
        let info = T::type_info();
        self.buf.extend(&info.oid().to_be_bytes());

        // write zeros for length
        self.buf.extend(&[0; 4]);

        let start = self.buf.len();
        value.encode(self.buf);

        let end = self.buf.len();
        let size = end - start;

        // replaces zeros with actual length
        self.buf[start - 4..start].copy_from_slice(&(size as u32).to_be_bytes());

        // keep track of count
        self.num += 1;

        self
    }
}

impl Encode<Postgres> for (bool, i32, i64, f64, &'_ str) {
    fn encode(&self, buf: &mut Vec<u8>) {
        PgRecordEncoder::new(buf)
            .encode(self.0)
            .encode(self.1)
            .encode(self.2)
            .encode(self.3)
            .encode(&self.4)
            .finish()
    }

    fn size_hint(&self) -> usize {
        // for each field; oid, length, value
        5 * (4 + 4)
            + (<bool as Encode<Postgres>>::size_hint(&self.0)
                + <i32 as Encode<Postgres>>::size_hint(&self.1)
                + <i64 as Encode<Postgres>>::size_hint(&self.2)
                + <f64 as Encode<Postgres>>::size_hint(&self.3)
                + <&'_ str as Encode<Postgres>>::size_hint(&self.4))
    }
}

pub struct PgRecordDecoder<'de> {
    value: PgValue<'de>,
}

impl<'de> PgRecordDecoder<'de> {
    pub fn new(mut value: PgValue<'de>) -> crate::Result<Self> {
        match value {
            PgValue::Binary(ref mut buf) => {
                let _expected_len = buf.get_u32::<BigEndian>()?;
            }

            PgValue::Text(ref mut s) => {
                // remove outer ( ... )
                *s = &s[1..s.len() - 1];
            }
        }

        Ok(Self { value })
    }

    pub fn decode<T>(&mut self) -> crate::Result<T>
    where
        T: Decode<'de, Postgres>,
    {
        match self.value {
            PgValue::Binary(ref mut buf) => {
                // TODO: We should fail if this type is not _compatible_; but
                //       I want to make sure we handle this _and_ the outer level
                //       type mismatch errors at the same time
                let _oid = buf.get_u32::<BigEndian>()?;
                let len = buf.get_i32::<BigEndian>()? as isize;

                let value = if len < 0 {
                    T::decode(None)?
                } else {
                    let value_buf = &buf[..(len as usize)];
                    *buf = &buf[(len as usize)..];

                    T::decode(Some(PgValue::Binary(value_buf)))?
                };

                Ok(value)
            }

            PgValue::Text(ref mut s) => {
                let mut in_quotes = false;
                let mut is_quoted = false;
                let mut prev_ch = '\0';
                let mut prev_index = 0;
                let mut value = String::new();

                let index = 'outer: loop {
                    let mut iter = s.char_indices();
                    while let Some((index, ch)) = iter.next() {
                        match ch {
                            ',' if prev_ch == '\0' => {
                                // NULL values have zero characters
                                // Empty strings are ""
                                break 'outer None;
                            }

                            ',' if !in_quotes => {
                                break 'outer Some(index);
                            }

                            '"' if in_quotes => {
                                in_quotes = false;
                            }

                            '"' if prev_ch == '"' => {
                                // Quotes are escaped with another quote
                                in_quotes = false;
                                value.push('"');
                            }

                            '"' => {
                                in_quotes = true;
                                is_quoted = true;
                            }

                            ch => {
                                value.push(ch);
                            }
                        }

                        prev_ch = ch;
                        prev_index = index;
                    }

                    break 'outer if prev_ch == '\0' {
                        None
                    } else {
                        Some(prev_index)
                    };
                };

                let value = index.map(|index| {
                    let mut s = &s[..index];

                    if is_quoted {
                        s = &s[1..s.len()];
                    }

                    PgValue::Text(s)
                });

                let value = T::decode(value)?;
                *s = &s[index.unwrap_or(0) + 1..];

                Ok(value)
            }
        }
    }
}

// TODO: Generalize over tuples
impl<'de, T5> Decode<'de, Postgres> for (bool, i32, i64, f64, T5)
where
    T5: 'de + Decode<'de, Postgres>,
{
    fn decode(value: Option<PgValue<'de>>) -> crate::Result<Self> {
        let mut decoder = PgRecordDecoder::new(value.try_into()?)?;

        let _1 = decoder.decode()?;
        let _2 = decoder.decode()?;
        let _3 = decoder.decode()?;
        let _4 = decoder.decode()?;
        let _5 = decoder.decode()?;

        Ok((_1, _2, _3, _4, _5))
    }
}
