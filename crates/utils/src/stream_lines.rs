use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    io::StreamReader,
};

/// OS read boundaries may split a UTF-8 code point. Preserve the incomplete
/// suffix for the next read instead of silently inserting replacement bytes.
/// Non-text output is a recording error, not a falsely faithful transcript.
pub fn utf8_chunks<S>(stream: S) -> futures::stream::BoxStream<'static, std::io::Result<String>>
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Unpin + 'static,
{
    futures::stream::try_unfold(
        (stream, Vec::<u8>::new(), false),
        |(mut stream, mut pending, mut ended)| async move {
            loop {
                if !pending.is_empty() {
                    match std::str::from_utf8(&pending) {
                        Ok(_) => {
                            let text = String::from_utf8(std::mem::take(&mut pending)).unwrap();
                            return Ok(Some((text, (stream, pending, ended))));
                        }
                        Err(error) if error.valid_up_to() > 0 => {
                            let tail = pending.split_off(error.valid_up_to());
                            let text = String::from_utf8(pending).unwrap();
                            return Ok(Some((text, (stream, tail, ended))));
                        }
                        Err(error) if error.error_len().is_some() || ended => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "native output is not valid UTF-8; recording incomplete",
                            ));
                        }
                        Err(_) => {} // At most three bytes await the next read.
                    }
                }
                if ended {
                    return Ok(None);
                }
                match stream.try_next().await? {
                    Some(bytes) => pending.extend_from_slice(&bytes),
                    None => ended = true,
                }
            }
        },
    )
    .boxed()
}

/// Extension trait for converting chunked string streams to line streams.
pub trait LinesStreamExt: Stream<Item = Result<String, std::io::Error>> + Sized {
    /// Convert a chunked string stream to a line stream.
    fn lines(self) -> futures::stream::BoxStream<'static, std::io::Result<String>>
    where
        Self: Send + 'static,
    {
        let reader = StreamReader::new(self.map(|result| result.map(Bytes::from)));
        FramedRead::new(reader, LinesCodec::new())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            .boxed()
    }
}

impl<S> LinesStreamExt for S where S: Stream<Item = Result<String, std::io::Error>> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_byte_boundary_preserves_multibyte_provider_text() {
        let text = "plain é 中文 🦀 tail\n";
        for boundary in 0..=text.len() {
            let chunks = vec![
                Ok(Bytes::copy_from_slice(&text.as_bytes()[..boundary])),
                Ok(Bytes::copy_from_slice(&text.as_bytes()[boundary..])),
            ];
            let output: Vec<String> = utf8_chunks(futures::stream::iter(chunks))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(output.concat(), text, "boundary {boundary}");
        }
    }

    #[tokio::test]
    async fn invalid_or_truncated_text_is_an_explicit_error_after_retained_prefix() {
        for bytes in [b"retained\xff".to_vec(), b"retained\xe4\xb8".to_vec()] {
            let mut stream = utf8_chunks(futures::stream::iter(vec![Ok(Bytes::from(bytes))]));
            assert_eq!(stream.next().await.unwrap().unwrap(), "retained");
            assert!(stream.next().await.unwrap().is_err());
        }
    }
}
