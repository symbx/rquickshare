use std::fmt::Debug;

use tokio::io::{AsyncRead, AsyncWrite};

/// Byte stream a Nearby session runs over (TCP today; RFCOMM or an upgraded Wi-Fi socket later).
pub trait Transport: AsyncRead + AsyncWrite + Unpin + Send + Sync + Debug {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync + Debug> Transport for T {}

pub type BoxedTransport = Box<dyn Transport>;
