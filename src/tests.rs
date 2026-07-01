//! Shared test-helper factory (fixtures + in-memory transports).
//!
//! Exported from the library — not gated behind `#[cfg(test)]` — so both in-module unit
//! tests and the out-of-crate integration / e2e suites can build fixtures from one place
//! (the house convention; DESIGN.md §22). Prefer the in-memory `tokio::io::duplex` transport
//! here over real sockets wherever a test doesn't specifically exercise the network.
//!
//! Key / signing fixtures arrive with the identity work in M1; today this seeds the transport
//! and path fixtures the later suites build on.

use tokio::io::DuplexStream;

use crate::base::SessionPath;

/// Buffer size for the in-memory [`duplex`] transport (64 KiB each direction).
pub const DUPLEX_BUF: usize = 64 * 1024;

/// A connected in-memory duplex stream pair, standing in for a socket in tests.
#[must_use]
pub fn duplex() -> (DuplexStream, DuplexStream) {
    tokio::io::duplex(DUPLEX_BUF)
}

/// A representative fully-qualified session path fixture (`aaron/workstation/razel`).
#[must_use]
pub fn sample_session_path() -> SessionPath {
    SessionPath::new("aaron", "workstation", "razel")
}

#[cfg(test)]
mod self_tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use pretty_assertions::assert_eq;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn duplex_round_trips_bytes() {
        let (mut a, mut b) = duplex();
        a.write_all(b"ping").await.unwrap();

        let mut buf = [0_u8; 4];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }

    #[test]
    fn sample_session_path_is_the_canonical_triple() {
        assert_eq!(sample_session_path().to_string(), "aaron/workstation/razel");
    }
}
