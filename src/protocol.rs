//! Wire frames shared between the bridge and the central server.
//!
//! This is the schema both peers serialize: control frames (register, join, admin,
//! presence) and data frames (channel message, whisper). Two properties are fixed by
//! DESIGN.md §13 and reserved here for forward-compat, so later additions are additive
//! rather than breaking:
//!
//! - a **protocol-version field** negotiated at connect (`Constant::PROTOCOL_VERSION`);
//!   peers advertising an incompatible version are rejected or upgraded,
//! - an opaque **encrypted-payload envelope + key-id** on the data frame, so end-to-end
//!   encryption (DESIGN.md §19) can be layered in without a wire break.
//!
//! The typed, wire-crossing `ProtocolError` boundary and the frame types themselves land
//! in M1; this module is a stub until then.
