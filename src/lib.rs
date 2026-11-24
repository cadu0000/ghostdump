//! GhostDump masks the values of a SQL dump in one streaming pass, with
//! rules per column, and copies everything else byte for byte.
//!
//! This library is the engine of the `ghostdump` binary: [`parser`] splits
//! the dump into rows and everything else, [`codec`] reads and writes the
//! fields of a row, [`engine`] applies the rules, [`io`] opens the input and
//! the output, [`inventory`] lists the tables of a dump for `ghostdump init`,
//! and [`cli`] holds the command line.

#![warn(missing_docs)]

pub mod cli;
pub mod codec;
pub mod engine;
pub mod inventory;
pub mod io;
pub mod parser;
