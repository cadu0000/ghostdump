//! The states of the parser, one per part of a dump it reads.

use crate::parser::lexer::ByteClass;

mod framer;
mod header;
mod normal;
mod values;

pub use header::{InsertHeaderEvent, InsertHeaderState};
pub use normal::{NormalEvent, NormalState};
pub use values::{ValueEvent, ValueState};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SqlDialect {
    Mysql,
    Postgres,
    Sqlite,
}

pub enum State {
    Normal(NormalState),
    InsertHeader(InsertHeaderState),
    ValueMode(ValueState),
}

/// Statement keywords that may open a data block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataKeyword {
    Insert,
    Copy,
}

/// What the parser still holds when the input ends.
#[derive(Debug, PartialEq)]
pub enum Remainder {
    /// Buffered bytes outside data rows, to be written unchanged.
    Bytes(Vec<u8>),
    /// A statement held for checking, without its end (see
    /// [`SqlEvent::Statement`](crate::parser::SqlEvent::Statement)).
    Statement(Vec<u8>),
    /// The input ended inside a data block, before its terminator. When
    /// `partial_row` is true, an incomplete row was dropped: it cannot be
    /// masked, so it must not be written. `footer` holds what came after the
    /// last row (a clause such as `ON CONFLICT ...`), to be checked like a
    /// footer before it is written.
    UnterminatedData { partial_row: bool, footer: Vec<u8> },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InsertFormat {
    Values,
    Copy,
}

/// Whitespace, comments, and a `-` or `/` that may open a comment.
fn is_separator(byte: u8, class: ByteClass) -> bool {
    match class {
        ByteClass::Comment | ByteClass::CommentStart | ByteClass::VersionEnd => true,
        ByteClass::Literal => false,
        ByteClass::Code => byte.is_ascii_whitespace() || byte == b'-' || byte == b'/',
    }
}
