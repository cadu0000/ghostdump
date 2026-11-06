//! Where the engine sends its output: bytes copied as they are, and rows to
//! mask with the plan of their data block.
//!
//! The output is cut into segments of at most [`SEGMENT_ROWS`] masked rows
//! or [`SEGMENT_BYTES`] input bytes. The rows of a segment draw from their
//! own generator, seeded from the run's generator in segment order. The cut
//! depends only on the input, so a seeded run gives the same output however
//! the segments are processed.

use std::io::{self, Write};
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::codec::{self, FieldMasker, SqlValue};
use crate::engine::config::{CompiledColumn, NullPolicy};
use crate::engine::mask::MaskStats;
use crate::parser::state::{InsertFormat, SqlDialect};

/// A segment closes once it holds this many masked rows...
pub const SEGMENT_ROWS: usize = 1024;
/// ...or this many input bytes, whichever comes first.
pub const SEGMENT_BYTES: usize = 256 * 1024;

/// For each position in a data block's column list, the masked column there, if any.
pub type MaskingPlan<'a> = Vec<Option<&'a CompiledColumn>>;

pub trait Sink<'a> {
    /// Bytes written as they are.
    fn write(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// A row whose fields are masked according to `plan`.
    fn mask(
        &mut self,
        row: Vec<u8>,
        format: InsertFormat,
        plan: &Arc<MaskingPlan<'a>>,
    ) -> io::Result<()>;

    /// Writes what is still pending and returns the masking counters.
    fn finish(self) -> io::Result<MaskStats>;
}

/// Says when the current segment is full.
#[derive(Default)]
struct SegmentClock {
    rows: usize,
    bytes: usize,
}

impl SegmentClock {
    /// Counts an item; true when the segment it belongs to is now full.
    fn tick(&mut self, rows: usize, bytes: usize) -> bool {
        self.rows += rows;
        self.bytes += bytes;
        if self.rows >= SEGMENT_ROWS || self.bytes >= SEGMENT_BYTES {
            *self = Self::default();
            true
        } else {
            false
        }
    }
}

/// The generator of the next segment.
fn segment_rng(run: &mut StdRng) -> StdRng {
    StdRng::from_seed(run.r#gen())
}

/// Masks the rows on the calling thread and writes everything in place.
pub struct SequentialSink<'s, W> {
    writer: W,
    run_rng: StdRng,
    rng: StdRng,
    clock: SegmentClock,
    dialect: SqlDialect,
    secret: &'s str,
    stats: MaskStats,
    row_buffer: Vec<u8>,
}

impl<'s, W: Write> SequentialSink<'s, W> {
    pub fn new(writer: W, mut run_rng: StdRng, dialect: SqlDialect, secret: &'s str) -> Self {
        let rng = segment_rng(&mut run_rng);
        Self {
            writer,
            run_rng,
            rng,
            clock: SegmentClock::default(),
            dialect,
            secret,
            stats: MaskStats::default(),
            row_buffer: Vec::with_capacity(1024),
        }
    }

    fn tick(&mut self, rows: usize, bytes: usize) {
        if self.clock.tick(rows, bytes) {
            self.rng = segment_rng(&mut self.run_rng);
        }
    }
}

impl<'a, W: Write> Sink<'a> for SequentialSink<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)?;
        self.tick(0, bytes.len());
        Ok(())
    }

    fn mask(
        &mut self,
        row: Vec<u8>,
        format: InsertFormat,
        plan: &Arc<MaskingPlan<'a>>,
    ) -> io::Result<()> {
        // Build the row in memory: one write instead of one per piece.
        self.row_buffer.clear();
        let mut masker = RowMasker {
            columns: plan,
            rng: &mut self.rng,
            secret: self.secret,
            stats: &mut self.stats,
        };
        codec::rewrite_row(
            &row,
            format,
            self.dialect,
            &mut masker,
            &mut self.row_buffer,
        )?;
        self.writer.write_all(&self.row_buffer)?;
        self.tick(1, row.len());
        Ok(())
    }

    fn finish(mut self) -> io::Result<MaskStats> {
        self.writer.flush()?;
        Ok(self.stats)
    }
}

struct RowMasker<'a, R> {
    columns: &'a [Option<&'a CompiledColumn>],
    rng: &'a mut R,
    secret: &'a str,
    stats: &'a mut MaskStats,
}

impl<R: Rng> FieldMasker for RowMasker<'_, R> {
    fn masks(&self, column: usize) -> bool {
        matches!(self.columns.get(column), Some(Some(_)))
    }

    fn mask(&mut self, column: usize, value: SqlValue) -> SqlValue {
        let Some(Some(config)) = self.columns.get(column) else {
            return value;
        };
        self.stats.masked_fields += 1;
        if value.is_null() && config.null_policy == NullPolicy::Keep {
            return SqlValue::Null;
        }
        config
            .masker
            .apply(value, self.rng, self.secret, self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segments_close_on_rows_or_bytes() {
        let mut clock = SegmentClock::default();
        for _ in 1..SEGMENT_ROWS {
            assert!(!clock.tick(1, 10));
        }
        assert!(clock.tick(1, 10));
        assert!(!clock.tick(0, SEGMENT_BYTES - 1));
        assert!(clock.tick(0, 1));
        assert!(clock.tick(1, SEGMENT_BYTES));
    }
}
