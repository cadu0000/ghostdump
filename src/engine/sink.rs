//! Where the engine sends its output: bytes copied as they are, and rows to
//! mask with the plan of their data block.
//!
//! The output is cut into segments of at most [`SEGMENT_ROWS`] masked rows
//! or [`SEGMENT_BYTES`] input bytes. The rows of a segment draw from their
//! own generator, seeded from the run's generator in segment order. The cut
//! depends only on the input, so a seeded run gives the same output however
//! the segments are processed: in place ([`SequentialSink`]) or by a pool of
//! threads ([`ParallelSink`]).

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::mem;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::Scope;

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
    context: RowContext<'s>,
    stats: MaskStats,
    row_buffer: Vec<u8>,
}

impl<'s, W: Write> SequentialSink<'s, W> {
    pub fn new(writer: W, mut run_rng: StdRng, context: RowContext<'s>) -> Self {
        let rng = segment_rng(&mut run_rng);
        Self {
            writer,
            run_rng,
            rng,
            clock: SegmentClock::default(),
            context,
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
        self.context.mask_row(
            &row,
            format,
            plan,
            &mut self.rng,
            &mut self.stats,
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

/// A segment on its way to a worker: the bytes to write, and the rows to
/// mask with where each one goes among them. Segments and their buffers are
/// reused, so the threads do not allocate per row or free what another
/// thread allocated.
struct Segment<'a> {
    bytes: Vec<u8>,
    /// The rows to mask, one after another.
    row_bytes: Vec<u8>,
    rows: Vec<PendingRow>,
    /// The plans of the rows, without repeats.
    plans: Vec<Arc<MaskingPlan<'a>>>,
}

struct PendingRow {
    /// Offset in `bytes` where the masked row is written.
    at: usize,
    /// End of the row in `row_bytes`; it starts where the previous one ends.
    end: usize,
    format: InsertFormat,
    plan: usize,
}

impl<'a> Segment<'a> {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            row_bytes: Vec::new(),
            rows: Vec::new(),
            plans: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty() && self.rows.is_empty()
    }

    fn push_row(&mut self, row: &[u8], format: InsertFormat, plan: &Arc<MaskingPlan<'a>>) {
        if !self
            .plans
            .last()
            .is_some_and(|last| Arc::ptr_eq(last, plan))
        {
            self.plans.push(Arc::clone(plan));
        }
        self.row_bytes.extend_from_slice(row);
        self.rows.push(PendingRow {
            at: self.bytes.len(),
            end: self.row_bytes.len(),
            format,
            plan: self.plans.len() - 1,
        });
    }

    /// Writes the segment to `out` with its rows masked.
    fn mask(
        &self,
        seed: [u8; 32],
        context: RowContext,
        stats: &mut MaskStats,
        out: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut rng = StdRng::from_seed(seed);
        let (mut copied, mut start) = (0, 0);
        for row in &self.rows {
            out.extend_from_slice(&self.bytes[copied..row.at]);
            copied = row.at;
            context.mask_row(
                &self.row_bytes[start..row.end],
                row.format,
                &self.plans[row.plan],
                &mut rng,
                stats,
                out,
            )?;
            start = row.end;
        }
        out.extend_from_slice(&self.bytes[copied..]);
        Ok(())
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.row_bytes.clear();
        self.rows.clear();
        self.plans.clear();
    }
}

struct Job<'a> {
    index: u64,
    seed: [u8; 32],
    segment: Segment<'a>,
    out: Vec<u8>,
}

struct Done<'a> {
    index: u64,
    result: io::Result<()>,
    stats: MaskStats,
    /// Returned for reuse, cleared.
    segment: Segment<'a>,
    out: Vec<u8>,
}

/// Groups the output into segments, masks them on a pool of threads and
/// writes them back in input order, from the calling thread.
///
/// At most `2 × threads` segments are out at a time (queued, being masked
/// or waiting for an earlier one to be written), so memory stays bounded
/// whatever the size of the dump.
pub struct ParallelSink<'a, W> {
    writer: W,
    run_rng: StdRng,
    clock: SegmentClock,
    segment: Segment<'a>,
    jobs: Option<Sender<Job<'a>>>,
    results: Receiver<Done<'a>>,
    /// Masked segments that arrived before an earlier one.
    waiting: BTreeMap<u64, Done<'a>>,
    /// Segments and output buffers ready for reuse.
    free: Vec<(Segment<'a>, Vec<u8>)>,
    sent: u64,
    written: u64,
    max_in_flight: u64,
    stats: MaskStats,
}

impl<'a, W: Write> ParallelSink<'a, W> {
    /// Starts `threads` workers in `scope`. They stop when the sink is
    /// finished or dropped.
    pub fn start<'scope>(
        scope: &'scope Scope<'scope, '_>,
        threads: usize,
        writer: W,
        run_rng: StdRng,
        context: RowContext<'a>,
    ) -> Self
    where
        'a: 'scope,
    {
        let threads = threads.max(1);
        let (jobs, queue) = mpsc::channel::<Job<'a>>();
        let (done, results) = mpsc::channel();
        let queue = Arc::new(Mutex::new(queue));
        for _ in 0..threads {
            let queue = Arc::clone(&queue);
            let done = done.clone();
            scope.spawn(move || work(&queue, &done, context));
        }
        Self {
            writer,
            run_rng,
            clock: SegmentClock::default(),
            segment: Segment::new(),
            jobs: Some(jobs),
            results,
            waiting: BTreeMap::new(),
            free: Vec::new(),
            sent: 0,
            written: 0,
            max_in_flight: 2 * threads as u64,
            stats: MaskStats::default(),
        }
    }

    fn tick(&mut self, rows: usize, bytes: usize) -> io::Result<()> {
        if self.clock.tick(rows, bytes) {
            self.dispatch()?;
        }
        Ok(())
    }

    /// Sends the current segment to the workers, then writes the segments
    /// that are ready, waiting while too many are out.
    fn dispatch(&mut self) -> io::Result<()> {
        let (next, out) = self
            .free
            .pop()
            .unwrap_or_else(|| (Segment::new(), Vec::new()));
        let job = Job {
            index: self.sent,
            seed: self.run_rng.r#gen(),
            segment: mem::replace(&mut self.segment, next),
            out,
        };
        let jobs = self
            .jobs
            .as_ref()
            .expect("jobs are sent until the sink finishes");
        jobs.send(job).map_err(|_| workers_gone())?;
        self.sent += 1;

        while let Ok(done) = self.results.try_recv() {
            self.collect(done)?;
        }
        while self.sent - self.written >= self.max_in_flight {
            self.wait()?;
        }
        Ok(())
    }

    fn wait(&mut self) -> io::Result<()> {
        let done = self.results.recv().map_err(|_| workers_gone())?;
        self.collect(done)
    }

    /// Writes every segment that is next in order.
    fn collect(&mut self, done: Done<'a>) -> io::Result<()> {
        self.waiting.insert(done.index, done);
        while let Some(done) = self.waiting.remove(&self.written) {
            done.result?;
            self.stats.add(&done.stats);
            self.writer.write_all(&done.out)?;
            self.written += 1;
            let mut out = done.out;
            out.clear();
            self.free.push((done.segment, out));
        }
        Ok(())
    }
}

impl<'a, W: Write> Sink<'a> for ParallelSink<'a, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.segment.bytes.extend_from_slice(bytes);
        self.tick(0, bytes.len())
    }

    fn mask(
        &mut self,
        row: Vec<u8>,
        format: InsertFormat,
        plan: &Arc<MaskingPlan<'a>>,
    ) -> io::Result<()> {
        // Copied, so the row is freed by the thread that allocated it.
        self.segment.push_row(&row, format, plan);
        self.tick(1, row.len())
    }

    fn finish(mut self) -> io::Result<MaskStats> {
        if !self.segment.is_empty() {
            self.dispatch()?;
        }
        // Closing the queue stops the workers once it is empty.
        self.jobs = None;
        while self.written < self.sent {
            self.wait()?;
        }
        self.writer.flush()?;
        Ok(self.stats)
    }
}

/// A worker: masks segments from the queue until it is closed.
fn work<'a>(queue: &Mutex<Receiver<Job<'a>>>, done: &Sender<Done<'a>>, context: RowContext) {
    loop {
        // The lock is held only while waiting for a job, not while masking.
        let job = queue.lock().unwrap_or_else(PoisonError::into_inner).recv();
        let Ok(Job {
            index,
            seed,
            mut segment,
            mut out,
        }) = job
        else {
            return;
        };
        let mut stats = MaskStats::default();
        // A panic becomes an error for this segment, so the writer is never
        // left waiting for a segment that will not come.
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            segment.mask(seed, context, &mut stats, &mut out)
        }))
        .unwrap_or_else(|_| Err(io::Error::other("a masking thread panicked")));
        segment.clear();
        let finished = Done {
            index,
            result,
            stats,
            segment,
            out,
        };
        if done.send(finished).is_err() {
            return;
        }
    }
}

fn workers_gone() -> io::Error {
    io::Error::other("the masking threads stopped")
}

/// What masking a row needs besides the row, its plan and a generator.
#[derive(Debug, Clone, Copy)]
pub struct RowContext<'s> {
    pub dialect: SqlDialect,
    pub secret: &'s str,
}

impl RowContext<'_> {
    /// Appends `row` to `out` with the fields of `plan` masked.
    fn mask_row(
        &self,
        row: &[u8],
        format: InsertFormat,
        plan: &MaskingPlan,
        rng: &mut StdRng,
        stats: &mut MaskStats,
        out: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut masker = RowMasker {
            columns: plan,
            rng,
            secret: self.secret,
            stats,
        };
        codec::rewrite_row(row, format, self.dialect, &mut masker, out)
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
