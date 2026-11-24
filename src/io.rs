//! Where a run reads the dump and writes its output.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::MultiGzDecoder;
use log::warn;

/// Where the dump is read from.
pub enum InputSource {
    /// A file read as it is.
    File(File),
    /// A file whose name ends in `.gz`, decompressed while it is read:
    /// every gzip member, as `gunzip` reads them, not only the first.
    Gzip(MultiGzDecoder<File>),
    /// The standard input.
    Stdin(io::Stdin),
}

impl InputSource {
    /// Opens `path`, decompressing it when its name ends in `.gz` (in any
    /// case); stdin when `None`.
    pub fn new(path: Option<PathBuf>) -> io::Result<Self> {
        match path {
            Some(p) => {
                let file = File::open(&p)?;
                let path_str = p.to_string_lossy().to_lowercase();
                if path_str.ends_with(".gz") {
                    return Ok(InputSource::Gzip(MultiGzDecoder::new(file)));
                }
                Ok(InputSource::File(file))
            }
            None => Ok(InputSource::Stdin(io::stdin())),
        }
    }

    /// Wraps the source in a [`BufReader`].
    pub fn into_buffered(self) -> BufReader<Self> {
        BufReader::new(self)
    }
}

impl Read for InputSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            InputSource::File(f) => f.read(buf),
            InputSource::Gzip(gz) => gz.read(buf).map_err(|e| {
                io::Error::new(e.kind(), format!("could not decompress the input: {e}."))
            }),
            InputSource::Stdin(s) => s.read(buf),
        }
    }
}

/// Where the masked dump is written.
pub enum OutputSource {
    /// A file: a temporary one when the target is a regular file (see
    /// [`PendingOutput`]), the target itself otherwise.
    File(File),
    /// The standard output.
    Stdout(io::Stdout),
}

impl OutputSource {
    /// Opens the output: stdout without a path. A path to a regular file,
    /// or to none yet, is written through a [`PendingOutput`], which leaves
    /// the file untouched until [`PendingOutput::commit`]; any other path
    /// (a device, a pipe, a symbolic link to nothing) is written directly.
    pub fn open(path: Option<&Path>) -> io::Result<(Self, Option<PendingOutput>)> {
        let Some(path) = path else {
            return Ok((OutputSource::Stdout(io::stdout()), None));
        };
        let (target, permissions) = match fs::metadata(path) {
            Ok(metadata) if metadata.is_file() => {
                (fs::canonicalize(path)?, Some(metadata.permissions()))
            }
            Ok(_) => return Ok((OutputSource::File(File::create(path)?), None)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if fs::symlink_metadata(path).is_ok() {
                    return Ok((OutputSource::File(File::create(path)?), None));
                }
                (path.to_path_buf(), None)
            }
            Err(e) => return Err(e),
        };
        let (file, temp) = create_temporary(&target, permissions.as_ref())?;
        let pending = PendingOutput {
            file: file.try_clone()?,
            temp,
            target,
            committed: false,
        };
        // A file that is replaced keeps its permissions, also the ones the
        // umask removed when the temporary file was created.
        if let Some(permissions) = permissions {
            fs::set_permissions(&pending.temp, permissions)?;
        }
        Ok((OutputSource::File(file), Some(pending)))
    }

    /// Wraps the output in a [`BufWriter`].
    pub fn into_buffered(self) -> BufWriter<Self> {
        BufWriter::new(self)
    }
}

impl Write for OutputSource {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            OutputSource::File(f) => f.write(buf),
            OutputSource::Stdout(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            OutputSource::File(f) => f.flush(),
            OutputSource::Stdout(s) => s.flush(),
        }
    }
}

/// An output file written in place of its target: to a temporary file in
/// the same directory, renamed over the target by [`commit`](Self::commit)
/// once the run succeeds. Dropped without it, the temporary file is
/// removed, so a run that fails leaves the target as it was, or absent.
pub struct PendingOutput {
    /// Another handle to the temporary file, to flush it to disk.
    file: File,
    temp: PathBuf,
    target: PathBuf,
    committed: bool,
}

impl PendingOutput {
    /// The file that `commit` replaces or creates.
    pub fn target(&self) -> &Path {
        &self.target
    }

    /// Flushes the temporary file to disk and renames it over the target.
    pub fn commit(mut self) -> io::Result<()> {
        self.file.sync_all()?;
        fs::rename(&self.temp, &self.target)?;
        self.committed = true;
        if let Err(e) = sync_directory(&self.target) {
            warn!("the output was written, but its directory could not be flushed to disk: {e}");
        }
        Ok(())
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.temp);
        }
    }
}

/// Creates `.<name>.<random>.tmp` next to `target`, so that renaming it
/// over the target stays on one file system. When the target exists, its
/// `permissions` are the most the file is created with: a reader who opens
/// it while it is more open keeps reading what is written to it.
fn create_temporary(
    target: &Path,
    permissions: Option<&fs::Permissions>,
) -> io::Result<(File, PathBuf)> {
    let name = target.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "the output path names no file")
    })?;
    let directory = match target.parent() {
        Some(directory) if !directory.as_os_str().is_empty() => directory,
        _ => Path::new("."),
    };
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if let Some(permissions) = permissions {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        options.mode(permissions.mode() & 0o777);
    }
    #[cfg(not(unix))]
    let _ = permissions;
    loop {
        let mut temp = OsString::from(".");
        temp.push(name);
        temp.push(format!(".{:016x}.tmp", rand::random::<u64>()));
        let temp = directory.join(temp);
        match options.open(&temp) {
            Ok(file) => return Ok((file, temp)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!(
                        "could not create a temporary file in {}, where the output is \
                         written before it replaces {}: {e}",
                        directory.display(),
                        target.display()
                    ),
                ));
            }
        }
    }
}

/// Flushes the directory entry of a renamed file, so the rename survives
/// a crash.
#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(directory) if !directory.as_os_str().is_empty() => File::open(directory)?.sync_all(),
        _ => File::open(".")?.sync_all(),
    }
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn the_temporary_file_is_never_more_open_than_its_target() {
        let directory = std::env::temp_dir().join(format!("ghostdump-io-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let target = directory.join("out.sql");
        let created = create_temporary(&target, Some(&fs::Permissions::from_mode(0o600)))
            .map(|(_, temp)| fs::metadata(temp).unwrap().permissions().mode());
        fs::remove_dir_all(&directory).unwrap();
        let mode = created.unwrap();
        assert_eq!(mode & 0o077, 0, "created with mode {mode:o}");
    }
}
