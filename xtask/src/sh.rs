//! Shared plumbing for the `cargo xtask` commands.
//!
//! Four things every task needs and a shell gave away for free: a repository
//! root to resolve paths from, `command -v` to name a missing tool before a
//! compiler dies of it, child processes whose failure carries a documented exit
//! code, and file operations that say which path failed. There is no
//! `set -euo pipefail` equivalent because there is nothing to emulate: every
//! fallible call returns [`Result`] and the commands `?` on all of them.

use std::env;
use std::ffi::OsStr;
#[cfg(windows)]
use std::ffi::OsString;
use std::fmt;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// A failed task: the exit code the process ends with, and the already-tagged
/// message `main` writes to stderr verbatim.
///
/// Each command documents its own codes in its module header. CI reads them to
/// name a missing prerequisite instead of parsing output.
pub struct Error {
    /// Process exit code.
    pub code: u8,
    /// Tagged, possibly multi-line text.
    pub message: String,
}

/// Task result. `Ok` means the step did what it printed.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The repository root.
///
/// `CARGO_MANIFEST_DIR` is `<root>/xtask`, baked in when this binary was
/// compiled, which anchors a task the same way `dirname "${BASH_SOURCE[0]}"/..`
/// did: it runs against the checkout it was built from, whatever the caller's
/// working directory.
pub fn repo_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap_or(manifest).to_path_buf()
}

/// `command -v`, portably.
///
/// Returns the resolved path *including* its extension. On Windows a tool is
/// often a `.cmd` shim, as `npm`, `npx`, `gradle` and `componentize-py` all
/// are, and `CreateProcess` cannot execute one. The standard library
/// routes a program through `cmd.exe`, with batch-safe argument quoting, only
/// when the name it is given ends in `.bat` or `.cmd`, so a bare
/// `Command::new("npm")` fails outright there.
pub fn which(name: &str) -> Option<PathBuf> {
    if name.contains('/') || name.contains('\\') {
        return probe(Path::new(name));
    }
    let path = env::var_os("PATH")?;
    env::split_paths(&path).find_map(|dir| probe(&dir.join(name)))
}

/// Windows tries each `PATHEXT` extension the way `CreateProcess` does. Unix
/// wants the executable bit, so a same-named directory or data file is not a
/// hit.
#[cfg(windows)]
fn probe(candidate: &Path) -> Option<PathBuf> {
    if candidate.extension().is_some() && candidate.is_file() {
        return Some(candidate.to_path_buf());
    }
    let exts = env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_owned());
    exts.split(';')
        .filter(|ext| !ext.is_empty())
        .map(|ext| {
            let mut name = OsString::from(candidate);
            name.push(ext);
            PathBuf::from(name)
        })
        .find(|path| path.is_file())
}

#[cfg(not(windows))]
fn probe(candidate: &Path) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let meta = candidate.metadata().ok()?;
    (meta.is_file() && meta.permissions().mode() & 0o111 != 0).then(|| candidate.to_path_buf())
}

/// Per-command context: the `[tag]` every line carries, the repository root
/// paths resolve from, and the PINS file a missing tool sends the reader to.
pub struct Ctx {
    tag: &'static str,
    pins: Option<&'static str>,
    root: PathBuf,
}

impl Ctx {
    /// A context whose failures name no PINS file.
    pub fn new(tag: &'static str) -> Self {
        Self {
            tag,
            pins: None,
            root: repo_root(),
        }
    }

    /// A context whose missing-tool failures point at `pins`, the file that
    /// carries the pinned version and the per-tool caveats.
    pub fn with_pins(tag: &'static str, pins: &'static str) -> Self {
        Self {
            tag,
            pins: Some(pins),
            root: repo_root(),
        }
    }

    /// The repository root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// A path below the repository root, from a `/`-separated relative form.
    pub fn path(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    /// One tagged progress line on stdout.
    pub fn log(&self, msg: impl fmt::Display) {
        println!("[{}] {msg}", self.tag);
    }

    /// A tagged failure: the first line is the error, the rest are context.
    pub fn error(&self, code: u8, lines: &[&str]) -> Error {
        let mut message = String::new();
        let mut lines = lines.iter();
        if let Some(first) = lines.next() {
            let _ = write!(message, "[{}] ERROR: {first}", self.tag);
        }
        for line in lines {
            let _ = write!(message, "\n[{}] {line}", self.tag);
        }
        Error { code, message }
    }

    /// [`Ctx::error`] as a `Result`, for `return ctx.fail(..)` call sites.
    pub fn fail<T>(&self, code: u8, lines: &[&str]) -> Result<T> {
        Err(self.error(code, lines))
    }

    /// Refuse to start when a tool is missing, naming it and how to install it.
    ///
    /// Checked up front rather than at the call site, so a failure names the
    /// prerequisite instead of surfacing as whatever a compiler prints when its
    /// linker is absent.
    pub fn require(&self, tool: &str, code: u8, hint: &str) -> Result<PathBuf> {
        if let Some(path) = which(tool) {
            return Ok(path);
        }
        let missing = format!("{tool} not found on PATH");
        let install = format!("install with: {hint}");
        let pins = self
            .pins
            .map(|pins| format!("see {pins} for the pinned versions"));
        let mut lines = vec![missing.as_str(), install.as_str()];
        if let Some(pins) = &pins {
            lines.push(pins);
        }
        self.fail(code, &lines)
    }

    /// A child process rooted at the repository root, its program resolved
    /// through [`which`] first.
    pub fn cmd(&self, program: &str) -> Result<Cmd> {
        let resolved = which(program)
            .ok_or_else(|| self.error(1, &[&format!("{program} not found on PATH")]))?;
        Ok(Cmd::new(self.tag, program, &resolved, &self.root))
    }

    /// The cargo that invoked this task, so a nested build uses the toolchain
    /// `cargo xtask` itself was resolved to rather than whatever PATH offers.
    pub fn cargo(&self) -> Result<Cmd> {
        match env::var_os("CARGO") {
            Some(exe) => Ok(Cmd::new(self.tag, "cargo", Path::new(&exe), &self.root)),
            None => self.cmd("cargo"),
        }
    }

    /// Copy a file, creating the destination's parent directory.
    pub fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        if let Some(parent) = to.parent() {
            self.ensure_dir(parent)?;
        }
        fs::copy(from, to).map(|_| ()).map_err(|e| {
            self.error(
                1,
                &[&format!(
                    "copying {} to {}: {e}",
                    from.display(),
                    to.display()
                )],
            )
        })
    }

    /// `mkdir -p`.
    pub fn ensure_dir(&self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir)
            .map_err(|e| self.error(1, &[&format!("creating {}: {e}", dir.display())]))
    }

    /// Remove a directory tree, tolerating its absence.
    pub fn remove_dir(&self, dir: &Path) -> Result<()> {
        match fs::remove_dir_all(dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => self.fail(1, &[&format!("removing {}: {e}", dir.display())]),
        }
    }

    /// Read a file to a `String`.
    pub fn read(&self, path: &Path) -> Result<String> {
        fs::read_to_string(path)
            .map_err(|e| self.error(1, &[&format!("reading {}: {e}", path.display())]))
    }

    /// Write a file, creating its parent directory.
    pub fn write(&self, path: &Path, contents: &str) -> Result<()> {
        if let Some(parent) = path.parent() {
            self.ensure_dir(parent)?;
        }
        fs::write(path, contents)
            .map_err(|e| self.error(1, &[&format!("writing {}: {e}", path.display())]))
    }

    /// A file's size in bytes, for the `built <name> (<n> bytes)` lines.
    pub fn size(&self, path: &Path) -> Result<u64> {
        fs::metadata(path)
            .map(|meta| meta.len())
            .map_err(|e| self.error(1, &[&format!("reading {}: {e}", path.display())]))
    }

    /// The argument gate for a command that takes no options: `Ok(true)` means
    /// usage was printed and there is nothing left to do.
    ///
    /// Only the first argument is read, because every outcome ends the command.
    pub fn no_options(&self, args: &[String], usage: &str) -> Result<bool> {
        match args.first() {
            None => Ok(false),
            Some(arg) if arg == "-h" || arg == "--help" => {
                println!("{usage}");
                Ok(true)
            }
            Some(other) => self.fail(1, &[&format!("unknown argument '{other}'"), usage]),
        }
    }

    /// Refuse to continue when a build produced no artifact.
    pub fn expect_artifact(&self, path: &Path, code: u8) -> Result<()> {
        if path.is_file() {
            return Ok(());
        }
        self.fail(
            code,
            &[&format!(
                "expected {} to exist after the build",
                path.display()
            )],
        )
    }

    /// Install hint for curl, which every download shares.
    pub const CURL_HINT: &'static str = "ships with macOS, Windows and most Linux distributions";

    /// Fetch a release asset with curl.
    ///
    /// curl ships with Windows 10 and later, macOS and most Linux
    /// distributions, and shelling out to it keeps this crate dependency-free:
    /// a TLS stack and an async runtime for two release downloads is not a
    /// trade worth making.
    pub fn download(&self, url: &str, dest: &Path, curl_missing: u8, failed: u8) -> Result<()> {
        self.require("curl", curl_missing, Self::CURL_HINT)?;
        let ok = self
            .cmd("curl")?
            .args([
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                "--output",
            ])
            .arg(dest)
            .arg(url)
            .status()?;
        if !ok {
            return self.fail(failed, &[&format!("download failed: {url}")]);
        }
        Ok(())
    }

    /// Mark a downloaded binary executable. A no-op on Windows, where an
    /// `.exe` needs no permission bit.
    pub fn make_executable(&self, path: &Path) -> Result<()> {
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut perms = fs::metadata(path)
                .map_err(|e| self.error(1, &[&format!("reading {}: {e}", path.display())]))?
                .permissions();
            perms.set_mode(perms.mode() | 0o755);
            fs::set_permissions(path, perms)
                .map_err(|e| self.error(1, &[&format!("chmod +x {}: {e}", path.display())]))
        }
        #[cfg(windows)]
        {
            let _ = path;
            Ok(())
        }
    }

    /// Run an executable at a concrete `path` (not resolved through PATH),
    /// inheriting stdio. Fails with the child's own exit code, like
    /// [`Cmd::run`], but for a binary `which` cannot find: the `saci-service`
    /// build product the example `validate`/`demo` tasks run.
    pub fn run_exe(&self, path: &Path, args: &[&str]) -> Result<()> {
        let status = Command::new(path)
            .current_dir(&self.root)
            .args(args)
            .status()
            .map_err(|e| self.error(1, &[&format!("could not run {}: {e}", path.display())]))?;
        if status.success() {
            return Ok(());
        }
        let code = status
            .code()
            .and_then(|code| u8::try_from(code).ok())
            .filter(|code| *code != 0)
            .unwrap_or(1);
        Err(self.error(
            code,
            &[&format!("`{}` failed (exit {code})", path.display())],
        ))
    }
}

/// A child process. Failure is the caller's to interpret: [`Cmd::status`]
/// reports it, [`Cmd::run`] turns it into the child's own exit code.
pub struct Cmd {
    tag: &'static str,
    desc: String,
    inner: Command,
}

impl Cmd {
    fn new(tag: &'static str, name: &str, program: &Path, dir: &Path) -> Self {
        let mut inner = Command::new(program);
        inner.current_dir(dir);
        Self {
            tag,
            desc: name.to_owned(),
            inner,
        }
    }

    /// One argument.
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        let arg = arg.as_ref();
        self.desc.push(' ');
        self.desc.push_str(&arg.to_string_lossy());
        self.inner.arg(arg);
        self
    }

    /// Several arguments, in order.
    pub fn args<I, A>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: AsRef<OsStr>,
    {
        for arg in args {
            self = self.arg(arg);
        }
        self
    }

    /// One environment variable for the child only.
    pub fn env(mut self, key: &str, value: impl AsRef<OsStr>) -> Self {
        self.inner.env(key, value);
        self
    }

    /// Run in `dir` instead of the repository root, the `( cd x && ... )`
    /// subshell every toolchain that reads its own manifest needs.
    pub fn dir(mut self, dir: &Path) -> Self {
        self.inner.current_dir(dir);
        self
    }

    /// Run to completion, inheriting stdio. `Ok(false)` is a non-zero exit.
    pub fn status(mut self) -> Result<bool> {
        match self.inner.status() {
            Ok(status) => Ok(status.success()),
            Err(e) => Err(self.spawn_error(&e)),
        }
    }

    /// Run to completion, failing with the child's own exit code.
    pub fn run(mut self) -> Result<()> {
        let status = match self.inner.status() {
            Ok(status) => status,
            Err(e) => return Err(self.spawn_error(&e)),
        };
        if status.success() {
            return Ok(());
        }
        // A signal-terminated child reports no code; 1 is the only honest
        // answer left, and the message says which process it was.
        let code = status
            .code()
            .and_then(|code| u8::try_from(code).ok())
            .filter(|code| *code != 0)
            .unwrap_or(1);
        Err(self.exit_error(code, status.code()))
    }

    /// Run to completion capturing stdout, stderr inherited. `false` is a
    /// non-zero exit, which the caller is expected to interpret.
    pub fn output_status(mut self) -> Result<(bool, String)> {
        self.inner.stderr(Stdio::inherit());
        match self.inner.output() {
            Ok(out) => Ok((
                out.status.success(),
                String::from_utf8_lossy(&out.stdout).into_owned(),
            )),
            Err(e) => Err(self.spawn_error(&e)),
        }
    }

    /// Run to completion capturing stdout and stderr together, for a build log
    /// that has to be both printed and read.
    pub fn output_merged(mut self) -> Result<(bool, String)> {
        match self.inner.output() {
            Ok(out) => {
                let mut merged = String::from_utf8_lossy(&out.stdout).into_owned();
                merged.push_str(&String::from_utf8_lossy(&out.stderr));
                Ok((out.status.success(), merged))
            }
            Err(e) => Err(self.spawn_error(&e)),
        }
    }

    fn spawn_error(&self, e: &std::io::Error) -> Error {
        Error {
            code: 1,
            message: format!("[{}] ERROR: could not run `{}`: {e}", self.tag, self.desc),
        }
    }

    fn exit_error(&self, code: u8, reported: Option<i32>) -> Error {
        let how = match reported {
            Some(reported) => format!("exit {reported}"),
            None => "terminated by signal".to_owned(),
        };
        Error {
            code,
            message: format!("[{}] ERROR: `{}` failed ({how})", self.tag, self.desc),
        }
    }
}
