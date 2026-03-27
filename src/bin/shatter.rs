use anyhow::Result;
use clap::Parser as _;
use itertools::Itertools as _;
use memchr::memmem;
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// input file
    path: PathBuf,

    chunks: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let file = fs::File::open(&cli.path)?;
    // safe if file is unmodified during runtime, assumption
    let file = unsafe { memmap2::Mmap::map(&file)? };
    let len = file.len();
    let step = len / cli.chunks;
    let prelude_end = memmem::find(&file, b" .\n\n<")
        .ok_or_else(|| anyhow::anyhow!("unable to find first graph"))?
        + 3;

    let prelude = &file[..prelude_end];

    let starts = (1..cli.chunks)
        .map(|c| c * step)
        .map(|start| {
            memmem::find(&file[start..], b"\n}\n\n<")
                .map(|pos| start + pos + 3)
                .unwrap_or(len)
        })
        .chain(std::iter::once(len))
        .collect::<HashSet<_>>();

    for (i, (a, b)) in std::iter::once(prelude_end)
        .chain(starts.into_iter())
        .sorted()
        .tuple_windows()
        .enumerate()
    {
        let seg = &file[a..b];

        let path = cli.path.with_added_extension(format!("part{i:03}"));
        let mut new_file = fs::File::create(&path)?;
        new_file.set_len(u64::try_from(prelude.len() + seg.len())?)?;
        new_file.write_all(prelude)?;
        new_file.write_all(seg)?;
    }
    Ok(())
}
