use anyhow::{Context, Result, bail};
use clap::Parser as _;
use compact_str::{CompactString, ToCompactString};
use icors::{MAGIC_TYPE_NAME, MAGIC_TYPE_TYPE};
use oxrdf::{GraphName, NamedOrBlankNode, Term};
use oxttl::TriGParser;
use rusqlite::params;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::{fs, io};

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// input file
    path: PathBuf,
}

enum NameOrLiteral {
    Name(CompactString),
    Literal(CompactString, CompactString),
}

fn main() -> Result<()> {
    let (to_db, from_file) = std::sync::mpsc::sync_channel(4096);

    let args = Cli::parse();

    let handle = std::thread::spawn(move || -> Result<()> {
        let file = fs::File::open(&args.path)?;
        let file = zstd::stream::Decoder::new(file)?;
        let file = io::BufReader::new(file);

        let parser = TriGParser::new();
        for quad in parser.for_reader(file) {
            let quad = quad?;
            to_db.send(quad)?;
        }
        Ok(())
    });

    let mut db = rusqlite::Connection::open("db.sqlite")?;
    let db = db.transaction()?;
    db.execute_batch(include_str!("schema.sql"))?;

    let mut value_insert = db.prepare("INSERT INTO value (id, type, value) VALUES (?, ?, ?)")?;
    let mut quad_insert =
        db.prepare("INSERT INTO quad (graph, subject, predicate, object) VALUES (?, ?, ?, ?)")?;

    let mut names: HashMap<CompactString, i64> = HashMap::with_capacity(4096);

    while let Some(quad) = from_file.recv().ok() {
        let graph_name = match quad.graph_name {
            GraphName::NamedNode(v) => v.into_string().to_compact_string(),
            other => bail!("unsupported name: {other:?}"),
        };

        let subject = match quad.subject {
            NamedOrBlankNode::NamedNode(v) => v.into_string().to_compact_string(),
            other => bail!("unsupported subject: {other:?}"),
        };

        let pred = quad.predicate.into_string().to_compact_string();

        let obj = match quad.object {
            Term::NamedNode(v) => NameOrLiteral::Name(v.into_string().to_compact_string()),
            Term::Literal(lit) => NameOrLiteral::Literal(
                lit.value().to_compact_string(),
                lit.datatype().to_compact_string(),
            ),
            other => bail!("unsupported object: {other:?}"),
        };

        let mut upsert = |v: CompactString, type_: i64| -> Result<i64> {
            let len = names.len();
            match names.entry(v.clone()) {
                Entry::Occupied(o) => Ok(*o.get()),
                Entry::Vacant(v) => {
                    let len = i64::try_from(len)?;
                    value_insert
                        .insert(params![len, type_, v.key().as_str().to_string()])
                        .context("insert name")?;
                    v.insert(len);
                    Ok(len)
                }
            }
        };

        let graph_name = upsert(graph_name, MAGIC_TYPE_NAME)?;
        let subject = upsert(subject, MAGIC_TYPE_NAME)?;
        let pred = upsert(pred, MAGIC_TYPE_NAME)?;
        let obj = match obj {
            NameOrLiteral::Name(v) => upsert(v, MAGIC_TYPE_NAME)?,
            NameOrLiteral::Literal(v, type_) => {
                let type_ = upsert(type_, MAGIC_TYPE_TYPE)?;
                upsert(v, type_)?
            }
        };

        quad_insert
            .execute(params![graph_name, subject, pred, obj])
            .context("insert quad")?;
    }

    handle
        .join()
        .expect("thread panic propagation")
        .context("reading file / submitting")?;

    drop(value_insert);
    drop(quad_insert);

    db.commit()?;

    Ok(())
}
