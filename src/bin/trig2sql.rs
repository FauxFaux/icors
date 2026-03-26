use anyhow::{Context, Result, bail};
use clap::Parser as _;
use compact_str::{CompactString, ToCompactString};
use oxrdf::{GraphName, NamedOrBlankNode, Term};
use oxttl::TriGParser;
use rusqlite::{Statement, params};
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
    Literal(CompactString),
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

    let mut name_insert = db.prepare("INSERT INTO name (id, value) VALUES (?, ?)")?;
    let mut literal_insert = db.prepare("INSERT INTO literal (id, value) VALUES (?, ?)")?;
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
            Term::Literal(lit) => NameOrLiteral::Literal(lit.value().to_compact_string()),
            other => bail!("unsupported object: {other:?}"),
        };

        let mut upsert = |v: CompactString, insert: &mut Statement| -> Result<i64> {
            let len = names.len();
            match names.entry(v.clone()) {
                Entry::Occupied(o) => Ok(*o.get()),
                Entry::Vacant(v) => {
                    let len = i64::try_from(len)?;
                    insert
                        .insert(params![len, v.key().as_str().to_string()])
                        .context("insert name")?;
                    v.insert(len);
                    Ok(len)
                }
            }
        };

        let graph_name = upsert(graph_name, &mut name_insert)?;
        let subject = upsert(subject, &mut name_insert)?;
        let pred = upsert(pred, &mut name_insert)?;
        let obj = match obj {
            NameOrLiteral::Name(v) => upsert(v, &mut name_insert)?,
            NameOrLiteral::Literal(v) => upsert(v, &mut literal_insert)?,
        };

        quad_insert
            .execute(params![graph_name, subject, pred, obj])
            .context("insert quad")?;
    }

    handle
        .join()
        .expect("thread panic propagation")
        .context("reading file / submitting")?;

    drop(name_insert);
    drop(quad_insert);
    drop(literal_insert);

    db.commit()?;

    Ok(())
}
