use anyhow::Result;

fn main() -> Result<()> {
    let db = rusqlite::Connection::open("db.sqlite")?;
    let mut stmt = db.prepare("select graph, subject, predicate, object from quad")?;
    let vec = stmt
        .query_map((), |row| {
            Ok((
                row.get::<_, i32>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, i32>(2)?,
                row.get::<_, i32>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    println!("{:?}", vec.iter().rev().take(10).collect::<Vec<_>>());
    Ok(())
}
