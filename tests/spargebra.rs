use anyhow::Result;

#[test]
fn test_resubmitted() -> Result<()> {
    let gp = icors::parse::parse_query(include_str!("resubmitted.sparql"))?;
    println!("{gp:#?}");
    Ok(())
}
#[test]
fn test_dcat_metadata() -> Result<()> {
    let gp = icors::parse::parse_query(include_str!("dcat-metadata.sparql"))?;
    println!("{gp:#?}");
    Ok(())
}
