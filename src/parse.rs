use anyhow::{Result, bail, ensure};
use spargebra::Query;

pub fn parse_query(s: &str) -> Result<sparopt::algebra::GraphPattern> {
    let query = spargebra::SparqlParser::new().parse_query(s)?;
    let pattern = match query {
        Query::Select {
            dataset,
            base_iri,
            pattern,
        } => {
            ensure!(dataset.is_none(), "{dataset:?} unsupported dataset");
            ensure!(base_iri.is_none(), "{base_iri:?} unsupported base_iri");
            pattern
        }
        other => bail!("{other:?} unsupported query"),
    };
    let pattern = (&pattern).into();
    Ok(pattern)
    // Ok(sparopt::Optimizer::optimize_graph_pattern((&pattern).into()))
}
