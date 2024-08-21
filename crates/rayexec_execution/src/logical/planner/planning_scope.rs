use rayexec_bullet::datatype::DataType;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanningAlias {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub name: String,
}

impl PlanningAlias {
    fn matches(&self, other: &PlanningAlias) -> bool {
        match (&self.database, &other.database) {
            (Some(a), Some(b)) if a != b => return false,
            _ => (),
        }
        match (&self.schema, &other.schema) {
            (Some(a), Some(b)) if a != b => return false,
            _ => (),
        }

        self.name == other.name
    }
}

impl fmt::Display for PlanningAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(database) = &self.database {
            write!(f, "{database}")?;
        }
        if let Some(schema) = &self.schema {
            write!(f, "{schema}")?;
        }
        write!(f, "{}", self.name)
    }
}

#[derive(Debug)]
pub struct PlanningScope {
    pub scope_idx: usize,
    pub alias: PlanningAlias,
    pub types: Vec<DataType>,
    pub column_names: Vec<String>,
}
