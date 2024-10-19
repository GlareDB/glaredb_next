use std::fmt;

pub mod assumptions {
    //! Assumptions when we don't have complete statistics available to us.

    /// Selectivity with '='.
    pub const EQUALITY_SELECTIVITY: f64 = 0.1;
    /// Selectivity with other comparison operators like '<', '>', '!=' etc.
    pub const INEQUALITY_SELECTIVITY: f64 = 0.3;
    /// Default selectivity to use if neither of the above apply.
    pub const DEFAULT_SELECTIVITY: f64 = 0.3;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsCount {
    Exact(usize),
    Estimated(usize),
    Unknown,
}

impl StatisticsCount {
    pub fn value(self) -> Option<usize> {
        match self {
            Self::Exact(v) | Self::Estimated(v) => Some(v),
            Self::Unknown => None,
        }
    }
}

impl fmt::Display for StatisticsCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(v) => write!(f, "{v}"),
            Self::Estimated(v) => write!(f, "{v} [estimated]"),
            Self::Unknown => write!(f, "[unknown]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statistics {
    /// Cardinality of the operator.
    pub cardinality: StatisticsCount,
    /// Statistics for each column emitted by an operator.
    ///
    /// May be None if no column statistics are available.
    pub column_stats: Option<Vec<ColumnStatistics>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnStatistics {
    /// Number of distinct values in the column.
    pub num_distinct: StatisticsCount,
}

impl Statistics {
    pub const fn unknown() -> Self {
        Statistics {
            cardinality: StatisticsCount::Unknown,
            column_stats: None,
        }
    }
}
