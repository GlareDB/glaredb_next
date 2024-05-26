use crate::planner::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::{ExplainFormat, LogicalOperator},
};
use rayexec_error::{Result, ResultExt};

pub fn format_logical_plan_for_explain(
    format: ExplainFormat,
    verbose: bool,
    plan: &LogicalOperator,
) -> Result<String> {
    let conf = ExplainConfig { verbose };
    match format {
        ExplainFormat::Text => ExplainNode::walk_logical(plan, conf).format_text(0, String::new()),
        ExplainFormat::Json => unimplemented!(),
    }
}

#[derive(Debug)]
struct ExplainNode {
    entry: ExplainEntry,
    children: Vec<ExplainNode>,
}

impl ExplainNode {
    fn walk_logical(plan: &LogicalOperator, conf: ExplainConfig) -> ExplainNode {
        let children = match plan {
            LogicalOperator::Projection(p) => vec![Self::walk_logical(&p.input, conf)],
            LogicalOperator::Filter(p) => vec![Self::walk_logical(&p.input, conf)],
            LogicalOperator::Aggregate(p) => vec![Self::walk_logical(&p.input, conf)],
            LogicalOperator::Order(p) => vec![Self::walk_logical(&p.input, conf)],
            LogicalOperator::AnyJoin(p) => {
                vec![
                    Self::walk_logical(&p.left, conf),
                    Self::walk_logical(&p.right, conf),
                ]
            }
            LogicalOperator::EqualityJoin(p) => {
                vec![
                    Self::walk_logical(&p.left, conf),
                    Self::walk_logical(&p.right, conf),
                ]
            }
            LogicalOperator::CrossJoin(p) => {
                vec![
                    Self::walk_logical(&p.left, conf),
                    Self::walk_logical(&p.right, conf),
                ]
            }
            LogicalOperator::Limit(p) => vec![Self::walk_logical(&p.input, conf)],
            LogicalOperator::CreateTableAs(p) => vec![Self::walk_logical(&p.input, conf)],
            LogicalOperator::Explain(p) => vec![Self::walk_logical(&p.input, conf)],
            LogicalOperator::Scan(p) => vec![ExplainNode {
                entry: p.source.explain_entry(conf),
                children: Vec::new(),
            }],
            LogicalOperator::Empty
            | LogicalOperator::ExpressionList(_)
            | LogicalOperator::SetVar(_)
            | LogicalOperator::ShowVar(_) => Vec::new(),
        };

        ExplainNode {
            entry: plan.explain_entry(conf),
            children,
        }
    }

    fn format_text(&self, indent: usize, mut buf: String) -> Result<String> {
        use std::fmt::Write as _;
        writeln!(buf, "{}{}", " ".repeat(indent), self.entry)
            .context("failed to write to explain buffer")?;

        for child in &self.children {
            buf = child.format_text(indent + 2, buf)?;
        }

        Ok(buf)
    }
}
