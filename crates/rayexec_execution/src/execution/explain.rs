use crate::{
    execution::executable::pipeline::ExecutablePipeline,
    logical::{
        context::QueryContext,
        explainable::{ExplainConfig, ExplainEntry, Explainable},
        operator::{ExplainFormat, LogicalOperator},
    },
};
use rayexec_error::{Result, ResultExt};
use serde::{Deserialize, Serialize};

use super::intermediate::IntermediatePipeline;

/// Formats a logical plan into explain output.
pub fn format_logical_plan_for_explain(
    context: Option<&QueryContext>,
    plan: &LogicalOperator,
    format: ExplainFormat,
    verbose: bool,
) -> Result<String> {
    let conf = ExplainConfig { verbose };
    let node = ExplainNode::walk_logical(context, plan, conf);
    match format {
        ExplainFormat::Text => node.format_text(0, String::new()),
        ExplainFormat::Json => node.format_json(),
    }
}

pub fn format_intermediate_pipelines_for_explain<'a>(
    _pipelines: impl Iterator<Item = &'a IntermediatePipeline>,
    _format: ExplainFormat,
    _verbose: bool,
) -> Result<String> {
    unimplemented!()
}

/// Formats pipelines into explain output.
pub fn format_pipelines_for_explain<'a>(
    pipelines: impl Iterator<Item = &'a ExecutablePipeline>,
    format: ExplainFormat,
    verbose: bool,
) -> Result<String> {
    let conf = ExplainConfig { verbose };

    let mut nodes: Vec<_> = pipelines
        .map(|p| ExplainNode::walk_executable_pipeline(p, conf))
        .collect();
    // Flip so that the "output" pipeline is at the top of the explain.
    nodes.reverse();

    match format {
        ExplainFormat::Text => {
            let mut buf = String::new();
            for node in nodes {
                buf = node.format_text(0, buf)?;
            }
            Ok(buf)
        }
        ExplainFormat::Json => {
            unimplemented!()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ExplainNode {
    entry: ExplainEntry,
    children: Vec<ExplainNode>,
}

impl ExplainNode {
    #[allow(dead_code)]
    fn walk_intermediate_pipeline(
        _intermediate: &IntermediatePipeline,
        _conf: ExplainConfig,
    ) -> ExplainNode {
        unimplemented!()
    }

    fn walk_executable_pipeline(pipeline: &ExecutablePipeline, conf: ExplainConfig) -> ExplainNode {
        let mut children: Vec<_> = pipeline
            .iter_operators()
            .map(|op| ExplainNode {
                entry: op.explain_entry(conf),
                children: Vec::new(),
            })
            .collect();

        // Flip the order so that the "sink" operator is on top for consistency
        // with the logical explain output.
        children.reverse();

        ExplainNode {
            entry: pipeline.explain_entry(conf),
            children,
        }
    }

    // TODO: Include location requirement in explain entry.
    fn walk_logical(
        context: Option<&QueryContext>,
        plan: &LogicalOperator,
        conf: ExplainConfig,
    ) -> ExplainNode {
        let children = match plan {
            LogicalOperator::Projection(p) => {
                vec![Self::walk_logical(context, &p.as_ref().input, conf)]
            }
            LogicalOperator::Filter2(p) => {
                vec![Self::walk_logical(context, &p.as_ref().input, conf)]
            }
            LogicalOperator::Aggregate2(p) => {
                vec![Self::walk_logical(context, &p.as_ref().input, conf)]
            }
            LogicalOperator::Order2(p) => {
                vec![Self::walk_logical(context, &p.as_ref().input, conf)]
            }
            LogicalOperator::AnyJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.as_ref().left, conf),
                    Self::walk_logical(context, &p.as_ref().right, conf),
                ]
            }
            LogicalOperator::EqualityJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.as_ref().left, conf),
                    Self::walk_logical(context, &p.as_ref().right, conf),
                ]
            }
            LogicalOperator::CrossJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.as_ref().left, conf),
                    Self::walk_logical(context, &p.as_ref().right, conf),
                ]
            }
            LogicalOperator::DependentJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.as_ref().left, conf),
                    Self::walk_logical(context, &p.as_ref().right, conf),
                ]
            }
            LogicalOperator::SetOperation(p) => {
                vec![
                    Self::walk_logical(context, &p.as_ref().top, conf),
                    Self::walk_logical(context, &p.as_ref().bottom, conf),
                ]
            }

            LogicalOperator::Limit2(p) => {
                vec![Self::walk_logical(context, &p.as_ref().input, conf)]
            }
            LogicalOperator::Insert2(p) => {
                vec![Self::walk_logical(context, &p.as_ref().input, conf)]
            }
            LogicalOperator::CopyTo2(p) => {
                vec![Self::walk_logical(context, &p.as_ref().source, conf)]
            }
            LogicalOperator::Explain2(p) => {
                vec![Self::walk_logical(context, &p.as_ref().input, conf)]
            }
            LogicalOperator::MaterializedScan(scan) => {
                if let Some(inner) = context {
                    let plan = &inner.materialized[scan.as_ref().idx].root;
                    vec![Self::walk_logical(context, plan, conf)]
                } else {
                    Vec::new()
                }
            }
            LogicalOperator::Empty2(_)
            | LogicalOperator::ExpressionList(_)
            | LogicalOperator::SetVar2(_)
            | LogicalOperator::ShowVar2(_)
            | LogicalOperator::ResetVar2(_)
            | LogicalOperator::Scan2(_)
            | LogicalOperator::TableFunction(_)
            | LogicalOperator::Drop2(_)
            | LogicalOperator::Describe2(_)
            | LogicalOperator::AttachDatabase2(_)
            | LogicalOperator::DetachDatabase2(_)
            | LogicalOperator::CreateSchema2(_)
            | LogicalOperator::CreateTable2(_) => Vec::new(),
            _ => unimplemented!(),
        };

        ExplainNode {
            entry: plan.explain_entry(conf),
            children,
        }
    }

    fn format_json(&self) -> Result<String> {
        let s = serde_json::to_string(self).context("failed to serialize to json")?;
        Ok(s)
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
