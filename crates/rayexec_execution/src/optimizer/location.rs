use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator};
use rayexec_error::Result;

use super::OptimizeRule;

/// Rule for pushing down and pulling up location requirements for operators.
///
/// This works by pushing down location requirements as far as possible, then
/// pulling them back up.
///
/// There is no preference for the location requirement for the root of the
/// plan.
#[derive(Debug, Clone)]
pub struct LocationRule {}

impl OptimizeRule for LocationRule {
    fn optimize(&self, mut plan: LogicalOperator) -> Result<LogicalOperator> {
        plan.walk_mut(
            &mut |op| {
                match op {
                    LogicalOperator::Projection(node) => {
                        // override_any_location(node, node.as_mut().input.as_mut())
                    }
                    _ => unimplemented!(),
                }
                //
                unimplemented!()
            },
            &mut |op| {
                //
                unimplemented!()
            },
        )?;

        Ok(plan)
    }
}

/// Set the location of some operator from a different operator if the location
/// reqirement is Any.
fn override_any_location<N1, N2>(from: LogicalNode<N1>, op: &mut LogicalNode<N2>) {
    if matches!(op.location, LocationRequirement::Any) {
        op.location = from.location
    }
}
