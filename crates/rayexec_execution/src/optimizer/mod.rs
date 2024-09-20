pub mod expr_rewrite;
pub mod filter_pushdown;
pub mod limit_pushdown;
pub mod location;

use std::time::Duration;

use crate::{
    logical::{binder::bind_context::BindContext, operator::LogicalOperator},
    runtime::time::{RuntimeInstant, Timer},
};
use filter_pushdown::FilterPushdown;
use limit_pushdown::LimitPushdown;
use rayexec_error::Result;
use tracing::debug;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OptimizerProfileData {
    pub total: Duration,
    pub timings: Vec<(&'static str, Duration)>,
}

#[derive(Debug)]
pub struct Optimizer {
    pub profile_data: OptimizerProfileData,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        Optimizer {
            profile_data: OptimizerProfileData::default(),
        }
    }

    /// Run a logical plan through the optimizer.
    pub fn optimize<I>(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator>
    where
        I: RuntimeInstant,
    {
        let total = Timer::<I>::start();

        // First filter pushdown.
        let timer = Timer::<I>::start();
        let mut rule = FilterPushdown::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("filter_pushdown_1", timer.stop()));

        // Limit pushdown.
        let timer = Timer::<I>::start();
        let mut rule = LimitPushdown;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("limit_pushdown", timer.stop()));

        // DO THE OTHER RULES

        // let rule = LocationRule {};
        // let optimized = rule.optimize(bind_context, optimized)?;

        // Filter pushdown again.
        let timer = Timer::<I>::start();
        let mut rule = FilterPushdown::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("filter_pushdown_2", timer.stop()));

        self.profile_data.total = total.stop();

        debug!(?self.profile_data, "optimizer timings");

        Ok(plan)
    }
}

pub trait OptimizeRule {
    /// Apply an optimization rule to the logical plan.
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator>;
}
