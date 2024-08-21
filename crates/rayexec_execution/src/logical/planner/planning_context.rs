use super::planning_scope::PlanningScope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeIdx(pub usize);

#[derive(Debug)]
pub struct PlanningContext {
    pub scopes: Vec<PlanningScope>,
}

impl PlanningContext {
    pub fn get_scope(&self, idx: ScopeIdx) -> Option<&PlanningScope> {
        self.scopes.get(idx.0)
    }

    pub fn put_scope(&mut self, scope: PlanningScope) -> ScopeIdx {
        let idx = self.scopes.len();
        self.scopes.push(scope);
        ScopeIdx(idx)
    }
}
