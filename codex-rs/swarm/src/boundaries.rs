use crate::DomainProfile;
use crate::PatternDefinition;
use crate::RiskOverlay;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCapacity {
    pub max_active_agents: usize,
    pub max_total_agents: usize,
    pub max_cell_size: usize,
    pub max_depth: usize,
}

impl Default for ExecutionCapacity {
    fn default() -> Self {
        Self {
            max_active_agents: 4,
            max_total_agents: 12,
            max_cell_size: 4,
            max_depth: 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmBoundaries {
    pub logical_agents: usize,
    pub initial_active_agents: usize,
    pub max_active_agents: usize,
    pub cell_size: usize,
    pub cell_count: usize,
    pub max_depth: usize,
}

impl SwarmBoundaries {
    pub const DIRECT: Self = Self {
        logical_agents: 0,
        initial_active_agents: 0,
        max_active_agents: 0,
        cell_size: 0,
        cell_count: 0,
        max_depth: 0,
    };

    pub(crate) fn derive(
        definition: PatternDefinition,
        domain: DomainProfile,
        risk: RiskOverlay,
        capacity: ExecutionCapacity,
    ) -> Self {
        if definition.default_logical_agents == 0 {
            return Self::DIRECT;
        }

        let domain_extra = domain.specialist_axes.saturating_sub(2) as usize;
        let risk_extra = usize::from(risk.ambiguous_scope || risk.broad_surface);
        let logical_agents = (definition.default_logical_agents + domain_extra + risk_extra)
            .min(capacity.max_total_agents)
            .max(1);
        let risk_active_cap = if risk.is_high_risk() { 3 } else { usize::MAX };
        let max_active_agents = definition
            .default_active_agents
            .saturating_add(risk_extra)
            .min(logical_agents)
            .min(capacity.max_active_agents)
            .min(risk_active_cap)
            .max(1);
        let initial_active_agents = max_active_agents.min(2);
        let cell_size = if definition.supports_cells {
            capacity.max_cell_size.max(1).min(logical_agents)
        } else {
            logical_agents
        };

        Self {
            logical_agents,
            initial_active_agents,
            max_active_agents,
            cell_size,
            cell_count: logical_agents.div_ceil(cell_size),
            max_depth: definition.default_depth.min(capacity.max_depth).max(1),
        }
    }
}
