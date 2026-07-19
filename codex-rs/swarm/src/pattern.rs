#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmPattern {
    Direct,
    SequentialPipeline,
    ScoutSynthesize,
    MapReduce,
    PlannerWorkersReviewer,
    SpecialistCouncil,
    AdversarialReview,
    IncidentCommand,
    DurableWorkflow,
    PhaseGraph,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternTopology {
    Direct,
    Pipeline,
    FanOutFanIn,
    Council,
    Command,
    Durable,
    Graph,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PatternDefinition {
    pub pattern: SwarmPattern,
    pub topology: PatternTopology,
    pub default_logical_agents: usize,
    pub default_active_agents: usize,
    pub default_depth: usize,
    pub supports_cells: bool,
}

pub const ALL_PATTERNS: [SwarmPattern; 10] = [
    SwarmPattern::Direct,
    SwarmPattern::SequentialPipeline,
    SwarmPattern::ScoutSynthesize,
    SwarmPattern::MapReduce,
    SwarmPattern::PlannerWorkersReviewer,
    SwarmPattern::SpecialistCouncil,
    SwarmPattern::AdversarialReview,
    SwarmPattern::IncidentCommand,
    SwarmPattern::DurableWorkflow,
    SwarmPattern::PhaseGraph,
];

pub const fn pattern_definition(pattern: SwarmPattern) -> PatternDefinition {
    use PatternTopology as Topology;
    use SwarmPattern as Pattern;

    let (topology, logical, active, depth, supports_cells) = match pattern {
        Pattern::Direct => (Topology::Direct, 0, 0, 0, false),
        Pattern::SequentialPipeline => (Topology::Pipeline, 2, 1, 2, false),
        Pattern::ScoutSynthesize => (Topology::FanOutFanIn, 4, 3, 2, true),
        Pattern::MapReduce => (Topology::FanOutFanIn, 6, 4, 2, true),
        Pattern::PlannerWorkersReviewer => (Topology::Graph, 5, 3, 3, true),
        Pattern::SpecialistCouncil => (Topology::Council, 4, 3, 2, true),
        Pattern::AdversarialReview => (Topology::Council, 3, 2, 2, false),
        Pattern::IncidentCommand => (Topology::Command, 4, 3, 3, true),
        Pattern::DurableWorkflow => (Topology::Durable, 2, 1, 3, false),
        Pattern::PhaseGraph => (Topology::Graph, 6, 4, 4, true),
    };

    PatternDefinition {
        pattern,
        topology,
        default_logical_agents: logical,
        default_active_agents: active,
        default_depth: depth,
        supports_cells,
    }
}
