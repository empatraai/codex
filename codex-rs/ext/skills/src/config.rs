/// Host-supplied configuration used by the skills extension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillsExtensionConfig {
    /// Whether the available-skills catalog is included in model context.
    pub include_instructions: bool,
    /// Whether host-owned skills are included in the turn-scoped catalog.
    ///
    /// Set this to `false` when core already owns the stable host catalog. Explicitly selected
    /// host bodies remain available regardless of this setting.
    pub include_host_catalog: bool,
    /// Whether bundled skills are eligible for discovery.
    pub bundled_skills_enabled: bool,
    /// Whether orchestrator-owned skills are eligible for discovery.
    pub orchestrator_skills_enabled: bool,
}
