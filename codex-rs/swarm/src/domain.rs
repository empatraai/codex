#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkDomain {
    Software,
    Research,
    Analytics,
    DocumentsMedia,
    ProductStrategy,
    DesignCreative,
    Operations,
    Security,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidencePolicy {
    BuildAndTest,
    IndependentSources,
    DatasetValidation,
    ArtifactReview,
    DecisionRationale,
    VisualReview,
    OperationalState,
    ThreatEvidence,
    MixedEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainProfile {
    pub domain: WorkDomain,
    pub evidence: EvidencePolicy,
    pub specialist_axes: u8,
    pub prefers_independent_passes: bool,
}

pub const fn domain_profile(domain: WorkDomain) -> DomainProfile {
    use EvidencePolicy as Evidence;
    use WorkDomain as Domain;

    let (evidence, specialist_axes, independent) = match domain {
        Domain::Software => (Evidence::BuildAndTest, 3, false),
        Domain::Research => (Evidence::IndependentSources, 4, true),
        Domain::Analytics => (Evidence::DatasetValidation, 3, true),
        Domain::DocumentsMedia => (Evidence::ArtifactReview, 3, false),
        Domain::ProductStrategy => (Evidence::DecisionRationale, 4, true),
        Domain::DesignCreative => (Evidence::VisualReview, 3, true),
        Domain::Operations => (Evidence::OperationalState, 3, false),
        Domain::Security => (Evidence::ThreatEvidence, 4, true),
        Domain::Mixed => (Evidence::MixedEvidence, 5, true),
    };

    DomainProfile {
        domain,
        evidence,
        specialist_axes,
        prefers_independent_passes: independent,
    }
}
