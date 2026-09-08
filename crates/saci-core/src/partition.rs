/// Routing metadata injected into a `Dataset` by `DistributedRunner` when
/// running multiple concurrent instances against disjoint key slices.
///
/// Each runner instance receives a different `instance_ordinal`. Window systems
/// use this to keep only the rows whose `key_hash % num_instances == instance_ordinal`
/// so that every runner accumulates a disjoint key slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPartition {
    /// Zero-based index for this runner (0 ≤ instance_ordinal < num_instances).
    pub instance_ordinal: u32,
    /// Total number of concurrent runner instances sharing the master batches.
    pub num_instances: u32,
}
