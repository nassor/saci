//! Checkpoint frequency strategy for Arrow distributed pipelines.

/// Controls how often the runner writes Arrow IPC checkpoints during pipeline
/// execution.
///
/// # Trade-offs
///
/// - [`EveryStage`](Self::EveryStage): safest; re-processes at most one stage
///   after a failure. Higher write overhead.
/// - [`EveryNStages`](Self::EveryNStages): balanced; re-processes up to N
///   stages.
/// - [`None`](Self::None): no checkpoints; re-processes the entire batch on
///   failure. Zero overhead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStrategy {
    /// Checkpoint after every pipeline stage.
    EveryStage,
    /// Checkpoint after every `n` stages.
    EveryNStages(usize),
    /// Never checkpoint.
    None,
}

impl CheckpointStrategy {
    /// Return `true` if a checkpoint should be written after stage `stage_idx`.
    pub fn should_checkpoint(&self, stage_idx: usize) -> bool {
        match self {
            Self::EveryStage => true,
            Self::EveryNStages(n) => *n > 0 && (stage_idx + 1).is_multiple_of(*n),
            Self::None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_stage_checkpoints_all() {
        let s = CheckpointStrategy::EveryStage;
        for i in 0..10 {
            assert!(s.should_checkpoint(i), "stage {i}");
        }
    }

    #[test]
    fn test_none_never_checkpoints() {
        let s = CheckpointStrategy::None;
        for i in 0..10 {
            assert!(!s.should_checkpoint(i), "stage {i}");
        }
    }

    #[test]
    fn test_every_n_stages() {
        let s = CheckpointStrategy::EveryNStages(3);
        // checkpoint at stage 2, 5, 8 (0-indexed)
        assert!(!s.should_checkpoint(0));
        assert!(!s.should_checkpoint(1));
        assert!(s.should_checkpoint(2));
        assert!(!s.should_checkpoint(3));
        assert!(!s.should_checkpoint(4));
        assert!(s.should_checkpoint(5));
        assert!(s.should_checkpoint(8));
    }

    #[test]
    fn test_every_n_stages_with_zero_never_checkpoints() {
        let s = CheckpointStrategy::EveryNStages(0);
        for i in 0..10 {
            assert!(!s.should_checkpoint(i));
        }
    }
}
