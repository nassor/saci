//! `StatelessRuntime` and the direct `PipelineRuntime` impl coexist downstream.
//!
//! `impl<T: StatelessRuntime> PipelineRuntime for T` is a blanket impl, so
//! coherence decides whether a crate other than saci can still implement
//! `PipelineRuntime` for its own type. This test is such a crate: it
//! implements `PipelineRuntime` directly for a stateful runtime, implements
//! `StatelessRuntime` for another, and drives both through one
//! `Box<dyn PipelineRuntime>` call site.

#![cfg(feature = "engine")]

use async_trait::async_trait;

use saci::prelude::*;

/// Persists a counter in the opaque state blob, so it needs the full trait.
struct Stateful;

#[async_trait(?Send)]
impl PipelineRuntime for Stateful {
    fn name(&self) -> &str {
        "stateful"
    }

    async fn run_batch(
        &self,
        _data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput> {
        let n = prior.and_then(|b| b.first().copied()).unwrap_or(0) + 1;
        Ok(RuntimeOutput {
            state: Some(vec![n]),
            routes: None,
            branch_batches: None,
        })
    }

    fn template_dataset(&self) -> Dataset {
        Dataset::new()
    }
}

/// Keeps nothing between batches, so the short trait is enough.
struct Stateless;

#[async_trait(?Send)]
impl StatelessRuntime for Stateless {
    fn name(&self) -> &str {
        "stateless"
    }

    async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
        Ok(())
    }

    fn template_dataset(&self) -> Dataset {
        Dataset::new()
    }
}

#[tokio::test]
async fn direct_and_stateless_runtimes_share_one_call_site() {
    let runtimes: Vec<Box<dyn PipelineRuntime>> = vec![Box::new(Stateful), Box::new(Stateless)];

    let mut names = Vec::new();
    let mut states = Vec::new();
    for rt in &runtimes {
        let mut data = rt.template_dataset();
        let first = rt.run_batch(&mut data, None).await.unwrap();
        let second = rt
            .run_batch(&mut data, first.state.as_deref())
            .await
            .unwrap();
        names.push(rt.name().to_string());
        states.push(second.state);
    }

    assert_eq!(names, ["stateful", "stateless"]);
    assert_eq!(states, [Some(vec![2]), None]);
}
