//! State carried across batches and routing decisions.

use crate::{Component, Dataset, SaciError, SaciResult};

/// Cross-batch state, held as a [`Dataset`] resource.
///
/// A pipeline declares its one state component with
/// [`Pipeline::state`](crate::Pipeline::state). Before the systems run, the
/// rows the previous batch left behind (empty on a cold start) are inserted
/// into the batch dataset as a `State<C>`; whatever the systems leave in it is
/// serialised into the checkpoint afterwards.
///
/// It is a resource rather than a registered component because [`Dataset`]'s
/// Arrow IPC format requires every component to hold exactly the dataset's row
/// count, while state rows are independent of batch rows. Resources are not
/// serialised by [`Dataset::write_ipc`], so state never leaks into the output.
///
/// A single-row state component is the common case, and
/// [`get_or_insert_default`](Self::get_or_insert_default) covers it:
///
/// ```ignore
/// fn count_batches(data: &mut Dataset) -> SaciResult<()> {
///     sdk::State::<Counter>::get_or_insert_default(data)?.count += 1;
///     Ok(())
/// }
/// ```
pub struct State<C> {
    /// The state rows. Systems mutate this in place.
    pub rows: Vec<C>,
}

impl<C> State<C> {
    /// Wrap the rows restored from the previous batch.
    pub fn new(rows: Vec<C>) -> Self {
        Self { rows }
    }

    /// `true` on a cold start, before any system has written state.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl<C: Default + 'static> State<C> {
    /// Borrow the single state row, creating `C::default()` on a cold start.
    ///
    /// The shape almost every stateful processor wants: one row of running
    /// totals that starts at zero. It is what the `#[fold]` expansion calls.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::ResourceNotFound`] when no `State<C>` resource is
    /// on the dataset. That means the pipeline did not declare
    /// `.state::<C>()`, so nothing restored the previous batch's rows and
    /// nothing will capture these; inserting the resource here would silently
    /// drop every update at the batch boundary.
    pub fn get_or_insert_default(data: &mut Dataset) -> SaciResult<&mut C> {
        let state = data.get_resource_mut::<State<C>>().ok_or_else(|| {
            let name = std::any::type_name::<C>();
            SaciError::resource_not_found(format!(
                "State<{name}>: the pipeline did not declare `.state::<{name}>()`"
            ))
        })?;
        if state.rows.is_empty() {
            state.rows.push(C::default());
        }
        Ok(&mut state.rows[0])
    }
}

impl<C> Default for State<C> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// The branches this batch's output is delivered to.
///
/// A system inserts one into the batch dataset to route the output. With none
/// inserted, the host delivers the output to every downstream link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDecision {
    branches: Vec<String>,
}

impl RouteDecision {
    /// Deliver the output to each named branch.
    pub fn to<I, S>(branches: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            branches: branches.into_iter().map(Into::into).collect(),
        }
    }

    /// Deliver the output nowhere.
    pub fn none() -> Self {
        Self {
            branches: Vec::new(),
        }
    }

    /// The branch names, in the order given.
    pub fn branches(&self) -> &[String] {
        &self.branches
    }

    /// Take the branch names.
    pub fn into_branches(self) -> Vec<String> {
        self.branches
    }
}

/// Insert the rows of `C` that `prior` carries as a [`State<C>`] resource.
///
/// The blob is a standalone single-component Arrow IPC stream, so the host
/// persists it with the checkpoint machinery without knowing the schema.
pub(crate) fn restore<C>(data: &mut Dataset, prior: Option<&[u8]>) -> SaciResult<()>
where
    C: Component + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let rows = match prior {
        // No prior, or an empty payload: either way this batch is the
        // pipeline's first.
        None | Some([]) => Vec::new(),
        Some(bytes) => {
            let name = C::name();
            let prior_data = Dataset::read_ipc(&mut &bytes[..])?;
            match prior_data.batch_named(name) {
                Err(_) => Vec::new(),
                Ok(batch) => {
                    let on_disk = prior_data.schemas().get_version(name).unwrap_or(1);
                    let migrated = C::migrate(on_disk, batch.clone())?;
                    C::from_record_batch(&migrated)?
                }
            }
        }
    };
    data.insert_resource(State::<C>::new(rows));
    Ok(())
}

/// Serialise the [`State<C>`] resource the systems left on `data`.
pub(crate) fn capture<C>(data: &Dataset) -> SaciResult<Option<Vec<u8>>>
where
    C: Component + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let state = data.get_resource::<State<C>>().ok_or_else(|| {
        SaciError::generic(format!(
            "saci sdk: State<{}> resource is missing after the run; \
             a system must have replaced the dataset wholesale",
            C::name()
        ))
    })?;

    // Serialise through a scratch dataset holding only `C`, so the stream
    // satisfies Dataset's "every component has row_count rows" invariant
    // whatever the batch's own row count is.
    let mut scratch = Dataset::new();
    scratch.register::<C>()?;
    if !state.rows.is_empty() {
        scratch.append::<C>(&state.rows)?;
    }
    let mut buf: Vec<u8> = Vec::new();
    scratch.write_ipc(&mut buf)?;
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single-row state component, as `#[fold]` expects.
    #[derive(Default, Debug, PartialEq)]
    struct Ledger {
        settled_count: i64,
    }

    #[test]
    fn get_or_insert_default_creates_the_row_on_a_cold_start() {
        let mut data = Dataset::new();
        data.insert_resource(State::<Ledger>::default());

        State::<Ledger>::get_or_insert_default(&mut data)
            .expect("state resource present")
            .settled_count += 7;

        assert_eq!(
            data.get_resource::<State<Ledger>>().unwrap().rows,
            vec![Ledger { settled_count: 7 }]
        );
    }

    #[test]
    fn get_or_insert_default_reuses_the_restored_row() {
        let mut data = Dataset::new();
        data.insert_resource(State::new(vec![Ledger { settled_count: 41 }]));

        State::<Ledger>::get_or_insert_default(&mut data)
            .expect("state resource present")
            .settled_count += 1;

        let rows = &data.get_resource::<State<Ledger>>().unwrap().rows;
        assert_eq!(rows.len(), 1, "a second row would be a lost update");
        assert_eq!(rows[0].settled_count, 42);
    }

    #[test]
    fn get_or_insert_default_without_the_resource_is_an_error() {
        let mut data = Dataset::new();
        let err = State::<Ledger>::get_or_insert_default(&mut data)
            .expect_err("a stateless pipeline must not silently accumulate state");
        assert_eq!(err.category(), "resource_not_found");
        assert!(err.message().contains("Ledger"), "got: {err}");
        assert!(err.message().contains(".state::<"), "got: {err}");
    }

    #[test]
    fn route_decision_constructors() {
        assert_eq!(RouteDecision::to(["a", "b"]).branches(), ["a", "b"]);
        assert!(RouteDecision::none().branches().is_empty());
        assert_eq!(
            RouteDecision::to(vec![String::from("x")]).into_branches(),
            vec!["x".to_string()]
        );
    }
}
