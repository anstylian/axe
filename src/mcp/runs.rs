//! Background load-test runs and their report artifacts.
//!
//! A load test can run far longer than a client will hold a request open, and
//! a client that times out is expected to cancel. Cancelling a flow that has
//! already submitted transactions loses the record of money already spent, so
//! these runs detach: starting one returns an identifier, and the report is
//! read back once it lands.
//!
//! Finished runs persist a JSON report named after their identifier, so the
//! artifact on disk is the store. This registry tracks only what is still in
//! flight, which is why a completed run survives a restart and an in-flight
//! one does not.
//!
//! Runs spend funds from shared accounts, so the registry admits one at a
//! time: a second start while one is in flight is refused rather than queued,
//! and the refusal names the run that is holding the slot.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::task::JoinHandle;

/// What a caller learns about a run.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum RunState {
    /// Still executing in this process.
    Running { run_id: String },
    /// Finished, with its report.
    Finished {
        run_id: String,
        report: serde_json::Value,
    },
    /// No report, and not running here. Either it failed before writing one,
    /// or it was started by a server that has since restarted. Deliberately
    /// distinct from running: a caller must not read "no report yet" as
    /// "still working".
    Unknown { run_id: String },
}

/// One line of the run listing.
#[derive(Debug, Serialize)]
pub struct RunListEntry {
    pub run_id: String,
    pub state: RunStatus,
}

/// Whether a listed run is still going.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Finished,
}

/// What a caller gets back when a run is accepted.
#[derive(Debug, Serialize)]
pub struct RunStarted {
    pub run_id: String,
    pub network: String,
    pub source_chain: String,
    pub destination_chain: String,
    pub transactions: u64,
}

/// A start was refused because another run holds the spend slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunInFlight {
    pub run_id: String,
}

/// Every run identifier starts with this. Anything else in the reports
/// directory is not a run, whatever its extension.
pub const RUN_ID_PREFIX: &str = "axe-load-test-";

/// How often a draining server checks whether its runs have finished.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// The last identifier minted, so two runs started in the same millisecond
/// still get distinct, ordered identifiers.
static LAST_RUN_MILLIS: AtomicU64 = AtomicU64::new(0);

/// Mint an identifier for a new run.
///
/// Milliseconds since the epoch, forced strictly increasing within this
/// process. That keeps identifiers unique and sortable, so listing newest
/// first is a reverse sort rather than a stat of every file.
fn mint_run_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let now = u64::try_from(now).unwrap_or(u64::MAX);

    let previous = LAST_RUN_MILLIS
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
            Some(now.max(last.saturating_add(1)))
        })
        .unwrap_or(now);
    let millis = now.max(previous.saturating_add(1));

    format!("{RUN_ID_PREFIX}{millis}")
}

/// Tracks load-test runs started through this server.
#[derive(Clone)]
pub struct RunRegistry {
    reports_dir: PathBuf,
    in_flight: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl RunRegistry {
    pub fn new(reports_dir: PathBuf) -> Self {
        Self {
            reports_dir,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Mint an identifier, run a flow under it on its own thread, and record
    /// it as in flight. Refused while another run is in flight.
    ///
    /// The identifier is minted and the handle recorded under one lock, so two
    /// concurrent starts cannot both find the slot free.
    ///
    /// Deliberately not `tokio::spawn`: the load-test future is not `Send`, so
    /// it cannot be moved onto the server's runtime. Building it inside a
    /// fresh thread means it is created and polled in one place and never
    /// crosses a thread boundary, which is what removes the `Send`
    /// requirement. The cost is one thread and one runtime per run, which is
    /// acceptable for a flow that runs for minutes.
    ///
    /// `make_flow` is a closure rather than a future for the same reason: the
    /// future must not exist until it is on the thread that will poll it. It
    /// receives the identifier so the flow can name its report after it.
    pub fn start<M, F>(&self, make_flow: M) -> Result<String, RunInFlight>
    where
        M: FnOnce(String) -> F + Send + 'static,
        F: Future<Output = ()>,
    {
        let mut runs = self
            .in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        runs.retain(|_, handle| !handle.is_finished());
        if let Some(run_id) = runs.keys().next() {
            return Err(RunInFlight {
                run_id: run_id.clone(),
            });
        }

        let run_id = mint_run_id();
        let flow_id = run_id.clone();
        let handle = tokio::task::spawn_blocking(move || {
            // Nothing to report a build failure to: the caller already holds
            // its identifier and will see the run as unknown, which is
            // accurate.
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(make_flow(flow_id));
        });
        runs.insert(run_id.clone(), handle);

        Ok(run_id)
    }

    /// Identifiers of the runs still executing in this process.
    pub fn running(&self) -> Vec<String> {
        self.in_flight
            .lock()
            .map(|runs| {
                runs.iter()
                    .filter(|(_, handle)| !handle.is_finished())
                    .map(|(id, _)| id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Wait until nothing is executing in this process.
    ///
    /// For a stdio server whose client has gone: exiting now would take a run
    /// that has already spent funds with it and leave no report. Polling is
    /// enough, since nothing else is happening by then.
    pub async fn wait_for_in_flight(&self) {
        while !self.running().is_empty() {
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }
    }

    /// Whether a run is still executing in this process.
    fn is_running(&self, run_id: &str) -> bool {
        self.in_flight
            .lock()
            .is_ok_and(|runs| runs.get(run_id).is_some_and(|h| !h.is_finished()))
    }

    /// The state of one run, reading its artifact if it has landed.
    ///
    /// Only identifiers this registry could have minted are looked up, so a
    /// caller cannot read an arbitrary file in the reports directory as a
    /// report.
    pub async fn state(&self, run_id: &str) -> RunState {
        if !run_id.starts_with(RUN_ID_PREFIX) {
            return RunState::Unknown {
                run_id: run_id.to_string(),
            };
        }
        if let Some(report) = self.read_report(run_id).await {
            return RunState::Finished {
                run_id: run_id.to_string(),
                report,
            };
        }
        if self.is_running(run_id) {
            return RunState::Running {
                run_id: run_id.to_string(),
            };
        }
        RunState::Unknown {
            run_id: run_id.to_string(),
        }
    }

    async fn read_report(&self, run_id: &str) -> Option<serde_json::Value> {
        let path = self.reports_dir.join(format!("{run_id}.json"));
        let text = tokio::fs::read_to_string(path).await.ok()?;
        serde_json::from_str(&text).ok()
    }

    /// Known runs, newest first.
    ///
    /// Reports outlive the process, so this finds runs from earlier sessions
    /// too. Keying off the artifact rather than memory is the point.
    pub async fn list(&self) -> Vec<RunListEntry> {
        let mut ids = Vec::new();

        if let Ok(mut entries) = tokio::fs::read_dir(&self.reports_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(id) = entry.file_name().to_string_lossy().strip_suffix(".json")
                    && id.starts_with(RUN_ID_PREFIX)
                {
                    ids.push(id.to_string());
                }
            }
        }

        // A handle that finished without writing a report is a failed run
        // with nothing to show. Pruned here so it reads as missing, not as
        // finished.
        if let Ok(mut runs) = self.in_flight.lock() {
            runs.retain(|_, handle| !handle.is_finished());
            for id in runs.keys() {
                if !ids.contains(id) {
                    ids.push(id.clone());
                }
            }
        }

        ids.sort_unstable();
        ids.reverse();

        ids.into_iter()
            .map(|run_id| {
                let state = if self.is_running(&run_id) {
                    RunStatus::Running
                } else {
                    RunStatus::Finished
                };
                RunListEntry { run_id, state }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use serde_json::json;

    use super::{RunInFlight, RunRegistry, RunState, RunStatus, mint_run_id};

    static DIRS: AtomicUsize = AtomicUsize::new(0);

    /// A fresh, empty reports directory per test.
    fn scratch_reports_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "axe-mcp-runs-{}-{}",
            std::process::id(),
            DIRS.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_report(dir: &std::path::Path, run_id: &str, report: &serde_json::Value) {
        std::fs::write(dir.join(format!("{run_id}.json")), report.to_string()).unwrap();
    }

    async fn wait_until_finished(registry: &RunRegistry, run_id: &str) {
        for _ in 0..500 {
            if !registry.is_running(run_id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{run_id} did not finish");
    }

    #[test]
    fn run_ids_are_unique_and_ascending_within_a_burst() {
        let ids: Vec<String> = (0..50).map(|_| mint_run_id()).collect();
        for pair in ids.windows(2) {
            assert!(
                pair[0] < pair[1],
                "{} should sort before {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[tokio::test]
    async fn unknown_run_has_no_state() {
        let registry = RunRegistry::new(scratch_reports_dir());
        assert!(matches!(
            registry.state("axe-load-test-0").await,
            RunState::Unknown { run_id } if run_id == "axe-load-test-0"
        ));
    }

    #[tokio::test]
    async fn finished_run_is_read_from_its_report_file() {
        let dir = scratch_reports_dir();
        let report = json!({"total_txs": 3, "network": "testnet"});
        write_report(&dir, "axe-load-test-1700000000000", &report);
        let registry = RunRegistry::new(dir);

        match registry.state("axe-load-test-1700000000000").await {
            RunState::Finished {
                run_id,
                report: read,
            } => {
                assert_eq!(run_id, "axe-load-test-1700000000000");
                assert_eq!(read, report);
            }
            other => panic!("expected finished, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unparseable_report_reads_as_unknown() {
        let dir = scratch_reports_dir();
        std::fs::write(dir.join("axe-load-test-5.json"), "not json").unwrap();
        let registry = RunRegistry::new(dir);
        assert!(matches!(
            registry.state("axe-load-test-5").await,
            RunState::Unknown { .. }
        ));
    }

    #[tokio::test]
    async fn listing_is_newest_first_and_ignores_other_files() {
        let dir = scratch_reports_dir();
        write_report(&dir, "axe-load-test-1700000000001", &json!({}));
        write_report(&dir, "axe-load-test-1700000000002", &json!({}));
        std::fs::write(dir.join("notes.txt"), "ignored").unwrap();
        let registry = RunRegistry::new(dir);

        let listed = registry.list().await;
        let ids: Vec<&str> = listed.iter().map(|entry| entry.run_id.as_str()).collect();
        assert_eq!(
            ids,
            ["axe-load-test-1700000000002", "axe-load-test-1700000000001"]
        );
        assert!(
            listed
                .iter()
                .all(|entry| entry.state == RunStatus::Finished)
        );
    }

    #[tokio::test]
    async fn only_one_run_is_admitted_at_a_time() {
        let registry = RunRegistry::new(scratch_reports_dir());
        let (release, held) = tokio::sync::oneshot::channel::<()>();

        let first = registry
            .start(move |_| async move {
                let _ = held.await;
            })
            .unwrap();
        assert!(matches!(
            registry.state(&first).await,
            RunState::Running { .. }
        ));
        assert_eq!(
            registry.start(|_| async {}),
            Err(RunInFlight {
                run_id: first.clone()
            })
        );

        drop(release);
        wait_until_finished(&registry, &first).await;

        let second = registry.start(|_| async {}).unwrap();
        assert!(second > first, "identifiers keep ascending across runs");
        wait_until_finished(&registry, &second).await;
        assert!(registry.list().await.is_empty(), "no report, no listing");
    }

    #[tokio::test]
    async fn flow_receives_the_identifier_it_was_started_under() {
        let registry = RunRegistry::new(scratch_reports_dir());
        let (send_id, seen_id) = tokio::sync::oneshot::channel::<String>();

        let run_id = registry
            .start(move |id| async move {
                let _ = send_id.send(id);
            })
            .unwrap();

        assert_eq!(seen_id.await.unwrap(), run_id);
    }

    #[tokio::test]
    async fn files_without_the_run_prefix_are_not_runs() {
        let dir = scratch_reports_dir();
        std::fs::write(dir.join("spend-ledger.json"), r#"{"transactions":2}"#).unwrap();
        write_report(&dir, "axe-load-test-1700000000001", &json!({}));
        let registry = RunRegistry::new(dir);

        let ids: Vec<String> = registry
            .list()
            .await
            .into_iter()
            .map(|entry| entry.run_id)
            .collect();
        assert_eq!(ids, ["axe-load-test-1700000000001"]);
        assert!(matches!(
            registry.state("spend-ledger").await,
            RunState::Unknown { .. }
        ));
    }

    #[tokio::test]
    async fn waiting_for_in_flight_runs_returns_once_they_finish() {
        let registry = RunRegistry::new(scratch_reports_dir());
        let (release, held) = tokio::sync::oneshot::channel::<()>();
        let run_id = registry
            .start(move |_| async move {
                let _ = held.await;
            })
            .unwrap();
        assert_eq!(registry.running(), [run_id]);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(release);
        });
        registry.wait_for_in_flight().await;
        assert!(registry.running().is_empty());
    }
}
