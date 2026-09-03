//! MCP server front end.
//!
//! A second front end over the same command implementations the CLI calls, so
//! a flow behaves identically whether a human or an agent drives it.

use std::path::PathBuf;

use eyre::Result;
use serde_json::json;

use crate::commands::load_test;
use crate::mcp::context::McpContext;
use crate::mcp::policy::{SpendLimits, SpendPolicy};
use crate::mcp::server::{AxeMcp, SPEND_TOOLS};
use crate::mcp::transport::Endpoint;
use crate::types::Network;
use crate::ui;

pub mod activity;
pub mod args;
pub mod context;
pub mod guidance;
pub mod outcome;
pub mod policy;
pub mod runs;
pub mod server;
pub mod transport;

/// Start the server and serve until the client disconnects.
///
/// The network and the spend limits are taken once, here, and held for the
/// process lifetime.
pub async fn serve(
    network: Network,
    allow_mainnet: bool,
    limits: SpendLimits,
    endpoint: Endpoint,
) -> Result<()> {
    // A client-launched server inherits whatever directory the client chose,
    // so the CLI default of a relative path would put reports somewhere
    // unpredictable. Anchor them under the per-user data dir instead.
    let reports_dir = report_dir();
    load_test::set_report_dir(reports_dir.clone());

    // The spend ledger sits next to the reports it accounts for, so a
    // lifetime budget survives a restart.
    let policy = SpendPolicy::persistent(limits, reports_dir.join("spend-ledger.json"))?;
    let context = McpContext::new(network, allow_mainnet, reports_dir.clone(), policy)?;

    activity::startup(&activity::Startup {
        network,
        endpoint: match &endpoint {
            Endpoint::Stdio => "stdio".to_string(),
            Endpoint::Http { listen, .. } => format!("http://{listen}/mcp"),
        },
        caps: context.policy().describe(),
        reports_dir,
    });
    transport::serve(AxeMcp::new(context), endpoint).await
}

/// Print what the server offers: every tool with the first sentence of its
/// description, and every documentation resource. The catalogue is static, so
/// this needs neither a network nor a running server.
pub fn list(json: bool) -> Result<()> {
    let tools = AxeMcp::catalogue();
    let resources = guidance::doc_resources();

    if json {
        let catalogue = json!({ "tools": tools, "resources": resources });
        println!("{}", serde_json::to_string_pretty(&catalogue)?);
        return Ok(());
    }

    ui::section(&format!("Tools ({})", tools.len()));
    for tool in &tools {
        let spends = if SPEND_TOOLS.contains(&tool.name.as_ref()) {
            "[spends funds] "
        } else {
            ""
        };
        let description = tool.description.as_deref().unwrap_or_default();
        ui::kv(
            &tool.name,
            &format!("{spends}{}", first_sentence(description)),
        );
    }

    ui::section(&format!("Resources ({})", resources.len()));
    for resource in &resources {
        let description = resource.description.as_deref().unwrap_or_default();
        ui::kv(&resource.uri, &format!("{}: {description}", resource.name));
    }

    Ok(())
}

/// Up to the first full stop, with the doc comment's line breaks folded.
fn first_sentence(text: &str) -> String {
    let folded = text.split_whitespace().collect::<Vec<_>>().join(" ");
    match folded.find(". ") {
        Some(end) => folded[..=end].to_string(),
        None => folded,
    }
}

/// Where load-test reports written by this server land.
///
/// Deliberately not the CLI's working-directory-relative location, which
/// existing scripts glob and which must keep working unchanged.
fn report_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("axe")
        .join("load-test-runs")
}

#[cfg(test)]
mod tests {
    use super::{first_sentence, report_dir};

    #[test]
    fn first_sentence_folds_wrapped_lines_and_stops_at_the_full_stop() {
        assert_eq!(
            first_sentence("Look up a block\nheight. With no arguments this\nreports the head."),
            "Look up a block height."
        );
        assert_eq!(first_sentence("No full stop here"), "No full stop here");
        assert_eq!(first_sentence("Ends. "), "Ends.");
    }

    #[test]
    fn reports_land_under_the_per_user_data_dir() {
        let dir = report_dir();
        assert!(dir.ends_with("axe/load-test-runs"), "{}", dir.display());
        assert!(dir.is_absolute() || dir.starts_with("."));
    }
}
