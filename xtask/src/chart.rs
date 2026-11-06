//! `check-chart [--values FILE]`: render the Helm chart and parse every document.
//!
//! `helm template` and `helm lint` don't parse what they render, and neither
//! does `kubectl`'s client side, strictly enough to notice two documents run
//! together. kustomize does, which is what ArgoCD uses: a stray `-}}` once
//! swallowed the newline before a `---`, turned `storage: 500Gi` into
//! `storage: 500Gi---`, passed all of those, and stopped a deploy (gh #320).
//! `serde_yaml` refuses duplicate keys, which is how that shows up.

use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

const CHART: &str = "deploy/helm/kaas";
const CI_VALUES: &str = "deploy/helm/ci";

/// How many non-empty documents `rendered` holds, or why it doesn't parse.
fn parse_documents(rendered: &str) -> Result<usize> {
    let mut documents = 0;
    for document in serde_yaml::Deserializer::from_str(rendered) {
        if !serde_yaml::Value::deserialize(document)?.is_null() {
            documents += 1;
        }
    }
    Ok(documents)
}

fn check_render(root: &Path, label: &str, args: &[&str]) -> Result<()> {
    let output = Command::new("helm")
        .current_dir(root)
        .args(["template", "check-chart", CHART])
        .args(args)
        .output()
        .context("running helm, which check-chart needs")?;
    if !output.status.success() {
        bail!(
            "{label}: helm template failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let rendered = String::from_utf8(output.stdout).context("helm wrote invalid UTF-8")?;
    let documents = parse_documents(&rendered)
        .with_context(|| format!("{label}: the chart renders invalid YAML"))?;
    if documents == 0 {
        bail!("{label}: the chart rendered nothing");
    }
    println!("check-chart: {label}: {documents} documents parse");
    Ok(())
}

/// Every chart shape CI knows of, plus `extra_values` if given. The invalid
/// cases are expected to fail to render, and the `helm` CI job pins that.
pub fn check(root: &Path, extra_values: Option<&str>) -> Result<()> {
    check_render(root, "defaults", &[])?;
    check_render(
        root,
        "control plane",
        &["--set", "storage.controlPlane.enabled=true"],
    )?;

    let mut value_files: Vec<_> = std::fs::read_dir(root.join(CI_VALUES))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| !name.ends_with(".invalid.yaml"))
        .collect();
    value_files.sort();
    for name in value_files {
        let path = format!("{CI_VALUES}/{name}");
        check_render(root, &name, &["-f", &path])?;
    }

    if let Some(values) = extra_values {
        check_render(root, values, &["-f", values])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_are_counted_and_empty_ones_ignored() {
        let rendered = "---\na: 1\n---\n# only a comment\n---\nb: 2\n";
        assert_eq!(parse_documents(rendered).unwrap(), 2);
    }

    /// What the bug looked like: the `---` joined to the line before it, so two
    /// documents read as one with every key twice.
    #[test]
    fn two_documents_run_together_are_refused() {
        let rendered = "apiVersion: v1\nspec:\n  storage: 500Gi---\napiVersion: v1\nspec: {}\n";
        let err = parse_documents(rendered).unwrap_err().to_string();
        assert!(err.contains("duplicate"), "{err}");
    }
}
