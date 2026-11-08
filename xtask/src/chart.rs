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

fn render(root: &Path, label: &str, args: &[&str]) -> Result<String> {
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
    String::from_utf8(output.stdout).context("helm wrote invalid UTF-8")
}

/// The value the rendered StatefulSet gives env var `name`, if it sets one.
fn statefulset_env(rendered: &str, name: &str) -> Result<Option<String>> {
    for document in serde_yaml::Deserializer::from_str(rendered) {
        let document = serde_yaml::Value::deserialize(document)?;
        if document.get("kind").and_then(serde_yaml::Value::as_str) != Some("StatefulSet") {
            continue;
        }
        let containers = document
            .get("spec")
            .and_then(|v| v.get("template"))
            .and_then(|v| v.get("spec"))
            .and_then(|v| v.get("containers"))
            .and_then(serde_yaml::Value::as_sequence)
            .map_or(&[][..], Vec::as_slice);
        for container in containers {
            let env = container
                .get("env")
                .and_then(serde_yaml::Value::as_sequence)
                .map_or(&[][..], Vec::as_slice);
            for var in env {
                if var.get("name").and_then(serde_yaml::Value::as_str) == Some(name) {
                    return Ok(var
                        .get("value")
                        .and_then(serde_yaml::Value::as_str)
                        .map(str::to_owned));
                }
            }
        }
    }
    Ok(None)
}

/// `0` is a documented setting for these, and Sprig's `default` reads it as
/// unset. `retentionCheckIntervalSeconds: 0` was documented as turning the
/// sweep off and rendered as 300, so data kept being deleted (gh #342).
fn check_zero_values(root: &Path) -> Result<()> {
    const ZERO_IS_VALID: [(&str, &str, &str); 3] = [
        (
            "broker.flushIntervalMessages",
            "KAAS_FLUSH_INTERVAL_MESSAGES",
            "1",
        ),
        (
            "broker.retentionCheckIntervalSeconds",
            "KAAS_RETENTION_CHECK_INTERVAL",
            "300",
        ),
        (
            "broker.fsyncMaxLatencyMs",
            "KAAS_FSYNC_MAX_LATENCY_MS",
            "30000",
        ),
    ];
    let defaults = render(root, "zero values", &[])?;
    for (value, env, default) in ZERO_IS_VALID {
        let rendered = render(root, "zero values", &["--set", &format!("{value}=0")])?;
        let zero = statefulset_env(&rendered, env)?;
        if zero.as_deref() != Some("0") {
            bail!("{value}=0 renders {env} as {zero:?}, not 0");
        }
        let unset = statefulset_env(&defaults, env)?;
        if unset.as_deref() != Some(default) {
            bail!("the default for {value} renders {env} as {unset:?}, not {default}");
        }
    }
    println!("check-chart: zero values: all reach the broker");
    Ok(())
}

/// The rendered documents of `kind` from cert-manager.
fn cert_manager_documents(rendered: &str, kind: &str) -> Result<Vec<serde_yaml::Value>> {
    let mut out = Vec::new();
    for document in serde_yaml::Deserializer::from_str(rendered) {
        let document = serde_yaml::Value::deserialize(document)?;
        let api = document
            .get("apiVersion")
            .and_then(serde_yaml::Value::as_str);
        if document.get("kind").and_then(serde_yaml::Value::as_str) == Some(kind)
            && api.is_some_and(|a| a.starts_with("cert-manager.io/"))
        {
            out.push(document);
        }
    }
    Ok(out)
}

fn field<'a>(v: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a str> {
    path.iter()
        .try_fold(v, |v, key| v.get(*key))
        .and_then(serde_yaml::Value::as_str)
}

/// Clients of an internal TLS listener trust the CA in `ca.crt`, so the
/// broker certificate must come from the chart's CA, and the CA must keep its
/// key when it renews. A self-signed broker certificate, or a CA on
/// cert-manager's default `rotationPolicy: Always`, changes the trust anchor
/// at a renewal and breaks every client holding the old one (gh #343).
fn check_internal_ca(root: &Path) -> Result<()> {
    let rendered = render(
        root,
        "internal CA",
        &[
            "--set-json",
            r#"listeners=[{"name":"tls","port":9093,"type":"internal","tls":true,"authentication":{"type":"none"}}]"#,
        ],
    )?;
    let certificates = cert_manager_documents(&rendered, "Certificate")?;
    let issuers = cert_manager_documents(&rendered, "Issuer")?;
    let cas: Vec<_> = certificates
        .iter()
        .filter(|c| {
            c.get("spec").and_then(|s| s.get("isCA")) == Some(&serde_yaml::Value::Bool(true))
        })
        .collect();
    let [ca] = cas.as_slice() else {
        bail!(
            "internal CA: want one isCA Certificate, the chart renders {}",
            cas.len()
        );
    };
    let policy = field(ca, &["spec", "privateKey", "rotationPolicy"]);
    if policy != Some("Never") {
        bail!("internal CA: rotationPolicy is {policy:?}, not Never, so a CA renewal would change the trust anchor");
    }
    let ca_secret = field(ca, &["spec", "secretName"]);
    let Some(ca_issuer) = issuers
        .iter()
        .find(|i| ca_secret.is_some() && field(i, &["spec", "ca", "secretName"]) == ca_secret)
        .and_then(|i| field(i, &["metadata", "name"]))
    else {
        bail!("internal CA: no Issuer signs with the CA's Secret {ca_secret:?}");
    };
    let leaves: Vec<_> = certificates
        .iter()
        .filter(|c| {
            field(c, &["metadata", "name"]).is_some_and(|n| n.ends_with("-broker-internal-tls"))
        })
        .collect();
    let [leaf] = leaves.as_slice() else {
        bail!(
            "internal CA: want one broker certificate, the chart renders {}",
            leaves.len()
        );
    };
    let leaf_issuer = field(leaf, &["spec", "issuerRef", "name"]);
    if leaf_issuer != Some(ca_issuer) {
        bail!("internal CA: the broker certificate is issued by {leaf_issuer:?}, not the CA's Issuer {ca_issuer}");
    }
    println!("check-chart: internal CA: the broker certificate comes from a CA that keeps its key");
    Ok(())
}

fn check_render(root: &Path, label: &str, args: &[&str]) -> Result<()> {
    let rendered = render(root, label, args)?;
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
    check_zero_values(root)?;
    check_internal_ca(root)?;

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
