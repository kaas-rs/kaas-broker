//! Repo-wide chores: `gen-proto`, `gen-crds`, `check-crd-drift`,
//! `gen-api-matrix`, `check-docs-drift`, `check-chart`, `fmt-check`, `ci`, `docs`.
//!
//! `gen-crds` (Phase 7) walks the four `kube-derive` types in
//! `kaas-operator-api`, canonicalises the generated YAML to drop the
//! `controller-gen.kubebuilder.io/version` annotation (kube-rs
//! doesn't run controller-gen, so the marker is meaningless), and
//! writes the result to both `deploy/crds/` and
//! `deploy/helm/kaas/crds/`. `check-crd-drift` runs the same and
//! errors if the two trees diverge from `HEAD`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

use anyhow::{bail, Context, Result};
use kaas_operator_api::{KafkaCluster, KafkaTopic, KafkaUser};
use kube::CustomResourceExt;

mod api_matrix;
mod chart;

fn main() -> Result<()> {
    let task = env::args().nth(1).unwrap_or_default();
    match task.as_str() {
        "gen-proto" => gen_proto(),
        "gen-crds" => gen_crds(),
        "check-crd-drift" => check_crd_drift(),
        "gen-api-matrix" => api_matrix::generate(&repo_root()?),
        "check-docs-drift" => check_docs_drift(),
        "check-chart" => {
            let args: Vec<String> = env::args().skip(2).collect();
            let values = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                [] => None,
                ["--values", file] => Some(file),
                _ => bail!("usage: check-chart [--values FILE]"),
            };
            chart::check(&repo_root()?, values)
        }
        "fmt-check" => run(&["cargo", "fmt", "--check"]),
        "ci" => ci(),
        "docs" => docs(),
        other => Err(anyhow::anyhow!(
            "unknown xtask: {other:?}. \
             try: gen-proto | gen-crds | check-crd-drift | gen-api-matrix | \
             check-docs-drift | check-chart [--values FILE] | fmt-check | ci | docs [--serve]"
        )),
    }
}

fn gen_proto() -> Result<()> {
    // tonic-build runs inside kaas-broker's build.rs. Forcing a rebuild
    // here means the generated code lands in target/ regardless of
    // cargo's incremental cache.
    run(&["cargo", "build", "-p", "kaas-broker"])
}

fn gen_crds() -> Result<()> {
    let root = repo_root()?;
    let crds_root = root.join("deploy").join("crds");
    let chart_crds = root.join("deploy").join("helm").join("kaas").join("crds");
    fs::create_dir_all(&crds_root)?;
    fs::create_dir_all(&chart_crds)?;

    type Renderer = fn() -> Result<String>;
    let entries: &[(&str, Renderer)] = &[
        ("kaas.rs_kafkaclusters.yaml", kafkacluster_yaml),
        ("kaas.rs_kafkatopics.yaml", kafkatopic_yaml),
        ("kaas.rs_kafkausers.yaml", kafkauser_yaml),
    ];

    for (filename, render) in entries {
        let yaml = render().with_context(|| format!("rendering {filename}"))?;
        let canonical =
            canonicalise(&yaml).with_context(|| format!("canonicalising {filename}"))?;
        write_atomic(&crds_root.join(filename), &canonical)?;
        write_atomic(&chart_crds.join(filename), &canonical)?;
    }
    eprintln!("gen-crds: wrote {} CRDs", entries.len());
    Ok(())
}

fn check_crd_drift() -> Result<()> {
    gen_crds()?;
    run(&[
        "git",
        "diff",
        "--exit-code",
        "deploy/crds",
        "deploy/helm/kaas/crds",
    ])
}

fn check_docs_drift() -> Result<()> {
    let root = repo_root()?;
    api_matrix::generate(&root)?;
    run(&[
        "git",
        "diff",
        "--exit-code",
        "docs/src/compat/api-matrix.md",
    ])?;
    api_matrix::check_api_anchors(&root)?;
    api_matrix::scan_source_paths(&root)
}

fn docs() -> Result<()> {
    // Needs mdbook + mdbook-mermaid + mdbook-linkcheck on PATH; CI
    // pins the versions in the `docs` job of ci.yml.
    if env::args().nth(2).as_deref() == Some("--serve") {
        run(&["mdbook", "serve", "docs"])
    } else {
        run(&["mdbook", "build", "docs"])
    }
}

fn ci() -> Result<()> {
    run(&["cargo", "fmt", "--check"])?;
    run(&[
        "cargo",
        "clippy",
        "--workspace",
        "--all-targets",
        "--",
        "-D",
        "warnings",
    ])?;
    run(&["cargo", "test", "--workspace"])?;
    run(&["cargo", "build", "--release", "--workspace", "--bins"])?;
    Ok(())
}

// --- CRD renderers --------------------------------------------------

fn kafkacluster_yaml() -> Result<String> {
    crd_yaml(&KafkaCluster::crd())
}
fn kafkatopic_yaml() -> Result<String> {
    crd_yaml(&KafkaTopic::crd())
}
fn kafkauser_yaml() -> Result<String> {
    crd_yaml(&KafkaUser::crd())
}

fn crd_yaml<T: serde::Serialize>(crd: &T) -> Result<String> {
    // serde_yaml writes a leading `---` document separator only when
    // we ask for it. controller-gen emits one, so we wrap the output
    // to match.
    let body = serde_yaml::to_string(crd)?;
    Ok(if body.starts_with("---\n") {
        body
    } else {
        format!("---\n{body}")
    })
}

// --- canonicalisation ----------------------------------------------

/// Drop the `controller-gen.kubebuilder.io/version` annotation
/// (and the empty `metadata.annotations` map it leaves behind)
/// before writing. kube-rs doesn't run controller-gen so stamping a
/// version number that doesn't exist would be misleading; dropping
/// it produces a stable, single one-shot diff at Phase 7 merge
/// time, and every subsequent `cargo xtask gen-crds` run is a
/// clean no-op.
///
/// Round-trip through `serde_yaml::Value` so the mutation is
/// structural rather than text-substitution.
fn canonicalise(yaml: &str) -> Result<String> {
    use serde_yaml::Value;
    let mut value: Value = serde_yaml::from_str(yaml)?;
    // schemars emits `default: ""` for `#[serde(default)]` String
    // fields, but the apiserver validates defaults against the same
    // field's own constraints — a `default: ""` next to a
    // `minLength: 1` or a non-empty-matching `pattern` makes the
    // whole CRD unapplyable ("Invalid value: \"\""). An empty-string
    // default is also semantically inert (serde already treats the
    // absent field as empty), so strip every one of them.
    strip_empty_string_defaults(&mut value);
    strip_empty_name_lists(&mut value);
    if let Some(map) = value.as_mapping_mut() {
        if let Some(meta) = map
            .get_mut(Value::String("metadata".into()))
            .and_then(Value::as_mapping_mut)
        {
            if let Some(annotations) = meta
                .get_mut(Value::String("annotations".into()))
                .and_then(Value::as_mapping_mut)
            {
                annotations.remove(Value::String(
                    "controller-gen.kubebuilder.io/version".into(),
                ));
                if annotations.is_empty() {
                    meta.remove(Value::String("annotations".into()));
                }
            }
            // controller-gen also omits `creationTimestamp: null`; if
            // serde_yaml emitted one, drop it.
            if let Some(v) = meta.get(Value::String("creationTimestamp".into())) {
                if v.is_null() {
                    meta.remove(Value::String("creationTimestamp".into()));
                }
            }
        }
    }
    let body = serde_yaml::to_string(&value)?;
    Ok(if body.starts_with("---\n") {
        body
    } else {
        format!("---\n{body}")
    })
}

/// Remove empty `spec.names.categories` / `spec.names.shortNames`
/// sequences.
///
/// kube-derive always populates these as `Some(vec![])` rather than
/// `None`, so they serialise as literal `[]`. The apiserver's Go type
/// tags both `omitempty` on a `[]string`, which can't distinguish
/// empty from nil — so the stored object comes back with the keys
/// absent entirely. A GitOps differ (ArgoCD, `kubectl diff`) then
/// compares `categories: []` against nothing and reports a
/// permanently-pending "add these fields" change that applying never
/// resolves.
/// controller-gen omits them, so stripping matches the shape every
/// other CRD in the cluster has.
fn strip_empty_name_lists(value: &mut serde_yaml::Value) {
    use serde_yaml::Value;
    let Some(names) = value
        .get_mut(Value::String("spec".into()))
        .and_then(|s| s.get_mut(Value::String("names".into())))
        .and_then(Value::as_mapping_mut)
    else {
        return;
    };
    for key in ["categories", "shortNames"] {
        let is_empty = names
            .get(Value::String(key.into()))
            .is_some_and(|v| v.as_sequence().is_some_and(|s| s.is_empty()));
        if is_empty {
            // `shift_remove`, not `remove` — the latter is swap-remove
            // on the backing IndexMap and would rotate an unrelated
            // key into the hole, churning the generated YAML.
            names.shift_remove(Value::String(key.into()));
        }
    }
}

/// Recursively remove `default: ""` mapping entries. See the note in
/// [`canonicalise`] — empty-string defaults violate their own field
/// constraints under apiserver CRD validation and carry no meaning.
fn strip_empty_string_defaults(value: &mut serde_yaml::Value) {
    use serde_yaml::Value;
    match value {
        Value::Mapping(map) => {
            let is_empty_default = |v: &Value| matches!(v, Value::String(s) if s.is_empty());
            if map
                .get(Value::String("default".into()))
                .is_some_and(is_empty_default)
            {
                map.remove(Value::String("default".into()));
            }
            for (_, v) in map.iter_mut() {
                strip_empty_string_defaults(v);
            }
        }
        Value::Sequence(seq) => {
            for v in seq.iter_mut() {
                strip_empty_string_defaults(v);
            }
        }
        _ => {}
    }
}

// --- shared helpers ------------------------------------------------

fn write_atomic(path: &Path, contents: &str) -> Result<()> {
    let tmp = path.with_extension("yaml.tmp");
    fs::write(&tmp, contents.as_bytes()).with_context(|| format!("writing {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("renaming {}", tmp.display()))?;
    Ok(())
}

/// Repository root resolved from `CARGO_MANIFEST_DIR` (xtask's
/// manifest is at `<repo>/xtask/Cargo.toml`).
fn repo_root() -> Result<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .map(Path::to_path_buf)
        .context("xtask Cargo.toml has no parent (repo root)")
}

fn run(argv: &[&str]) -> Result<()> {
    let status = Command::new(argv[0]).args(&argv[1..]).status()?;
    if !status.success() {
        bail!("{:?} failed: {status}", argv);
    }
    Ok(())
}
