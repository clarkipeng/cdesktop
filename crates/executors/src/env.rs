use std::{collections::HashMap, path::PathBuf};

use git::GitService;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::command::CmdOverrides;

/// Repository context for executor operations
#[derive(Debug, Clone, Default)]
pub struct RepoContext {
    pub workspace_root: PathBuf,
    /// Names of repositories in the workspace (subdirectory names)
    pub repo_names: Vec<String>,
    /// Absolute on-disk paths of repositories in the workspace, in the same
    /// order as `repo_names`. For worktree-mode workspaces each entry is
    /// `<container_ref>/<name>`. For direct-mode workspaces each entry is
    /// the repo's own real path, which is NOT derivable from `workspace_root`
    /// because direct-mode secondaries can live anywhere on disk.
    pub repo_paths: Vec<PathBuf>,
}

impl RepoContext {
    pub fn new(workspace_root: PathBuf, repo_names: Vec<String>, repo_paths: Vec<PathBuf>) -> Self {
        Self {
            workspace_root,
            repo_names,
            repo_paths,
        }
    }

    /// Absolute on-disk paths for each repo. Prefer the explicit field; fall
    /// back to deriving from `workspace_root + repo_names` for legacy
    /// constructors that did not populate `repo_paths`.
    pub fn repo_paths(&self) -> Vec<PathBuf> {
        if !self.repo_paths.is_empty() {
            return self.repo_paths.clone();
        }
        self.repo_names
            .iter()
            .map(|name| self.workspace_root.join(name))
            .collect()
    }

    /// Check all repos for uncommitted changes.
    /// Returns a formatted string describing any uncommitted changes found,
    /// or an empty string if all repos are clean.
    pub async fn check_uncommitted_changes(&self) -> String {
        let repo_paths = self.repo_paths();
        if repo_paths.is_empty() {
            return String::new();
        }

        tokio::task::spawn_blocking(move || {
            let git = GitService::new();
            let mut all_status = String::new();

            for repo_path in &repo_paths {
                // Skip if not a git repository
                if !repo_path.join(".git").exists() {
                    continue;
                }

                match git.get_worktree_status(repo_path) {
                    Ok(status) if !status.entries.is_empty() => {
                        let mut status_output = String::new();
                        for entry in &status.entries {
                            status_output.push(entry.staged);
                            status_output.push(entry.unstaged);
                            status_output.push(' ');
                            status_output.push_str(&String::from_utf8_lossy(&entry.path));
                            status_output.push('\n');
                        }
                        all_status.push_str(&format!(
                            "\n{}:\n{}",
                            repo_path.display(),
                            status_output
                        ));
                    }
                    _ => {}
                }
            }

            all_status
        })
        .await
        .unwrap_or_default()
    }
}

/// Codex-specific spawn injection beyond plain env vars.
///
/// Codex's `app-server` JSON-RPC subcommand accepts arbitrary
/// `model_providers.<id>.<key>` overrides via `ThreadStartParams.config`
/// (a free-form `HashMap<String, serde_json::Value>` that the server feeds
/// to the same dotted-path applier the `-c key=value` CLI flag uses;
/// see `related/codex/.../apply_single_override`). The `model_provider`
/// field on `ThreadStartParams` is a separate typesafe knob that picks
/// which `model_providers.<id>` block to use.
///
/// Spawned only when the user picks a non-Default provider record for
/// a Codex session (see `Provider::build_codex_injection`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodexProviderInjection {
    /// Dotted-path keys merged into `ThreadStartParams.config`. Per plan §3.2:
    /// `model_providers.cdt.{name,base_url,env_key,wire_api}`.
    #[serde(default)]
    pub config_overrides: HashMap<String, serde_json::Value>,
    /// Value for `ThreadStartParams.model_provider`. Hardcoded to `"cdt"` for
    /// our injected provider id; carried explicitly so consumers don't have
    /// to know the magic slug.
    pub model_provider_id: String,
}

/// Environment variables to inject into executor processes
#[derive(Clone)]
pub struct ExecutionEnv {
    pub vars: HashMap<String, String>,
    pub repo_context: RepoContext,
    pub commit_reminder: bool,
    pub commit_reminder_prompt: String,
    /// Provider-selected env vars. Applied last in `apply_to_command`, after
    /// profile/cmd env, so per-message provider selection takes highest precedence.
    pub provider_vars: HashMap<String, String>,
    /// Codex-specific spawn injection (config overrides + model_provider id).
    /// Populated only when the active session's provider record routes Codex.
    pub provider_codex: Option<CodexProviderInjection>,
}

/// Manual `Debug` so a logged spawn environment can never print resolved
/// credential values: env var names stay visible, values are redacted.
impl std::fmt::Debug for ExecutionEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted_keys =
            |vars: &HashMap<String, String>| vars.keys().cloned().collect::<Vec<_>>();
        f.debug_struct("ExecutionEnv")
            .field("vars", &redacted_keys(&self.vars))
            .field("repo_context", &self.repo_context)
            .field("commit_reminder", &self.commit_reminder)
            .field("provider_vars", &redacted_keys(&self.provider_vars))
            .field(
                "provider_codex",
                &self.provider_codex.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl ExecutionEnv {
    pub fn new(
        repo_context: RepoContext,
        commit_reminder: bool,
        commit_reminder_prompt: String,
    ) -> Self {
        Self {
            vars: HashMap::new(),
            repo_context,
            commit_reminder,
            commit_reminder_prompt,
            provider_vars: HashMap::new(),
            provider_codex: None,
        }
    }

    /// Insert an environment variable
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.vars.insert(key.into(), value.into());
    }

    /// Merge additional vars into this env. Incoming keys overwrite existing ones.
    pub fn merge(&mut self, other: &HashMap<String, String>) {
        self.vars
            .extend(other.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    /// Return a new env with overrides applied. Overrides take precedence.
    pub fn with_overrides(mut self, overrides: &HashMap<String, String>) -> Self {
        self.merge(overrides);
        self
    }

    /// Return a new env with profile env from CmdOverrides merged in.
    pub fn with_profile(self, cmd: &CmdOverrides) -> Self {
        if let Some(ref profile_env) = cmd.env {
            self.with_overrides(profile_env)
        } else {
            self
        }
    }

    /// Apply all environment variables to a Command.
    /// `provider_vars` are applied last so per-message provider selection
    /// overrides any profile defaults.
    pub fn apply_to_command(&self, command: &mut Command) {
        for (key, value) in &self.vars {
            command.env(key, value);
        }
        for (key, value) in &self.provider_vars {
            command.env(key, value);
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.vars.contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.vars.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_overrides_runtime_env() {
        let mut base = ExecutionEnv::new(RepoContext::default(), false, String::new());
        base.insert("VK_PROJECT_NAME", "runtime");
        base.insert("FOO", "runtime");

        let mut profile = HashMap::new();
        profile.insert("FOO".to_string(), "profile".to_string());
        profile.insert("BAR".to_string(), "profile".to_string());

        let merged = base.with_overrides(&profile);

        assert_eq!(merged.vars.get("VK_PROJECT_NAME").unwrap(), "runtime");
        assert_eq!(merged.vars.get("FOO").unwrap(), "profile"); // overrides
        assert_eq!(merged.vars.get("BAR").unwrap(), "profile");
    }
}
