//! Sticky composer defaults — the new-chat "remember my last picks" store
//! (zeron parity: localStorage `zeron.composer.defaults:v1`, defaults.ts).
//!
//! A small JSON file beside `ui-settings.json` (that file is the shell's and
//! is saved debounced from its own boot-time copy, so the composer keeps its
//! own file rather than racing it): last harness, last model per harness
//! (id + label, so the chip names the pick before the model list loads),
//! and last reasoning level. Written synchronously on every pick (picks are
//! rare); corrupt or missing files fall back to defaults.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use zeron_proto::{HarnessId, Model, ReasoningLevel};

const FILE_NAME: &str = "composer-defaults.json";

/// One model's option selections (option id → choice id), the same shape
/// `ChatConfig::model_options` carries.
pub type OptionPicks = serde_json::Map<String, serde_json::Value>;

/// Remembered model per harness — id plus display label, mirroring zeron's
/// `modelByHarness` storing the full `Model` object "so the pill never flashes
/// a raw id or 'Default'".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RememberedModel {
    pub id: String,
    pub label: String,
}

/// One starred model in the picker (t3code client-settings `favorites`,
/// keyed `provider:model`) — harness + model id, insertion-ordered.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FavoriteModel {
    pub harness: HarnessId,
    pub model: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ComposerDefaults {
    /// Last harness picked on the new-chat canvas.
    pub harness: Option<HarnessId>,
    /// Last model picked, per harness (restored on harness switch).
    pub model_by_harness: HashMap<HarnessId, RememberedModel>,
    /// Last reasoning level picked (global, like zeron's `reasoning` key).
    pub reasoning: Option<ReasoningLevel>,
    /// Last model-option picks (the Traits popover's Context Window / Fast
    /// Mode / Thinking / Service Tier rows), keyed harness → model id →
    /// (option id → choice id). Model-keyed because the option set belongs to
    /// the model (Opus carries Fast Mode, Sonnet doesn't), harness-keyed
    /// because ACP harnesses share generic model ids like `default`.
    pub options_by_model: HashMap<HarnessId, HashMap<String, OptionPicks>>,
    /// Every model label ever seen (id → label), fed from catalog loads.
    /// The chip's fallback while a harness's list is still loading — a
    /// session whose configured model differs from the remembered pick
    /// would otherwise flash the raw id on switch.
    pub model_labels: HashMap<String, String>,
    /// Every model-option / choice label ever seen, keyed `optionId` and
    /// `optionId/choiceId`. Lets Settings render the remembered picks as
    /// "Context Window: 1M" without loading a model catalog of its own.
    pub option_labels: HashMap<String, String>,
    /// Last device picked for new sessions (the composer's device selector).
    pub device: Option<String>,
    /// Last project picked for new sessions; `None` + `no_project` = the
    /// remembered "Don't work in a project" state.
    pub project: Option<String>,
    /// Remembered "Don't work in a project" opt-out.
    pub no_project: bool,
    /// Starred models (the picker's favorites rail), in starring order.
    pub favorites: Vec<FavoriteModel>,
}

impl ComposerDefaults {
    /// Load from `{data_dir}/composer-defaults.json`; defaults on any failure.
    pub fn load(data_dir: &Path) -> Self {
        match std::fs::read_to_string(Self::path(data_dir)) {
            Ok(text) => match serde_json::from_str::<ComposerDefaults>(&text) {
                Ok(defaults) => defaults,
                Err(err) => {
                    tracing::warn!(error = %err, "composer-defaults corrupt; using defaults");
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        }
    }

    /// Write atomically (temp file + rename) so a crash mid-write never corrupts.
    pub fn save(&self, data_dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(data_dir)?;
        let path = Self::path(data_dir);
        let tmp = path.with_extension("json.tmp");
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, &path)
    }

    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join(FILE_NAME)
    }

    /// The remembered model for a harness, if any.
    pub fn model_for(&self, harness: HarnessId) -> Option<&RememberedModel> {
        self.model_by_harness.get(&harness)
    }

    /// Remember a pick (zeron `saveDefaults({ harness, modelByHarness })`).
    pub fn remember_model(&mut self, harness: HarnessId, id: String, label: String) {
        self.harness = Some(harness);
        self.model_by_harness
            .insert(harness, RememberedModel { id, label });
    }

    /// The remembered option picks for a model, if any.
    pub fn options_for(&self, harness: HarnessId, model: &str) -> Option<&OptionPicks> {
        self.options_by_model.get(&harness)?.get(model)
    }

    /// Remember a model's whole option selection. An empty map is stored as
    /// such rather than dropped — it means "everything back to default", and
    /// removing the entry would let an older memory refill the next new chat.
    pub fn remember_options(&mut self, harness: HarnessId, model: String, picks: OptionPicks) {
        self.options_by_model
            .entry(harness)
            .or_default()
            .insert(model, picks);
    }

    /// The cached display label for a model id, if ever seen.
    pub fn label_for(&self, id: &str) -> Option<&str> {
        self.model_labels.get(id).map(String::as_str)
    }

    /// Whether a model is starred.
    pub fn is_favorite(&self, harness: HarnessId, model: &str) -> bool {
        self.favorites
            .iter()
            .any(|f| f.harness == harness && f.model == model)
    }

    /// Star/unstar a model; returns whether it is starred AFTER the toggle.
    pub fn toggle_favorite(&mut self, harness: HarnessId, model: &str) -> bool {
        if let Some(at) = self
            .favorites
            .iter()
            .position(|f| f.harness == harness && f.model == model)
        {
            self.favorites.remove(at);
            false
        } else {
            self.favorites.push(FavoriteModel {
                harness,
                model: model.to_string(),
            });
            true
        }
    }

    /// Merge a loaded catalog into the label cache. Returns whether anything
    /// changed (callers only save when it did).
    pub fn remember_labels<'a>(
        &mut self,
        models: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> bool {
        let mut changed = false;
        for (id, label) in models {
            if self.model_labels.get(id).map(String::as_str) != Some(label) {
                self.model_labels.insert(id.to_string(), label.to_string());
                changed = true;
            }
        }
        changed
    }

    /// Merge a loaded catalog's option and choice labels. Same contract as
    /// [`Self::remember_labels`] — returns whether anything changed.
    pub fn remember_option_labels<'a>(&mut self, models: impl Iterator<Item = &'a Model>) -> bool {
        let mut changed = false;
        let mut put = |key: String, label: &str| {
            if self.option_labels.get(&key).map(String::as_str) != Some(label) {
                self.option_labels.insert(key, label.to_string());
                changed = true;
            }
        };
        for model in models {
            for option in &model.options {
                put(option.id.clone(), &option.label);
                for choice in &option.choices {
                    put(format!("{}/{}", option.id, choice.id), &choice.label);
                }
            }
        }
        changed
    }

    /// The remembered picks for a model as "Context Window: 1M · Fast Mode:
    /// On", for the Settings readout. Cached labels where they exist, raw ids
    /// otherwise (a catalog this device never loaded). `None` when nothing is
    /// remembered or every option sits at its default.
    pub fn options_summary(&self, harness: HarnessId, model: &str) -> Option<String> {
        let picks = self.options_for(harness, model)?;
        let parts: Vec<String> = picks
            .iter()
            .filter_map(|(option_id, value)| {
                let choice_id = value.as_str()?;
                let option = self.option_label(option_id, option_id);
                let choice = self.option_label(&format!("{option_id}/{choice_id}"), choice_id);
                Some(format!("{option}: {choice}"))
            })
            .collect();
        (!parts.is_empty()).then(|| parts.join(" · "))
    }

    fn option_label(&self, key: &str, fallback: &str) -> String {
        self.option_labels
            .get(key)
            .cloned()
            .unwrap_or_else(|| fallback.to_string())
    }

    /// Every remembered option selection, as (harness, model label, summary)
    /// rows for the Settings readout — models whose picks are all defaults are
    /// omitted. Sorted by label so the list doesn't reshuffle between opens.
    pub fn remembered_option_rows(&self) -> Vec<(HarnessId, String, String)> {
        let mut rows: Vec<(HarnessId, String, String)> = self
            .options_by_model
            .iter()
            .flat_map(|(harness, models)| {
                models.keys().filter_map(move |model| {
                    let summary = self.options_summary(*harness, model)?;
                    let label = self.label_for(model).unwrap_or(model).to_string();
                    Some((*harness, label, summary))
                })
            })
            .collect();
        rows.sort_by(|a, b| a.1.cmp(&b.1));
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut defaults = ComposerDefaults {
            harness: Some(HarnessId::ClaudeCode),
            reasoning: Some(ReasoningLevel::XHigh),
            ..Default::default()
        };
        defaults.remember_model(
            HarnessId::ClaudeCode,
            "claude-fable-5".into(),
            "Fable 5".into(),
        );
        defaults.remember_model(HarnessId::Codex, "gpt-5.2-codex".into(), "GPT-5.2".into());
        defaults.save(dir.path()).unwrap();
        let loaded = ComposerDefaults::load(dir.path());
        assert_eq!(loaded, defaults);
        assert_eq!(
            loaded.model_for(HarnessId::ClaudeCode).map(|m| &*m.label),
            Some("Fable 5")
        );
    }

    #[test]
    fn missing_and_corrupt_files_yield_defaults() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            ComposerDefaults::load(dir.path()),
            ComposerDefaults::default()
        );
        std::fs::write(ComposerDefaults::path(dir.path()), "{nope").unwrap();
        assert_eq!(
            ComposerDefaults::load(dir.path()),
            ComposerDefaults::default()
        );
    }

    #[test]
    fn favorites_toggle_and_persist() {
        let dir = tempfile::tempdir().unwrap();
        let mut defaults = ComposerDefaults::default();
        assert!(defaults.toggle_favorite(HarnessId::ClaudeCode, "claude-opus-5"));
        assert!(defaults.toggle_favorite(HarnessId::Codex, "gpt-5.2-codex"));
        assert!(defaults.is_favorite(HarnessId::ClaudeCode, "claude-opus-5"));
        // Same id under a different harness is a distinct star.
        assert!(!defaults.is_favorite(HarnessId::Codex, "claude-opus-5"));
        defaults.save(dir.path()).unwrap();
        assert_eq!(ComposerDefaults::load(dir.path()), defaults);
        // Untoggle removes, preserving the other's order.
        assert!(!defaults.toggle_favorite(HarnessId::ClaudeCode, "claude-opus-5"));
        assert!(!defaults.is_favorite(HarnessId::ClaudeCode, "claude-opus-5"));
        assert!(defaults.is_favorite(HarnessId::Codex, "gpt-5.2-codex"));
    }

    #[test]
    fn option_picks_round_trip_and_summarize() {
        let dir = tempfile::tempdir().unwrap();
        let mut defaults = ComposerDefaults::default();
        let mut picks = OptionPicks::new();
        picks.insert("contextWindow".into(), "1m".into());
        defaults.remember_options(HarnessId::ClaudeCode, "claude-opus-5".into(), picks.clone());
        // Same model id under another harness is a distinct memory (ACP
        // harnesses all ship a model called "default").
        defaults.remember_options(HarnessId::Codex, "claude-opus-5".into(), OptionPicks::new());
        defaults.save(dir.path()).unwrap();
        let mut loaded = ComposerDefaults::load(dir.path());
        assert_eq!(loaded, defaults);
        assert_eq!(
            loaded.options_for(HarnessId::ClaudeCode, "claude-opus-5"),
            Some(&picks)
        );
        assert!(
            loaded
                .options_for(HarnessId::Codex, "claude-opus-5")
                .unwrap()
                .is_empty()
        );
        assert!(
            loaded
                .options_for(HarnessId::Cursor, "claude-opus-5")
                .is_none()
        );

        // Raw ids until a catalog is seen, labels afterwards.
        assert_eq!(
            loaded.options_summary(HarnessId::ClaudeCode, "claude-opus-5"),
            Some("contextWindow: 1m".into())
        );
        let model = Model {
            id: "claude-opus-5".into(),
            label: "Opus 5".into(),
            description: None,
            reasoning_levels: vec![],
            options: vec![zeron_proto::ModelOption {
                id: "contextWindow".into(),
                label: "Context Window".into(),
                choices: vec![
                    zeron_proto::ModelOptionChoice {
                        id: "200k".into(),
                        label: "200K".into(),
                    },
                    zeron_proto::ModelOptionChoice {
                        id: "1m".into(),
                        label: "1M".into(),
                    },
                ],
                default_choice: "200k".into(),
            }],
        };
        assert!(loaded.remember_option_labels(std::iter::once(&model)));
        assert!(!loaded.remember_option_labels(std::iter::once(&model)));
        loaded.remember_labels(std::iter::once(("claude-opus-5", "Opus 5")));
        assert_eq!(
            loaded.options_summary(HarnessId::ClaudeCode, "claude-opus-5"),
            Some("Context Window: 1M".into())
        );

        // The readout skips models whose picks are all defaults (stored as an
        // empty map by a deliberate reset) and names models by label.
        assert_eq!(
            loaded.remembered_option_rows(),
            vec![(
                HarnessId::ClaudeCode,
                "Opus 5".to_string(),
                "Context Window: 1M".to_string()
            )]
        );
    }

    #[test]
    fn remember_model_updates_harness_and_row() {
        let mut defaults = ComposerDefaults::default();
        defaults.remember_model(HarnessId::Codex, "m1".into(), "One".into());
        defaults.remember_model(HarnessId::Codex, "m2".into(), "Two".into());
        assert_eq!(defaults.harness, Some(HarnessId::Codex));
        assert_eq!(
            defaults.model_for(HarnessId::Codex).map(|m| &*m.id),
            Some("m2")
        );
        assert!(defaults.model_for(HarnessId::ClaudeCode).is_none());
    }
}
