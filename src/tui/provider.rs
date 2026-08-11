use std::sync::Arc;

use super::app::App;
use crate::interpreter::{ContextArc, options::ChDigViews};

/// Provider of a view type: knows how to construct and present it.
pub trait ViewProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Name of the main widget the provider shows (`with_name`), or None when
    /// there is no named widget (client). Used to map a widget back to its
    /// view type (per-view config settings, layout focus). Defaults to the
    /// stable view name; MUST be overridden when the widget is named
    /// differently (the mapping silently breaks otherwise).
    fn view_name(&self) -> Option<&'static str> {
        Some(self.view_type().config_name())
    }

    fn view_type(&self) -> ChDigViews;

    /// Shows the view in the focused pane. `instance` is the name of a named
    /// view instance (`views:` config section) to show instead of the default
    /// singleton: the widget is named after it, so several instances of one
    /// view type can coexist. None everywhere but the layout instantiation.
    fn show(&self, app: &mut App, context: ContextArc, instance: Option<&str>);
}

pub struct ViewRegistry {
    providers: Vec<(&'static str, Arc<dyn ViewProvider>)>,
}

impl ViewRegistry {
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    pub fn register(&mut self, provider: Arc<dyn ViewProvider>) {
        let name = provider.name();
        self.providers.push((name, provider));
    }

    pub fn get(&self, name: &str) -> Arc<dyn ViewProvider> {
        self.providers
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, p)| p.clone())
            .unwrap()
    }

    pub fn get_by_view_type(&self, view_type: ChDigViews) -> Arc<dyn ViewProvider> {
        self.providers
            .iter()
            .find(|(_, p)| p.view_type() == view_type)
            .map(|(_, p)| p.clone())
            .unwrap()
    }

    pub fn view_type_by_view_name(&self, view_name: &str) -> Option<ChDigViews> {
        self.providers
            .iter()
            .find(|(_, p)| p.view_name() == Some(view_name))
            .map(|(_, p)| p.view_type())
    }
}

impl Default for ViewRegistry {
    fn default() -> Self {
        Self::new()
    }
}
