use std::sync::Arc;

use super::app::App;
use crate::interpreter::{ContextArc, options::ChDigViews};

/// Provider of a view type: knows how to construct and present it.
pub trait ViewProvider: Send + Sync {
    fn name(&self) -> &'static str;

    fn view_type(&self) -> ChDigViews;

    fn show(&self, app: &mut App, context: ContextArc);
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
}

impl Default for ViewRegistry {
    fn default() -> Self {
        Self::new()
    }
}
