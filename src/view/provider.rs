use crate::interpreter::{ContextArc, options::ChDigViews};
use cursive::Cursive;

/// Trait for providing views in the application.
/// Each provider is responsible for showing a specific view type.
pub trait ViewProvider: Send + Sync {
    /// Returns the unique name of this view provider
    fn name(&self) -> &'static str;

    /// Returns the view type enum value for this provider
    fn view_type(&self) -> ChDigViews;

    /// Shows the view in the given Cursive instance
    fn show(&self, siv: &mut Cursive, context: ContextArc);
}
