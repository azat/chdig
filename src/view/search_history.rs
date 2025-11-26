use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct SearchHistory {
    history: Arc<Mutex<VecDeque<String>>>,
    current_index: Arc<Mutex<Option<usize>>>,
    temp_content: Arc<Mutex<String>>,
}

impl SearchHistory {
    pub fn new() -> Self {
        SearchHistory {
            history: Arc::new(Mutex::new(VecDeque::new())),
            current_index: Arc::new(Mutex::new(None)),
            temp_content: Arc::new(Mutex::new(String::new())),
        }
    }

    pub fn add_entry(&self, entry: String) {
        if entry.is_empty() {
            return;
        }
        let mut history = self.history.lock().unwrap();

        // Remove duplicate if it exists
        if let Some(pos) = history.iter().position(|x| x == &entry) {
            history.remove(pos);
        }

        // Add to front
        history.push_front(entry);
    }

    pub fn reset_index(&self) {
        *self.current_index.lock().unwrap() = None;
    }

    pub fn navigate_up(&self, current_content: &str) -> Option<String> {
        let history = self.history.lock().unwrap();
        if history.is_empty() {
            return None;
        }

        let mut index = self.current_index.lock().unwrap();
        let mut temp = self.temp_content.lock().unwrap();

        match *index {
            None => {
                // First time pressing up - save current content and go to most recent
                *temp = current_content.to_string();
                *index = Some(0);
                Some(history[0].clone())
            }
            Some(i) => {
                // Move to older entry
                if i + 1 < history.len() {
                    *index = Some(i + 1);
                    Some(history[i + 1].clone())
                } else {
                    None
                }
            }
        }
    }

    pub fn navigate_down(&self) -> Option<String> {
        let history = self.history.lock().unwrap();
        let mut index = self.current_index.lock().unwrap();
        let temp = self.temp_content.lock().unwrap();

        match *index {
            None => None,
            Some(0) => {
                // Back to the temporary content
                *index = None;
                Some(temp.clone())
            }
            Some(i) => {
                // Move to newer entry
                *index = Some(i - 1);
                Some(history[i - 1].clone())
            }
        }
    }
}

impl Default for SearchHistory {
    fn default() -> Self {
        Self::new()
    }
}
