use anyhow::{Result, anyhow};
use ratatui::layout::{Position, Rect, Size};

use super::component::{Boxed, Canvas, Component};
use super::event::{Event, EventResult, Key, MouseButton, MouseEvent};
use super::style::{Style, print_str};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Id(u64);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Orientation {
    Horizontal,
    Vertical,
}

enum Node {
    Leaf {
        id: Id,
        view: Boxed,
    },
    Split {
        orientation: Orientation,
        /// Fraction of the area given to the first child.
        ratio: f32,
        first: Box<Node>,
        second: Box<Node>,
    },
}

impl Node {
    fn leaves(&self, out: &mut Vec<Id>) {
        match self {
            Node::Leaf { id, .. } => out.push(*id),
            Node::Split { first, second, .. } => {
                first.leaves(out);
                second.leaves(out);
            }
        }
    }

    fn contains(&self, id: Id) -> bool {
        match self {
            Node::Leaf { id: leaf, .. } => *leaf == id,
            Node::Split { first, second, .. } => first.contains(id) || second.contains(id),
        }
    }

    fn find_leaf_mut(&mut self, id: Id) -> Option<&mut Boxed> {
        match self {
            Node::Leaf { id: leaf, view } => (*leaf == id).then_some(view),
            Node::Split { first, second, .. } => {
                first.find_leaf_mut(id).or_else(|| second.find_leaf_mut(id))
            }
        }
    }

    fn first_leaf(&self) -> Id {
        match self {
            Node::Leaf { id, .. } => *id,
            Node::Split { first, .. } => first.first_leaf(),
        }
    }

    fn for_each_view(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        match self {
            Node::Leaf { view, .. } => f(view),
            Node::Split { first, second, .. } => {
                first.for_each_view(f);
                second.for_each_view(f);
            }
        }
    }
}

/// tmux-like pane multiplexer: a binary tree of splits with draggable
/// separators, directional focus movement, per-split resize and zoom.
pub struct Mux {
    root: Option<Node>,
    focus: Id,
    next_id: u64,
    zoomed: bool,
    /// (id, rect) of every leaf from the last draw (mouse routing,
    /// directional focus).
    pane_rects: Vec<(Id, Rect)>,
    /// Separator cells from the last draw with the split path they belong to.
    separators: Vec<(Vec<bool>, Orientation, Rect)>,
    /// Split path being resized by a mouse drag.
    resize_drag: Option<(Vec<bool>, Orientation)>,
    last_area: Rect,
}

impl Default for Mux {
    fn default() -> Self {
        Self::new()
    }
}

impl Mux {
    pub fn new() -> Self {
        Self {
            root: None,
            focus: Id(0),
            next_id: 1,
            zoomed: false,
            pane_rects: Vec::new(),
            separators: Vec::new(),
            resize_drag: None,
            last_area: Rect::default(),
        }
    }

    pub fn focus(&self) -> Id {
        self.focus
    }

    pub fn set_focus(&mut self, id: Id) {
        if self.root.as_ref().is_some_and(|r| r.contains(id)) {
            self.focus = id;
        }
    }

    pub fn panes(&self) -> Vec<Id> {
        let mut out = Vec::new();
        if let Some(root) = &self.root {
            root.leaves(&mut out);
        }
        out
    }

    pub fn is_zoomed(&self) -> bool {
        self.zoomed
    }

    pub fn active_view(&self) -> Option<&dyn Component> {
        // Symmetry with active_view_mut; immutable tree search.
        fn find(node: &Node, id: Id) -> Option<&Boxed> {
            match node {
                Node::Leaf { id: leaf, view } => (*leaf == id).then_some(view),
                Node::Split { first, second, .. } => find(first, id).or_else(|| find(second, id)),
            }
        }
        self.root
            .as_ref()
            .and_then(|r| find(r, self.focus))
            .map(|b| b.0.as_ref())
    }

    pub fn active_view_mut(&mut self) -> Option<&mut dyn Component> {
        let focus = self.focus;
        self.root
            .as_mut()
            .and_then(|r| r.find_leaf_mut(focus))
            .map(|b| b.0.as_mut())
    }

    fn add_split<V: Component + 'static>(
        &mut self,
        view: V,
        target: Id,
        orientation: Orientation,
    ) -> Result<Id> {
        let id = Id(self.next_id);
        self.next_id += 1;
        let new_leaf = Node::Leaf {
            id,
            view: Boxed::new(view),
        };

        match &mut self.root {
            None => {
                self.root = Some(new_leaf);
                self.focus = id;
                self.zoomed = false;
                return Ok(id);
            }
            Some(root) => {
                if !root.contains(target) {
                    return Err(anyhow!("pane {:?} not found", target));
                }
                fn split_at(node: &mut Node, target: Id, new_leaf: Node, orientation: Orientation) {
                    match node {
                        Node::Leaf { id, .. } if *id == target => {
                            let old = std::mem::replace(
                                node,
                                Node::Split {
                                    orientation,
                                    ratio: 0.5,
                                    first: Box::new(Node::Leaf {
                                        id: Id(0),
                                        view: Boxed::new(super::component::DummyView),
                                    }),
                                    second: Box::new(new_leaf),
                                },
                            );
                            if let Node::Split { first, .. } = node {
                                **first = old;
                            }
                        }
                        Node::Leaf { .. } => {}
                        Node::Split { first, second, .. } => {
                            if first.contains(target) {
                                split_at(first, target, new_leaf, orientation);
                            } else if second.contains(target) {
                                split_at(second, target, new_leaf, orientation);
                            }
                        }
                    }
                }
                split_at(root, target, new_leaf, orientation);
            }
        }
        self.focus = id;
        self.zoomed = false;
        Ok(id)
    }

    pub fn add_right_of<V: Component + 'static>(&mut self, view: V, target: Id) -> Result<Id> {
        self.add_split(view, target, Orientation::Horizontal)
    }

    pub fn add_below<V: Component + 'static>(&mut self, view: V, target: Id) -> Result<Id> {
        self.add_split(view, target, Orientation::Vertical)
    }

    /// Remove a pane. The lone pane cannot be removed.
    pub fn remove_id(&mut self, id: Id) -> Result<()> {
        let Some(root) = &mut self.root else {
            return Err(anyhow!("empty mux"));
        };
        if let Node::Leaf { .. } = root {
            return Err(anyhow!("cannot remove the last pane"));
        }
        if !root.contains(id) {
            return Err(anyhow!("pane {:?} not found", id));
        }

        fn remove(node: &mut Node, id: Id) -> bool {
            if let Node::Split { first, second, .. } = node {
                let replace_with_sibling = |node: &mut Node, keep_first: bool| {
                    let Node::Split { first, second, .. } = std::mem::replace(
                        node,
                        Node::Leaf {
                            id: Id(0),
                            view: Boxed::new(super::component::DummyView),
                        },
                    ) else {
                        unreachable!();
                    };
                    *node = if keep_first { *first } else { *second };
                };
                if matches!(first.as_ref(), Node::Leaf { id: leaf, .. } if *leaf == id) {
                    replace_with_sibling(node, false);
                    return true;
                }
                if matches!(second.as_ref(), Node::Leaf { id: leaf, .. } if *leaf == id) {
                    replace_with_sibling(node, true);
                    return true;
                }
                return remove(first, id) || remove(second, id);
            }
            false
        }
        remove(root, id);
        self.zoomed = false;
        if self.focus == id || !root.contains(self.focus) {
            self.focus = root.first_leaf();
        }
        Ok(())
    }

    fn layout_rects(
        node: &Node,
        area: Rect,
        path: &mut Vec<bool>,
        panes: &mut Vec<(Id, Rect)>,
        separators: &mut Vec<(Vec<bool>, Orientation, Rect)>,
    ) {
        match node {
            Node::Leaf { id, .. } => panes.push((*id, area)),
            Node::Split {
                orientation,
                ratio,
                first,
                second,
            } => {
                let (first_rect, sep_rect, second_rect) = match orientation {
                    Orientation::Horizontal => {
                        let first_w =
                            ((area.width.saturating_sub(1)) as f32 * ratio).round() as u16;
                        let second_w = area.width.saturating_sub(1).saturating_sub(first_w);
                        (
                            Rect::new(area.x, area.y, first_w, area.height),
                            Rect::new(area.x + first_w, area.y, 1, area.height),
                            Rect::new(area.x + first_w + 1, area.y, second_w, area.height),
                        )
                    }
                    Orientation::Vertical => {
                        let first_h =
                            ((area.height.saturating_sub(1)) as f32 * ratio).round() as u16;
                        let second_h = area.height.saturating_sub(1).saturating_sub(first_h);
                        (
                            Rect::new(area.x, area.y, area.width, first_h),
                            Rect::new(area.x, area.y + first_h, area.width, 1),
                            Rect::new(area.x, area.y + first_h + 1, area.width, second_h),
                        )
                    }
                };
                separators.push((path.clone(), *orientation, sep_rect));
                path.push(false);
                Self::layout_rects(first, first_rect, path, panes, separators);
                path.pop();
                path.push(true);
                Self::layout_rects(second, second_rect, path, panes, separators);
                path.pop();
            }
        }
    }

    fn compute_layout(&mut self, area: Rect) {
        self.pane_rects.clear();
        self.separators.clear();
        if self.zoomed {
            self.pane_rects.push((self.focus, area));
            return;
        }
        if let Some(root) = &self.root {
            let mut path = Vec::new();
            Self::layout_rects(
                root,
                area,
                &mut path,
                &mut self.pane_rects,
                &mut self.separators,
            );
        }
    }

    fn split_by_path_mut(&mut self, path: &[bool]) -> Option<(&mut Orientation, &mut f32)> {
        let mut node = self.root.as_mut()?;
        for &second in path {
            match node {
                Node::Split {
                    first, second: s, ..
                } => {
                    node = if second { s } else { first };
                }
                Node::Leaf { .. } => return None,
            }
        }
        match node {
            Node::Split {
                orientation, ratio, ..
            } => Some((orientation, ratio)),
            Node::Leaf { .. } => None,
        }
    }

    fn pane_rect(&self, id: Id) -> Option<Rect> {
        self.pane_rects
            .iter()
            .find(|(pid, _)| *pid == id)
            .map(|(_, r)| *r)
    }

    /// Move focus to the nearest pane in the given direction.
    fn focus_direction(&mut self, dx: i32, dy: i32) -> EventResult {
        let Some(current) = self.pane_rect(self.focus) else {
            return EventResult::Ignored;
        };
        let center = |r: Rect| {
            (
                r.x as i32 + r.width as i32 / 2,
                r.y as i32 + r.height as i32 / 2,
            )
        };
        let (cx, cy) = center(current);
        let mut best: Option<(i32, Id)> = None;
        for (id, rect) in &self.pane_rects {
            if *id == self.focus {
                continue;
            }
            let (px, py) = center(*rect);
            let (vx, vy) = (px - cx, py - cy);
            // Must lie in the requested direction.
            if (dx != 0 && vx * dx <= 0) || (dy != 0 && vy * dy <= 0) {
                continue;
            }
            let distance = vx.abs() + vy.abs();
            if best.is_none_or(|(d, _)| distance < d) {
                best = Some((distance, *id));
            }
        }
        match best {
            Some((_, id)) => {
                self.focus = id;
                self.zoomed = false;
                EventResult::consumed()
            }
            None => EventResult::Ignored,
        }
    }

    /// Resize the focused pane by moving the separator of the nearest
    /// enclosing split with a matching orientation.
    fn resize(&mut self, orientation: Orientation, grow_first: bool) -> EventResult {
        // Deepest matching split containing the focus.
        let mut best: Option<Vec<bool>> = None;
        for (path, sep_orientation, _) in &self.separators {
            if *sep_orientation != orientation {
                continue;
            }
            if self.split_contains_focus(path) && best.as_ref().is_none_or(|b| path.len() > b.len())
            {
                best = Some(path.clone());
            }
        }
        let (Some(path), Some(area)) = (best, Some(self.last_area)) else {
            return EventResult::Ignored;
        };
        let total = match orientation {
            Orientation::Horizontal => area.width,
            Orientation::Vertical => area.height,
        }
        .max(2) as f32;
        if let Some((_, ratio)) = self.split_by_path_mut(&path) {
            let step = 1.0 / total;
            *ratio = (*ratio + if grow_first { step } else { -step }).clamp(0.05, 0.95);
            return EventResult::consumed();
        }
        EventResult::Ignored
    }

    fn split_contains_focus(&self, path: &[bool]) -> bool {
        let mut node = match &self.root {
            Some(root) => root,
            None => return false,
        };
        for &second in path {
            match node {
                Node::Split {
                    first, second: s, ..
                } => {
                    node = if second { s.as_ref() } else { first.as_ref() };
                }
                Node::Leaf { .. } => return false,
            }
        }
        node.contains(self.focus)
    }

    fn drag_separator(&mut self, path: &[bool], orientation: Orientation, position: Position) {
        // The separator position is relative to the split's own area; find it
        // by walking the layout again.
        let Some(root) = &self.root else { return };
        fn split_area(node: &Node, area: Rect, path: &[bool]) -> Option<Rect> {
            if path.is_empty() {
                return Some(area);
            }
            match node {
                Node::Leaf { .. } => None,
                Node::Split {
                    orientation,
                    ratio,
                    first,
                    second,
                } => {
                    let (first_rect, second_rect) = match orientation {
                        Orientation::Horizontal => {
                            let first_w =
                                ((area.width.saturating_sub(1)) as f32 * ratio).round() as u16;
                            (
                                Rect::new(area.x, area.y, first_w, area.height),
                                Rect::new(
                                    area.x + first_w + 1,
                                    area.y,
                                    area.width.saturating_sub(1).saturating_sub(first_w),
                                    area.height,
                                ),
                            )
                        }
                        Orientation::Vertical => {
                            let first_h =
                                ((area.height.saturating_sub(1)) as f32 * ratio).round() as u16;
                            (
                                Rect::new(area.x, area.y, area.width, first_h),
                                Rect::new(
                                    area.x,
                                    area.y + first_h + 1,
                                    area.width,
                                    area.height.saturating_sub(1).saturating_sub(first_h),
                                ),
                            )
                        }
                    };
                    if path[0] {
                        split_area(second, second_rect, &path[1..])
                    } else {
                        split_area(first, first_rect, &path[1..])
                    }
                }
            }
        }
        let Some(area) = split_area(root, self.last_area, path) else {
            return;
        };
        let ratio = match orientation {
            Orientation::Horizontal => {
                (position.x.saturating_sub(area.x)) as f32 / (area.width.max(2) - 1) as f32
            }
            Orientation::Vertical => {
                (position.y.saturating_sub(area.y)) as f32 / (area.height.max(2) - 1) as f32
            }
        };
        if let Some((_, r)) = self.split_by_path_mut(path) {
            *r = ratio.clamp(0.05, 0.95);
        }
    }
}

impl Component for Mux {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.last_area = area;
        self.compute_layout(area);

        for (path, orientation, rect) in &self.separators {
            let _ = path;
            match orientation {
                Orientation::Horizontal => {
                    for y in rect.top()..rect.bottom() {
                        print_str(canvas.buf, rect.x, y, *rect, "│", Style::default());
                    }
                }
                Orientation::Vertical => {
                    for x in rect.left()..rect.right() {
                        print_str(canvas.buf, x, rect.y, *rect, "─", Style::default());
                    }
                }
            }
        }

        let focus = self.focus;
        let zoomed = self.zoomed;
        let rects: Vec<(Id, Rect)> = self.pane_rects.clone();
        if let Some(root) = &mut self.root {
            for (id, rect) in rects {
                if zoomed && id != focus {
                    continue;
                }
                if rect.width == 0 || rect.height == 0 {
                    continue;
                }
                if let Some(view) = root.find_leaf_mut(id) {
                    view.draw(canvas, rect, focused && id == focus);
                }
            }
        }
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        if let Event::Mouse {
            position,
            event: mouse,
        } = event
        {
            match mouse {
                MouseEvent::Press(MouseButton::Left) => {
                    if let Some((path, orientation, _)) = self
                        .separators
                        .iter()
                        .find(|(_, _, rect)| rect.contains(*position))
                        .cloned()
                    {
                        self.resize_drag = Some((path, orientation));
                        return EventResult::consumed();
                    }
                    if let Some((id, _)) = self
                        .pane_rects
                        .iter()
                        .find(|(_, rect)| rect.contains(*position))
                        .copied()
                    {
                        let refocused = id != self.focus;
                        self.focus = id;
                        if let Some(view) = self.active_view_mut() {
                            let result = view.on_event(event);
                            if refocused || result.is_consumed() {
                                return result.and(EventResult::consumed());
                            }
                            return result;
                        }
                    }
                    return EventResult::Ignored;
                }
                MouseEvent::Hold(MouseButton::Left) => {
                    if let Some((path, orientation)) = self.resize_drag.clone() {
                        self.drag_separator(&path, orientation, *position);
                        return EventResult::consumed();
                    }
                }
                MouseEvent::Release(MouseButton::Left) => {
                    if self.resize_drag.take().is_some() {
                        return EventResult::consumed();
                    }
                }
                _ => {}
            }
            // Wheel and other mouse events go to the pane under the cursor.
            if let Some((id, _)) = self
                .pane_rects
                .iter()
                .find(|(_, rect)| rect.contains(*position))
                .copied()
                && let Some(root) = &mut self.root
                && let Some(view) = root.find_leaf_mut(id)
            {
                return view.on_event(event);
            }
            return EventResult::Ignored;
        }

        if let Some(view) = self.active_view_mut() {
            let result = view.on_event(event);
            if result.is_consumed() {
                return result;
            }
        }

        match event {
            Event::Alt(Key::Left) => self.focus_direction(-1, 0),
            Event::Alt(Key::Right) => self.focus_direction(1, 0),
            Event::Alt(Key::Up) => self.focus_direction(0, -1),
            Event::Alt(Key::Down) => self.focus_direction(0, 1),
            Event::Ctrl(Key::Left) => self.resize(Orientation::Horizontal, false),
            Event::Ctrl(Key::Right) => self.resize(Orientation::Horizontal, true),
            Event::Ctrl(Key::Up) => self.resize(Orientation::Vertical, false),
            Event::Ctrl(Key::Down) => self.resize(Orientation::Vertical, true),
            Event::CtrlChar('x') => {
                if self.pane_rects.len() > 1 || self.zoomed {
                    self.zoomed = !self.zoomed;
                    EventResult::consumed()
                } else {
                    EventResult::Ignored
                }
            }
            _ => EventResult::Ignored,
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        max
    }

    fn take_focus(&mut self) -> bool {
        true
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        if let Some(root) = &mut self.root {
            root.for_each_view(&mut |view| f(view));
        }
    }

    fn focus_name(&mut self, name: &str) -> bool {
        let mut found = None;
        if let Some(root) = &mut self.root {
            fn search(node: &mut Node, name: &str, found: &mut Option<Id>) {
                match node {
                    Node::Leaf { id, view } => {
                        if found.is_none() && view.focus_name(name) {
                            *found = Some(*id);
                        }
                    }
                    Node::Split { first, second, .. } => {
                        search(first, name, found);
                        search(second, name, found);
                    }
                }
            }
            search(root, name, &mut found);
        }
        match found {
            Some(id) => {
                self.focus = id;
                true
            }
            None => false,
        }
    }
}
