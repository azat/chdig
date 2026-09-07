pub const BAR_FILLED: char = '█';
pub const BAR_EMPTY: char = '░';

/// Filled cells of a `width` wide bar showing `value` out of `max` (none when
/// `max` is not positive).
pub fn bar_filled(value: f64, max: f64, width: usize) -> usize {
    if max <= 0.0 {
        return 0;
    }
    (((value / max) * width as f64).round() as usize).min(width)
}

pub fn render_bar(value: f64, max: f64, width: usize) -> String {
    let filled = bar_filled(value, max, width);
    std::iter::repeat_n(BAR_FILLED, filled)
        .chain(std::iter::repeat_n(BAR_EMPTY, width - filled))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_bar() {
        assert_eq!(render_bar(0.0, 0.0, 4), "░░░░");
        assert_eq!(render_bar(1.0, 4.0, 4), "█░░░");
        assert_eq!(render_bar(5.0, 4.0, 4), "████");
        assert_eq!(bar_filled(0.04, 1.0, 10), 0);
    }
}
