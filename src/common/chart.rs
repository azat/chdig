const BLOCKS: &[char] = &[' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

fn format_value(value: f64) -> String {
    if value >= 100. {
        format!("{:.0}", value)
    } else if value >= 1. {
        format!("{:.2}", value)
    } else {
        format!("{:.4}", value)
    }
}

/// Renders values as a column chart of `height` rows (one column per value,
/// scaled from 0 to the maximum), with the maximum as a y-axis label.
pub fn render_column_chart(values: &[f64], height: usize) -> String {
    let max = values.iter().copied().fold(0.0_f64, f64::max);

    let eighths: Vec<usize> = values
        .iter()
        .map(|v| {
            if max > 0. {
                (v.max(0.) / max * (height * 8) as f64).round() as usize
            } else {
                0
            }
        })
        .collect();

    let max_label = format_value(max);
    let label_width = max_label.len();

    let mut lines = Vec::with_capacity(height);
    for row in 0..height {
        let row_base = (height - 1 - row) * 8;
        let label = match row {
            0 => max_label.as_str(),
            _ if row == height - 1 => "0",
            _ => "",
        };
        let columns: String = eighths
            .iter()
            .map(|&e| BLOCKS[e.saturating_sub(row_base).min(8)])
            .collect();
        lines.push(format!("{:>label_width$} ┤{}", label, columns));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_column_chart() {
        let chart = render_column_chart(&[0., 1., 2., 4.], 2);
        let lines: Vec<&str> = chart.split('\n').collect();
        assert_eq!(lines.len(), 2);
        // 4 is the maximum (full height), 2 is half, 1 is a quarter
        assert_eq!(lines[0], "4.00 ┤   █");
        assert_eq!(lines[1], "   0 ┤ ▄██");
    }

    #[test]
    fn test_render_column_chart_empty() {
        let chart = render_column_chart(&[0., 0.], 2);
        let lines: Vec<&str> = chart.split('\n').collect();
        assert_eq!(lines[0], "0.0000 ┤  ");
        assert_eq!(lines[1], "     0 ┤  ");
    }
}
