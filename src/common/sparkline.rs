use std::collections::VecDeque;

const BLOCKS: &[char] = &['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

pub struct SparklineBuffer {
    data: VecDeque<f64>,
    capacity: usize,
}

impl SparklineBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, value: f64) {
        if self.data.len() == self.capacity {
            self.data.pop_front();
        }
        self.data.push_back(value);
    }

    pub fn render(&self, width: usize) -> String {
        if self.data.is_empty() {
            return String::new();
        }

        let samples: Vec<f64> = self
            .data
            .iter()
            .rev()
            .take(width)
            .copied()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        let min = samples.iter().copied().fold(f64::INFINITY, f64::min);
        let max = samples.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let range = max - min;

        samples
            .iter()
            .map(|&v| {
                if range == 0.0 {
                    BLOCKS[BLOCKS.len() / 2]
                } else {
                    let idx = ((v - min) / range * (BLOCKS.len() - 1) as f64).round() as usize;
                    BLOCKS[idx.min(BLOCKS.len() - 1)]
                }
            })
            .collect()
    }
}
