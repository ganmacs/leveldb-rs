use super::FileMetaData;

const LEVEL: usize = 12;

pub struct Compaction {
    level: usize,
    pub inputs: Vec<Vec<FileMetaData>>,
}

impl Compaction {
    pub fn new(level: usize) -> Self {
        Self {
            level: level,
            inputs: vec![Vec::new(); LEVEL],
        }
    }
}

pub struct TwoLevelIterator<F, S> {
    first: F,
    second: Option<S>,
}

impl<F, S> TwoLevelIterator<F, S>
where
    F: Iterator<Item = S>,
    S: Iterator,
{
    pub fn new(iter: F) -> Self {
        TwoLevelIterator {
            first: iter,
            second: None,
        }
    }
}

impl<F, S> Iterator for TwoLevelIterator<F, S>
where
    F: Iterator<Item = S>,
    S: Iterator,
{
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.second.as_mut().and_then(|s| s.next()).or_else(|| {
            self.first.next().and_then(|f| {
                self.second = Some(f);
                self.second.as_mut().and_then(|s| s.next())
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_two_level_iterator() {
        let expecteds = (1..10).into_iter();
        let v = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]
            .into_iter()
            .map(|v| v.into_iter());
        let actuals = TwoLevelIterator::new(v.into_iter());

        for (e, a) in expecteds.zip(actuals) {
            assert_eq!(e, a);
        }
    }
}
