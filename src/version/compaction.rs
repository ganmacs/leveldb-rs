use std::cmp;

use super::FileMetaData;

const LEVEL: usize = 12;

pub struct Compaction {
    pub level: usize,
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

pub struct MergeingIterator<I: Iterator> {
    iters: Vec<I>,
    nexts: Vec<Option<I::Item>>,
    idx: usize,
    first: bool,
}

impl<I> MergeingIterator<I>
where
    I: Iterator,
    I::Item: Clone + cmp::Ord,
{
    pub fn new(iters: Vec<I>) -> Self {
        let l = iters.len();
        Self {
            iters: iters,
            nexts: vec![None; l],
            idx: 0,
            first: true,
        }
    }
}

impl<I> Iterator for MergeingIterator<I>
where
    I: Iterator,
    I::Item: Clone + cmp::Ord,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iters.len() == 0 {
            return None;
        }

        if self.first {
            for (i, it) in self.iters.iter_mut().enumerate() {
                self.nexts[i] = it.next()
            }
        } else {
            let it = self.iters.get_mut(self.idx).expect("Must be existed");
            self.nexts[self.idx] = it.next();
        }

        for (i, n) in self.nexts.iter().enumerate() {
            if n.is_none() {
                continue;
            }

            if self.first {
                self.idx = i;
                self.first = false;
                continue;
            }

            if self.nexts[self.idx].is_none() || &self.nexts[self.idx] > n {
                self.idx = i;
            }
        }

        self.nexts[self.idx].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_two_level_iterator() {
        let expecteds = vec![4, 5, 1, 2, 3, 7, 8, 9].into_iter();
        let v = vec![vec![4, 5], vec![1, 2, 3], vec![7, 8, 9], vec![]]
            .into_iter()
            .map(|v| v.into_iter());
        let mut actuals = TwoLevelIterator::new(v.into_iter());

        for e in expecteds {
            assert_eq!(Some(e), actuals.next());
        }
    }

    #[test]
    fn mergeing_iterator() {
        let expecteds = vec![1, 2, 4, 5, 6, 7, 8, 10, 20, 30, 40, 55, 100].into_iter();
        let v = vec![
            vec![1, 5, 10, 20, 30, 40],
            vec![2, 4, 6, 8],
            vec![7, 55, 100],
            vec![],
        ].into_iter()
            .map(|v| v.into_iter())
            .collect();
        let mut actuals = MergeingIterator::new(v);

        for e in expecteds {
            assert_eq!(Some(e), actuals.next());
        }
    }

    #[test]
    fn empty_mergeing_iterator() {
        let v: Vec<Vec<u8>> = vec![vec![]];
        let v = v.into_iter().map(|v| v.into_iter()).collect();
        let mut actuals = MergeingIterator::new(v);
        assert_eq!(actuals.next(), None)
    }

    #[test]
    fn empty_outer_mergeing_iterator() {
        let v: Vec<Vec<u8>> = vec![];
        let v = v.into_iter().map(|v| v.into_iter()).collect();
        let mut actuals = MergeingIterator::new(v);
        assert_eq!(actuals.next(), None)
    }
}
