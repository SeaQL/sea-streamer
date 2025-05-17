use std::{collections::HashSet, future::Future, num::NonZeroU32};

use crate::{FileErr, SeekErr, format::Beacon};

pub trait BeaconReader {
    type Future<'a>: Future<Output = Result<Beacon, FileErr>>
    where
        Self: 'a;

    fn survey(&mut self, at: NonZeroU32) -> Self::Future<'_>;

    /// Returns the max N-th Beacon
    fn max_beacons(&self) -> u32;
}

/// The goal of Surveyor is to find the two closest Beacons that pince our search target.
/// If would be pretty simple, if not for the fact that a given location may not contain
/// a relevant Beacon, which could yield Undecided.
pub struct Surveyor<'a, B, F>
where
    B: BeaconReader,
    F: Fn(&Beacon) -> SurveyResult,
{
    reader: &'a mut B,
    visitor: Visitor,
    left: u32,
    right: u32,
    func: F,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SurveyResult {
    Left,
    Right,
    Undecided,
}

use SurveyResult::{Left, Right, Undecided};

#[derive(Clone)]
pub struct MockBeacon {
    // the 0th item is ignored
    beacons: Vec<Option<Beacon>>,
}

/// The goal of Visitor is to make sure that we have either visited, or by induction,
/// eliminated all potential Beacon.
struct Visitor {
    min: u32, // we visited all N <= min already
    max: u32, // we visited all N >= max already
    visited: HashSet<u32>,
}

impl<'a, B, F> Surveyor<'a, B, F>
where
    B: BeaconReader,
    F: Fn(&Beacon) -> SurveyResult,
{
    pub async fn new(reader: &'a mut B, func: F) -> Result<Surveyor<'a, B, F>, FileErr> {
        let mut min = 0;
        let mut max = reader.max_beacons();
        if max > 0 {
            let left = reader.survey(NonZeroU32::new(1).unwrap()).await?;
            if func(&left) == Right {
                // it is still possible that the target is between the file header and the 1st beacon
                max = 1;
            }
            let right = reader.survey(NonZeroU32::new(max).unwrap()).await?;
            if func(&right) == Left {
                // it is still possible that the target is between the last beacon and remaining bytes
                min = max;
            }
        }
        Ok(Self {
            reader,
            visitor: Visitor::new(max),
            left: min,
            right: u32::MAX,
            func,
        })
    }

    pub async fn run(mut self) -> Result<(u32, u32), FileErr> {
        let max_steps = self.reader.max_beacons();
        let mut i = 0;
        while i < max_steps {
            i += 1;
            if !self.step().await? {
                break;
            } else if i == max_steps {
                return Err(FileErr::SeekErr(SeekErr::Exhausted));
            }
        }
        Ok(self.result())
    }

    /// If false, it means that search has ended
    pub async fn step(&mut self) -> Result<bool, FileErr> {
        if let Some(at) = self.visitor.next_unvisited() {
            let beacon = self.reader.survey(NonZeroU32::new(at).unwrap()).await?;
            match (self.func)(&beacon) {
                Undecided => self.visitor.visit(at),
                Left => {
                    self.left = self.left.max(at);
                    self.visitor.visit_upto(at);
                }
                Right => {
                    self.right = self.right.min(at);
                    self.visitor.visit_beyond(at);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get the search result
    pub fn result(&self) -> (u32, u32) {
        (self.left, self.right)
    }
}

impl Visitor {
    pub fn new(max: u32) -> Self {
        Self {
            min: 0,
            max,
            visited: Default::default(),
        }
    }

    pub fn next_unvisited(&self) -> Option<u32> {
        if self.remaining() > 0 {
            loop {
                // somewhat like a randomized binary search
                let n = fastrand::u32(self.min + 1..self.max);
                if !self.visited.contains(&n) {
                    return Some(n);
                }
            }
        } else {
            None
        }
    }

    pub fn remaining(&self) -> usize {
        self.max as usize - self.min as usize - 1 - self.visited.len()
    }

    pub fn visit(&mut self, n: u32) {
        if n <= self.min || n >= self.max {
        } else {
            self.visited.insert(n);
        }
    }

    /// Eliminate all locations up to (and including) N
    pub fn visit_upto(&mut self, n: u32) {
        self.min = n;
        self.visited.retain(|x| x > &n);
    }

    /// Eliminate all locations (including) N and beyond
    pub fn visit_beyond(&mut self, n: u32) {
        self.max = n;
        self.visited.retain(|x| x < &n);
    }

    #[cfg(test)]
    fn used_size(&self) -> usize {
        self.visited.len()
    }
}

impl MockBeacon {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);
        Self {
            beacons: vec![None; size + 1],
        }
    }

    pub fn add(&mut self, n: u32, b: Beacon) {
        self.beacons[n as usize] = Some(b);
    }
}

impl BeaconReader for MockBeacon {
    type Future<'a> = std::future::Ready<Result<Beacon, FileErr>>;

    fn survey(&mut self, at: NonZeroU32) -> Self::Future<'_> {
        std::future::ready(Ok(self.beacons[at.get() as usize]
            .clone()
            .unwrap_or_else(Beacon::empty)))
    }

    fn max_beacons(&self) -> u32 {
        (self.beacons.len() - 1) as u32
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_visitor() {
        let mut visitor = Visitor::new(10);
        visitor.visit(4);
        visitor.visit_upto(5);
        visitor.visit(6);
        visitor.visit(7);
        visitor.visit_beyond(9);
        assert_eq!(visitor.used_size(), 2);
        assert_eq!(visitor.next_unvisited(), Some(8));

        let mut visitor = Visitor::new(4);
        visitor.visit(1);
        visitor.visit(3);
        assert_eq!(visitor.used_size(), 2);
        assert_eq!(visitor.next_unvisited(), Some(2));

        let mut visitor = Visitor::new(10);
        for _ in 0..9 {
            visitor.visit(visitor.next_unvisited().unwrap());
        }
        assert_eq!(visitor.remaining(), 0);
    }
}
