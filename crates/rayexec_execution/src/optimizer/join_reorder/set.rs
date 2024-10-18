//! Set utilies on a slice.

use std::collections::HashSet;
use std::hash::Hash;

/// Produce the powerset of a slice.
pub fn powerset<T>(s: &[T]) -> Vec<Vec<T>>
where
    T: Copy,
{
    (0..2usize.pow(s.len() as u32))
        .map(|i| {
            s.iter()
                .enumerate()
                .filter(|&(t, _)| (i >> t) % 2 == 1)
                .map(|(_, &element)| element)
                .collect()
        })
        .collect()
}

/// Produce all possible non-overlapping binary partitions of a slice.
///
/// This will treat '([1], [2,3])' and '([2,3], [1])' as the same, and unioning
/// them would produce the same set.
pub fn binary_partitions<T>(s: &[T]) -> HashSet<(Vec<T>, Vec<T>)>
where
    T: Copy + Ord + Hash,
{
    let mut result = HashSet::new();
    let n = s.len();

    // Iterate over all 2^(n-1) non-trivial partitions (skip empty and full
    // partition cases)
    for i in 1..(1 << n) - 1 {
        let mut set1 = Vec::new();
        let mut set2 = Vec::new();

        // For each bit, decide which set the element goes to
        for j in 0..n {
            if (i & (1 << j)) != 0 {
                set1.push(s[j]);
            } else {
                set2.push(s[j]);
            }
        }
        // Sort both sets to guarantee a canonical order
        set1.sort_unstable();
        set2.sort_unstable();

        // Ensure consistent ordering: smaller set comes first, or if equal
        // size, lexicographical order
        if set1 > set2 {
            std::mem::swap(&mut set1, &mut set2);
        }

        result.insert((set1, set2));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn powerset_simple() {
        let v = [1, 2, 3];
        let expected = vec![
            vec![],
            vec![1],
            vec![2],
            vec![1, 2],
            vec![3],
            vec![1, 3],
            vec![2, 3],
            vec![1, 2, 3],
        ];

        let got = powerset(&v);

        assert_eq!(expected, got);
    }

    #[test]
    fn binary_partitions_simple() {
        let v = [1, 2, 3];
        let expected: HashSet<_> = [
            (vec![1], vec![2, 3]),
            (vec![1, 3], vec![2]),
            (vec![1, 2], vec![3]),
        ]
        .into_iter()
        .collect();

        let got = binary_partitions(&v);

        assert_eq!(expected, got);
    }
}
