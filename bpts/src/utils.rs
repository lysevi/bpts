pub unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts((p as *const T) as *const u8, ::core::mem::size_of::<T>())
}

pub fn insert_to_array<T>(target: &mut Vec<T>, pos: usize, value: T) {
    let last = target.len();
    for i in (pos + 1..last).rev() {
        target.swap(i, i - 1);
    }
    target[pos] = value;
}

pub fn remove_with_shift<T>(target: &mut Vec<T>, pos: usize) {
    let last = target.len();
    for i in pos..last - 1 {
        target.swap(i, i + 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert() {
        let mut v = vec![1, 2, 3, 4, 0];
        insert_to_array(&mut v, 0, 5);

        assert_eq!(v, [5, 1, 2, 3, 4]);

        insert_to_array(&mut v, 1, 5);
        assert_eq!(v, [5, 5, 1, 2, 3]);

        insert_to_array(&mut v, 4, 5);
        assert_eq!(v, [5, 5, 1, 2, 5]);
    }

    #[test]
    fn remove_from_array() {
        {
            let mut v = vec![1, 2, 3, 4, 5];
            remove_with_shift(&mut v, 0);

            assert_eq!(v, [2, 3, 4, 5, 1]);
        }
        {
            let mut v = vec![1, 2, 3, 4, 5];
            remove_with_shift(&mut v, 2);

            assert_eq!(v, [1, 2, 4, 5, 3]);
        }

        {
            let mut v = vec![1, 2, 3, 4, 5];
            remove_with_shift(&mut v, 4);

            assert_eq!(v, [1, 2, 3, 4, 5]);
        }
    }
}
