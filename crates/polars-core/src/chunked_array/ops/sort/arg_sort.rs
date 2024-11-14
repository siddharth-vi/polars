use super::*;

// Reduce monomorphisation.
fn sort_impl<T>(vals: &mut [(IdxSize, T)], options: SortOptions)
where
    T: TotalOrd + Send + Sync,
{
    sort_by_branch(
        vals,
        options.descending,
        |a, b| a.1.tot_cmp(&b.1),
        options.multithreaded,
    );
}
fn reverse_stable_no_nulls<I, J, T>(iters: I, len: usize) -> Vec<u32>
where
    I: IntoIterator<Item = J>,
    J: IntoIterator<Item = T>,
    T: TotalOrd + Send + Sync,
{
    let mut current_start = 0;
    let mut current_end = 1;
    let mut flattened_iter = iters.into_iter().flatten();
    let first_element = flattened_iter.next();
    let mut rev_idx = Vec::with_capacity(len);
    let mut i: IdxSize;
    match first_element {
        Some(value) => {
            let mut previous_element = value;
            while let Some(current_element) = flattened_iter.next() {
                if current_element.tot_cmp(&previous_element) == Ordering::Equal {
                    current_end += 1;
                } else {
                    //rev_idx.extend((current_start..current_end).rev());
                    i = current_end;
                    while i > current_start {
                        i = i - 1;
                        unsafe { rev_idx.push_unchecked(i) };
                    }
                    current_start = current_end;
                    current_end = current_end + 1;
                }
                previous_element = current_element;
            }
            rev_idx.extend((current_start..current_end).rev());
            rev_idx.reverse();
            return rev_idx;
        },
        None => return rev_idx,
    }
}

pub(super) fn arg_sort<I, J, T>(
    name: PlSmallStr,
    iters: I,
    options: SortOptions,
    null_count: usize,
    len: usize,
    is_sorted_descending_flag: bool,
    is_sorted_ascending_flag: bool,
    first_element_null: bool,
) -> IdxCa
where
    I: IntoIterator<Item = J>,
    J: IntoIterator<Item = Option<T>>,
    T: TotalOrd + Send + Sync,
{
    let nulls_last = options.nulls_last;
    let null_cap = if nulls_last { null_count } else { len };

    if (options.descending && is_sorted_descending_flag)
        || (!options.descending && is_sorted_ascending_flag)
    {
        if (nulls_last && !first_element_null) || (!nulls_last && first_element_null) {
            return ChunkedArray::with_chunk(
                name,
                IdxArr::from_data_default(
                    Buffer::from((0..(len as IdxSize)).collect::<Vec<IdxSize>>()),
                    None,
                ),
            );
        }
    }

    let mut vals = Vec::with_capacity(len - null_count);
    let mut nulls_idx = Vec::with_capacity(null_cap);
    let mut count: IdxSize = 0;

    for arr_iter in iters {
        let iter = arr_iter.into_iter().filter_map(|v| {
            let i = count;
            count += 1;
            match v {
                Some(v) => Some((i, v)),
                None => {
                    // SAFETY: we allocated enough.
                    unsafe { nulls_idx.push_unchecked(i) };
                    None
                },
            }
        });
        vals.extend(iter);
    }
    sort_impl(vals.as_mut_slice(), options);
    let iter = vals.into_iter().map(|(idx, _v)| idx);
    let idx = if nulls_last {
        let mut idx = Vec::with_capacity(len);
        idx.extend(iter);
        idx.extend(nulls_idx);
        idx
    } else {
        let ptr = nulls_idx.as_ptr() as usize;
        nulls_idx.extend(iter);
        // We had a realloc.
        debug_assert_eq!(nulls_idx.as_ptr() as usize, ptr);
        nulls_idx
    };

    ChunkedArray::with_chunk(name, IdxArr::from_data_default(Buffer::from(idx), None))
}

pub(super) fn arg_sort_no_nulls<I, J, T>(
    name: PlSmallStr,
    iters: I,
    options: SortOptions,
    len: usize,
    is_sorted_descending_flag: bool,
    is_sorted_ascending_flag: bool,
) -> IdxCa
where
    I: IntoIterator<Item = J>,
    J: IntoIterator<Item = T>,
    T: TotalOrd + Send + Sync,
{
    if (options.descending && is_sorted_descending_flag)
        || (!options.descending && is_sorted_ascending_flag)
    {
        return ChunkedArray::with_chunk(
            name,
            IdxArr::from_data_default(
                Buffer::from((0..(len as IdxSize)).collect::<Vec<IdxSize>>()),
                None,
            ),
        );
    } else if (options.descending && is_sorted_ascending_flag)
        || (!options.descending && is_sorted_descending_flag)
    {
        return ChunkedArray::with_chunk(
            name,
            IdxArr::from_data_default(Buffer::from(reverse_stable_no_nulls(iters, len)), None),
        );
    }

    let mut vals = Vec::with_capacity(len);

    let mut count: IdxSize = 0;
    for arr_iter in iters {
        vals.extend(arr_iter.into_iter().map(|v| {
            let idx = count;
            count += 1;
            (idx, v)
        }));
    }
    sort_impl(vals.as_mut_slice(), options);

    let iter = vals.into_iter().map(|(idx, _v)| idx);
    let idx: Vec<_> = iter.collect_trusted();

    ChunkedArray::with_chunk(name, IdxArr::from_data_default(Buffer::from(idx), None))
}
