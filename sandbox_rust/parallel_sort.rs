fn qsort_partition<T: Ord + Copy>(to_sort: &mut Vec<T>, lo : usize, hi : usize) -> usize
{
  let mid : usize = lo + ((hi - lo) >> 1);
  let lmid : usize = lo + ((mid - lo) >> 1);
  let rmid : usize = mid + ((hi - mid) >> 1);

  if to_sort[lo]>to_sort[lmid] {to_sort.swap(lo, lmid)}
  if to_sort[rmid]>to_sort[hi] {to_sort.swap(rmid, hi)}
  if to_sort[mid]>to_sort[hi] {to_sort.swap(mid, hi)}
  if to_sort[mid]>to_sort[rmid] {to_sort.swap(mid, rmid)}
  if to_sort[lo]>to_sort[rmid] {to_sort.swap(lo, rmid)}
  if to_sort[lo]>to_sort[mid] {to_sort.swap(lo, mid)}
  if to_sort[lmid]>to_sort[hi] {to_sort.swap(lmid,hi)}
  if to_sort[lmid]>to_sort[rmid] {to_sort.swap(lmid,rmid)}
  if to_sort[lmid]>to_sort[mid] {to_sort.swap(lmid,mid)}

  let pivot : T = to_sort[mid];

  let mut i : usize = lo - 1;
  let mut j : usize = hi + 1;
  loop 
  {
    i += 1;
    j -= 1;
    loop
    {
      let mut changed : bool = false;
      if to_sort[i] < pivot
      {
        changed = true;
        i += 1;
      }
      if to_sort[j] > pivot
      {
        changed = true;
        j -= 1;
      }
      if !changed {break;}
    }
    if i >= j {return j;}
    to_sort.swap(i,j);
  }
}

fn qsort_heap<T: Ord + Copy>(to_sort: &mut Vec<T>, lo : usize, hi : usize)
{
    let count: usize = hi - lo + 1;
    let mut start: usize = count/2;
    let mut end: usize = count;
    while end > 1
    {
      if start > 0 
      {
        start-=1;
      }
      else 
      {
        end-=1;
        to_sort.swap(lo + end,lo);
      }
      let mut root: usize = start;
      while 2*root + 1 < end 
      {
        let mut child: usize = 2*root + 1;
        if child + 1 < end && to_sort[lo + child] < to_sort[lo +child+1] {child+=1;}
        
        if to_sort[lo + root] < to_sort[lo + child]
        {
          to_sort.swap(lo + root, lo + child);
          root = child;
        }
        else 
        {
          break;
        }
      }
    }
}

fn qsort_insertion<T: Ord + Copy>(to_sort: &mut Vec<T>, lo : usize, hi : usize)
{
  let mut i: usize = 1;
  while lo + i <= hi
  {
    let mut j: usize = i;
    while j > 0 && to_sort[lo + j-1] > to_sort[lo + j]
    {
      to_sort.swap(lo + j-1, lo + j);
      j -= 1;
    }
    i += 1;
  }
}


pub fn main() 
{
    println!("Hello World");
}