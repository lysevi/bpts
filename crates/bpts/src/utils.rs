pub unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts((p as *const T) as *const u8, ::core::mem::size_of::<T>())
}

pub trait BuferWriter {
    fn size(&self) -> usize;
    fn write_id(&mut self, v: bpts_tree::types::Id);
    fn write_u32(&mut self, v: u32);
    fn write_i32(&mut self, v: i32);
    fn write_bool(&mut self, v: bool);
    fn write_sized<T: Sized>(&mut self, v: &T);
}

pub struct Counter {
    count: usize,
}

impl Counter {
    pub fn new() -> Counter {
        Counter { count: 0 }
    }

    fn plus<T>(&mut self) {
        self.count += std::mem::size_of::<T>();
    }
}
impl BuferWriter for Counter {
    fn size(&self) -> usize {
        self.count
    }

    fn write_u32(&mut self, _: u32) {
        self.plus::<u32>();
    }

    fn write_sized<T: Sized>(&mut self, _: &T) {
        self.count += std::mem::size_of::<T>();
    }

    fn write_id(&mut self, _: bpts_tree::types::Id) {
        self.plus::<i32>();
    }

    fn write_i32(&mut self, _: i32) {
        self.plus::<i32>();
    }
    fn write_bool(&mut self, _: bool) {
        self.plus::<bool>();
    }
}

pub struct UnsafeWriter {
    buffer: *mut u8,
    count: usize,
}

impl UnsafeWriter {
    pub fn new(buffer: *mut u8) -> UnsafeWriter {
        UnsafeWriter { buffer, count: 0 }
    }

    fn plus<T>(&mut self) {
        self.count += std::mem::size_of::<T>();
    }
}

impl BuferWriter for UnsafeWriter {
    fn size(&self) -> usize {
        self.count
    }

    fn write_id(&mut self, v: bpts_tree::types::Id) {
        self.write_i32(v.0);
    }

    fn write_u32(&mut self, v: u32) {
        unsafe {
            let ptr = self.buffer.add(self.count) as *mut u32;
            ptr.write(v);
            //*ptr = v;
            self.plus::<u32>();
        }
    }

    fn write_i32(&mut self, v: i32) {
        unsafe {
            let ptr = self.buffer.add(self.count) as *mut i32;
            ptr.write(v);
            //*ptr = v;
            self.plus::<i32>();
        }
    }

    fn write_bool(&mut self, v: bool) {
        unsafe {
            let ptr = self.buffer.add(self.count) as *mut bool;
            ptr.write(v);
            //*ptr = v;
            self.plus::<bool>();
        }
    }

    fn write_sized<T: Sized>(&mut self, v: &T) {
        unsafe {
            let src_ptr = v as *const T;
            let dest_ptr = self.buffer.add(self.count) as *mut T;
            std::ptr::copy(src_ptr, dest_ptr, 1);
            self.plus::<T>();
        }
    }
}
