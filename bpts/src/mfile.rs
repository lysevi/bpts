extern crate tempfile;
use memmap2::Mmap;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::DerefMut;

struct Header {
    id: u64,
}

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts((p as *const T) as *const u8, ::core::mem::size_of::<T>())
}

fn read_mm() {
    let tempdir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(tempdir.path().join("make_mut"))
        .unwrap();
    print!(">>> {:?}", tempdir.path());
    file.set_len(128).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    let mut mut_mmap = mmap.make_mut().unwrap();
    let mut hdr = Header { id: 2 };
    let hdr_buf = unsafe { any_as_u8_slice(&hdr) };
    mut_mmap.deref_mut().write_all(hdr_buf).unwrap();

    let size_of_header = std::mem::size_of::<Header>();
    let buf = &mut_mmap[0..size_of_header];

    {
        let (head, body, _tail) = unsafe { buf.align_to::<Header>() };
        assert!(head.is_empty(), "Data was not aligned");
        let my_struct = &body[0];
        assert!(my_struct.id == 2);
    }
    hdr.id = 22;
    let hdr_buf = &hdr as *const Header;
    let mut buf = mut_mmap.as_mut_ptr() as *mut Header;

    for i in 0..size_of_header {
        unsafe {
            std::ptr::copy(hdr_buf, buf, size_of_header);
        }
    }
    

    {
        let buf = &mut_mmap[0..size_of_header];
        let (head, body, _tail) = unsafe { buf.align_to::<Header>() };
        assert!(head.is_empty(), "Data was not aligned");
        let my_struct = &body[0];
        println!(">> {}", my_struct.id);
        assert!(my_struct.id == 22);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        super::read_mm();
    }
}
