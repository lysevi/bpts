extern crate tempfile;
use crate::utils;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::DerefMut;

#[allow(dead_code)]
struct Header {
    id: u64,
}

#[allow(dead_code)]
pub fn read_mm() {
    let tempdir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(tempdir.path().join("make_mut"))
        .unwrap();
    print!(">>> {:?}", tempdir.path());
    file.set_len(128).unwrap();
    let mut mut_mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };

    //let mut mut_mmap = mmap.make_mut().unwrap();
    let mut hdr = Header { id: 2 };
    let hdr_buf = unsafe { utils::any_as_u8_slice(&hdr) };
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
    let hdr_src = &hdr as *const Header;
    let dest_buf = mut_mmap.as_mut_ptr() as *mut Header;

    unsafe {
        std::ptr::copy(hdr_src, dest_buf, size_of_header);
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

#[allow(dead_code)]
pub fn read_mm_anon() {
    let mut mut_mmap = memmap2::MmapMut::map_anon(1024 * 1024).unwrap();

    //let mut mut_mmap = mmap.make_mut().unwrap();
    let mut hdr = Header { id: 2 };
    let hdr_buf = unsafe { utils::any_as_u8_slice(&hdr) };
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
    let hdr_src = &hdr as *const Header;
    let dest_buf = mut_mmap.as_mut_ptr() as *mut Header;

    unsafe {
        std::ptr::copy(hdr_src, dest_buf, size_of_header);
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

    #[test]
    fn it_works_anon() {
        super::read_mm_anon();
    }
}