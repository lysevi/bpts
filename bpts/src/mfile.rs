use memmap::MmapOptions;
use std::error::Error;
use std::io::{Read, Write};
use std::fs::File;
use memmap::Mmap;

fn read_mm(file:&str) -> String{
    let mut file = File::open("README.md").unwrap();

    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();

    let mmap = unsafe { Mmap::map(&file).unwrap()  };

    let result= &mmap[..];
    String::from(result)
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        
    }
}
