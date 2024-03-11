struct Header {}

struct Page {
    hdr: *mut Option<Header>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn page_test() {
        assert!(true);
    }
}
