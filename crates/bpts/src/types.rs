#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub struct Id(pub u32);

const EMPTY_ID: Id = Id(std::u32::MAX);

impl Id {
    pub fn empty() -> Id {
        EMPTY_ID
    }
    pub fn unwrap(&self) -> u32 {
        self.0
    }

    pub fn is_empty(self) -> bool {
        return self == EMPTY_ID;
    }

    pub fn exists(self) -> bool {
        return !self.is_empty();
    }

    pub fn clear(&mut self) {
        self.0 = EMPTY_ID.0;
    }
}

pub struct SingleElementStore<T: Clone> {
    value: Option<T>,
}

impl<T: Clone> SingleElementStore<T> {
    pub fn new_with(t: T) -> SingleElementStore<T> {
        SingleElementStore { value: Some(t) }
    }

    pub fn new() -> SingleElementStore<T> {
        SingleElementStore { value: None }
    }

    pub fn replace(&mut self, v: T) {
        self.value = Some(v);
    }

    pub fn is_empty(&self) -> bool {
        return self.value.is_none();
    }

    pub fn as_ptr(&self) -> *const T {
        match self.value {
            Some(ref x) => x as *const T,
            None => std::ptr::null(),
        }
    }

    pub fn as_value(&self) -> T {
        match self.value {
            Some(ref x) => x.clone(),
            None => panic!(),
        }
    }

    pub fn apply<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut T),
    {
        match self.value {
            Some(ref mut x) => f(x),
            None => return,
        }
    }
}
