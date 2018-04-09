use std::ptr;

type Link<T> = Box<Node<T>>;

pub struct CircularLinkedList<T> {
    dummy: Link<T>,
}

pub struct Node<T> {
    elem: T,
    next: *mut Node<T>,
    prev: *mut Node<T>,
}

impl<T> Node<T> {
    pub fn new(elem: T) -> Self {
        Node {
            elem: elem,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

pub struct Iter<'a, T: 'a> {
    head: *const Node<T>,
    next: &'a Node<T>,
}

impl<T> Drop for CircularLinkedList<T> {
    fn drop(&mut self) {
        let finish = self.dummy.as_ref() as *const _;
        let mut n = self.dummy.prev;

        while (n as *const _) != finish {
            unsafe {
                Box::from_raw(n); // take back the right of releasing
                n = (*n).prev;
            }
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if (self.next as *const _) == self.head {
            None
        } else {
            let v = self.next;
            self.next = unsafe { &(*self.next.next) };
            Some(&v.elem)
        }
    }
}

impl<T> CircularLinkedList<T> {
    pub fn new(v: T) -> Self {
        let mut n = Box::new(Node::new(v));
        n.next = &mut *n;
        n.prev = &mut *n;

        CircularLinkedList { dummy: n }
    }

    pub fn append(&mut self, n: T) {
        let mut new_node = Box::new(Node::new(n));

        new_node.prev = self.dummy.prev;
        new_node.next = &mut *self.dummy;

        unsafe {
            (*new_node.prev).next = &mut *new_node;
            // Take over the right of releaseing new_node
            self.dummy.prev = Box::into_raw(new_node);
        };
    }

    pub fn current(&self) -> Option<&T> {
        let r = self.dummy.prev;
        Some(unsafe { &(*r).elem })
    }

    pub fn iter(&self) -> Iter<T> {
        let r = self.dummy.as_ref();
        let v = unsafe { self.dummy.next.as_ref().expect("failed to unwrap") };
        Iter { head: r, next: &v }
    }
}
