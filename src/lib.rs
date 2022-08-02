use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{self, Seek};
use std::mem;
use std::path::PathBuf;

use lru::LruCache;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

type NodeRef = PathBuf;

pub struct BTree<K, V> {
    root_node: Node<K, V>,
    backing_dir: PathBuf,
    node_cache: LruCache<NodeRef, Node<K, V>>,
}

struct Node<K, V> {
    file: File,
    data: NodeData<K, V>,
}

#[derive(Deserialize, Serialize)]
struct NodeData<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    children: Option<Vec<NodeRef>>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("An I/O error occurred.")]
    Io(#[from] io::Error),
    #[error("A serialization error occurred.")]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("A node error occurred. {0}")]
    Node(#[from] NodeError),
}

#[derive(thiserror::Error, Debug)]
enum NodeError {
    #[error("An I/O error occurred.")]
    Io(#[from] io::Error),
    #[error("A serialization error occurred.")]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("A deserialization error occurred.")]
    Deserialization(#[from] rmp_serde::decode::Error),
}

impl<K, V> BTree<K, V>
where
    K: for<'a> Deserialize<'a> + Serialize + Ord,
    V: for<'a> Deserialize<'a> + Serialize,
{
    pub fn new(backing_dir: PathBuf, capacity: usize) -> Result<Self, Error> {
        DirBuilder::new().create(&backing_dir)?;
        let mut root_node = backing_dir.clone();
        root_node.push("root");
        let root_node = Node::new(root_node, capacity)?;
        let node_cache = LruCache::new(256);

        Ok(Self {
            root_node,
            backing_dir,
            node_cache,
        })
    }

    /// If the key was already present, return the old value. If the key was not present, return
    /// None.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>, Error> {
        if self.root_node.is_full() {
            let capacity = self.root_node.capacity();
            let (key, value, right) = self.root_node.split(self.new_node_name())?;
            self.root_node.rename(self.new_node_name())?;
            let mut new_root = self.backing_dir.clone();
            new_root.push("root");
            let mut new_root = Node::new(new_root, capacity)?;
            // This .unwrap() is safe because we just allocated the Node, so it can't have any
            // existing values.
            new_root.insert(key, value).unwrap();
            self.root_node = new_root;
        }

        todo!()
    }

    fn new_node_name(&self) -> NodeRef {
        let mut path = self.backing_dir.clone();
        path.push(Uuid::new_v4().to_string());

        path
    }
}

impl<K, V> NodeData<K, V>
where
    K: for<'a> Deserialize<'a> + Serialize + Ord,
    V: for<'a> Deserialize<'a> + Serialize,
{
    fn new(capacity: usize) -> Self {
        assert!(capacity % 2 == 1 && capacity > 3);

        Self {
            keys: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            children: None,
        }
    }

    // This method assumes there is space to insert a new value if needed. If this proves untrue,
    // panic.
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        let idx = &self.keys[..].binary_search(&key);

        match idx {
            Ok(idx) => {
                let old = value;
                mem::swap(&mut old, &mut self.values[*idx]);
                Some(old)
            }
            Err(idx) => {
                if !self.is_full() {
                    self.keys.insert(*idx, key);
                    self.values.insert(*idx, value);
                    None
                } else {
                    panic!("insert called on Node without remaining space.")
                }
            }
        }
    }

    fn is_leaf(&self) -> bool {
        self.children.is_none()
    }

    fn is_full(&self) -> bool {
        self.keys.len() == self.keys.capacity()
    }

    fn capacity(&self) -> usize {
        self.keys.capacity()
    }

    // Splits self on the middle value and returns the split value and the new NodeData.
    fn split(&mut self) -> (K, V, Self) {
        assert!(self.is_full());

        let split_idx = self.keys.capacity() / 2 + 1;
        let keys = self.keys.split_off(split_idx);
        let values = self.values.split_off(split_idx);
        let children = self.children.as_mut().map(|v| v.split_off(split_idx));
        let other = NodeData::new(self.keys.capacity());
        other.keys.append(&mut keys);
        other.values.append(&mut values);
        other.children = children;

        // .unwrap() is fine here, because we know this value will exist.
        let key = self.keys.pop().unwrap();
        let value = self.values.pop().unwrap();

        (key, value, other)
    }
}

impl<K, V> Node<K, V>
where
    K: for<'a> Deserialize<'a> + Serialize + Ord,
    V: for<'a> Deserialize<'a> + Serialize,
{
    fn reset_file(&mut self) -> Result<(), NodeError> {
        self.file.set_len(0)?;
        self.file.rewind()?;

        Ok(())
    }

    fn new(path: PathBuf, capacity: usize) -> Result<Self, NodeError> {
        Node::new_with_data(path, NodeData::new(capacity))
    }

    fn new_with_data(path: PathBuf, data: NodeData<K, V>) -> Result<Self, NodeError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        Ok(Node { file, data })
    }

    fn save(&mut self) -> Result<(), NodeError> {
        self.reset_file()?;

        self.data.serialize(&mut Serializer::new(&mut self.file))?;

        Ok(())
    }

    fn load(path: &NodeRef) -> Result<Self, NodeError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let data = rmp_serde::from_read(file.try_clone()?)?;

        Ok(Self { file, data })
    }

    fn split(&mut self, other: NodeRef) -> Result<(K, V, Self), NodeError> {
        let (key, value, data) = self.data.split();
        let other = Node::new_with_data(other, data)?;

        Ok((key, value, other))
    }

    fn is_full(&self) -> bool {
        self.data.is_full()
    }

    fn capacity(&self) -> usize {
        self.data.capacity()
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.data.insert(key, value)
    }

    fn data(self) -> NodeData<K, V> {
        self.data
    }

    fn rename(&mut self, new_name: NodeRef) -> Result<(), NodeError> {
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(new_name)?;
        let old_file = mem::replace(&mut self.file, new_file);
        self.save()?;
        // TODO
    }
}
