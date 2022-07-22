use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{self, Seek};
use std::mem;
use std::path::PathBuf;

use lru::LruCache;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};

type NodeRef = PathBuf;

pub struct BTree<K, V> {
    root_node: Node<K, V>,
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
}

#[derive(thiserror::Error, Debug)]
enum NodeError {
    #[error("Cannot insert into the node because it is too full.")]
    NeedsSplit,
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
    pub fn new(dir: PathBuf, capacity: usize) -> Result<Self, Error> {
        DirBuilder::new().create(&dir)?;
        let mut root_node = dir.clone();
        root_node.push("root");
        let root_node = match Node::new(root_node, capacity) {
            Ok(node) => node,
            Err(NodeError::Io(e)) => {
                return Err(Error::Io(e));
            }
            _ => unreachable!(),
        };
        let node_cache = LruCache::new(256);

        Ok(Self {
            root_node,
            node_cache,
        })
    }

    /// If the key was already present, return the old value. If the key was not present, return
    /// None.
    pub fn insert(&mut self, key: K, mut value: V) -> Option<V> {
        let mut curr_node = &mut self.root_node;

        while !curr_node.is_leaf() {
            match curr_node.data.keys[..].binary_search(&key) {
                Ok(idx) => {
                    mem::swap(&mut value, &mut curr_node.data.values[idx]);
                    return Some(value);
                }
                Err(idx) => {
                    let child = curr_node.data.children.as_ref().unwrap()[idx].clone();
                    if self.node_cache.contains(&child) {
                        curr_node = self.node_cache.get_mut(&child).unwrap();
                    } else {
                        let node = Node::load(&child).expect("TODO");
                        self.node_cache.push(child.clone(), node);
                        curr_node = self.node_cache.get_mut(&child).unwrap();
                    }
                }
            }
        }

        curr_node.insert_if_space(key, value).expect("TODO");
        curr_node.save().expect("TODO");

        None
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let data = NodeData {
            keys: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            children: None,
        };

        Ok(Node { file, data })
    }

    fn save(&mut self) -> Result<(), NodeError> {
        self.reset_file()?;

        self.data.serialize(&mut Serializer::new(&mut self.file))?;

        Ok(())
    }

    // TODO: This should return Result<Option<V>, NodeError> to handle the case of the value
    // already existing.
    fn insert_if_space(&mut self, key: K, value: V) -> Result<(), NodeError> {
        let idx = &self.data.keys[..].binary_search(&key);

        match idx {
            Ok(idx) => Ok(self.data.values[*idx] = value),
            Err(idx) => {
                if self.data.keys.len() < self.data.keys.capacity() {
                    self.data.keys.insert(*idx, key);
                    self.data.values.insert(*idx, value);
                    Ok(())
                } else {
                    Err(NodeError::NeedsSplit)
                }
            }
        }
    }

    fn is_leaf(&self) -> bool {
        self.data.children.is_none()
    }

    fn load(path: &NodeRef) -> Result<Self, NodeError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let data = rmp_serde::from_read(file.try_clone()?)?;

        Ok(Self { file, data })
    }
}
