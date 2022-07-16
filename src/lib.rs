use std::error::Error as StdError;
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{self, Seek};
use std::path::PathBuf;

use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

type NodeRef = PathBuf;

pub struct BTree<K, V> {
    root_node: Node<K, V>,
    dir: PathBuf,
}

struct Node<K, V> {
    file: File,
    data: NodeData<K, V>,
}

#[derive(Deserialize, Serialize)]
struct NodeData<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    children: Vec<NodeRef>,
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
}

impl<K, V> BTree<K, V>
where
    K: Serialize + Ord,
    V: Serialize,
{
    pub fn new(dir: PathBuf, capacity: usize) -> Result<Self, Error> {
        DirBuilder::new().create(&dir)?;
        let mut root_node = dir.clone();
        root_node.push(Uuid::new_v4().to_string());
        let root_node = match Node::new(root_node, capacity) {
            Ok(node) => node,
            Err(NodeError::Io(e)) => {
                return Err(Error::Io(e));
            }
            _ => unreachable!(),
        };

        Ok(Self { root_node, dir })
    }
}

impl<K, V> Node<K, V>
where
    K: Serialize + Ord,
    V: Serialize,
{
    fn reset_file(&mut self) -> Result<(), NodeError> {
        self.file.set_len(0)?;
        self.file.rewind()?;

        Ok(())
    }

    pub fn new(path: PathBuf, capacity: usize) -> Result<Self, NodeError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let data = NodeData {
            keys: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            children: Vec::with_capacity(capacity + 1),
        };

        Ok(Node { file, data })
    }

    pub fn save(&mut self) -> Result<(), NodeError> {
        self.reset_file()?;

        self.data.serialize(&mut Serializer::new(&mut self.file))?;

        Ok(())
    }

    pub fn insert_if_space(&mut self, key: K, value: V) -> Result<(), NodeError> {
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
}
