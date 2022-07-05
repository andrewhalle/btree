use std::fs::{File, OpenOptions};
use std::io::{self, Seek};
use std::path::PathBuf;

use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use thiserror::Error;

type NodeRef = PathBuf;

pub struct Node<K, V> {
    file: File,
    data: NodeData<K, V>,
}

#[derive(Deserialize, Serialize)]
pub struct NodeData<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    children: Vec<NodeRef>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("An I/O error occurred.")]
    Io(#[from] io::Error),
    #[error("A serialization error occurred.")]
    Serialization(#[from] rmp_serde::encode::Error),
}

impl<K, V> Node<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn reset_file(&mut self) -> Result<(), Error> {
        self.file.set_len(0)?;
        self.file.rewind()?;

        Ok(())
    }

    pub fn new(path: PathBuf) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let data = NodeData {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        };

        Ok(Node { file, data })
    }

    pub fn save(&mut self) -> Result<(), Error> {
        self.reset_file()?;

        self.data.serialize(&mut Serializer::new(&mut self.file))?;

        Ok(())
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.data.keys.push(key);
        self.data.values.push(value);
    }
}
