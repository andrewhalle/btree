use std::env;
use std::path::PathBuf;

use btree::Node;

fn main() {
    let filename = PathBuf::from(env::var("BTREE_FILENAME").expect("BTREE_FILENAME not set"));

    let mut node = Node::new(filename).expect("Could not create node.");
    node.insert(0, "hello");
    node.insert(1, "world");
    node.save().expect("Could not save node.");

    node.insert(2, "hello!");
    node.save().expect("Could not save node.");
}
