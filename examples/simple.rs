use std::env;
use std::path::PathBuf;

use btree::Node;

fn main() {
    let filename = PathBuf::from(env::var("BTREE_FILENAME").expect("BTREE_FILENAME not set"));

    let mut node = Node::new(filename, 2).expect("Could not create node.");
    let _ = node.insert_if_space(0, "hello");
    let _ = node.insert_if_space(1, "world");
    node.save().expect("Could not save node.");

    let _ = dbg!(node.insert_if_space(2, "hello!"));
    node.save().expect("Could not save node.");
}
