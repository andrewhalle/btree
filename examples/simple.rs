use std::env;
use std::path::PathBuf;

use btree::BTree;

fn main() {
    let dir = PathBuf::from(env::var("BTREE_FILENAME").expect("BTREE_FILENAME not set"));

    let _tree: BTree<String, String> = BTree::new(dir, 16).expect("Could not create BTree.");
}
