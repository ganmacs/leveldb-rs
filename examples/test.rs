extern crate leveldb;

// use logdb;

fn main() {
    let mut db = leveldb::open("level");
    db.set("key1", "value2");

    println!("{:?}", db.get("key"));
    println!("{:?}", db.get("key1"));
}
