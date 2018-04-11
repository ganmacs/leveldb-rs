extern crate leveldb;

fn main() {
    let mut db = leveldb::open("level");
    // db.set("key0", "value0");
    // db.set("key1", "value1");
    // db.set("key2", "value2");
    // db.set("key3", "value3");

    println!("{:?}", db.get("key0"));
    println!("{:?}", db.get("key1"));
    println!("{:?}", db.get("key2"));
    println!("{:?}", db.get("key3"));
    println!("{:?}", db.get("key4"));

    // let size = 100;

    // let mut w = leveldb::WriteBatch::new();
    // for i in 0..size {
    //     w.put(&format!("key-{:}", i), &format!("value-{:}", i))
    // }
    // db.apply(w);

    // for i in 0..size {
    //     if db.get(&format!("key-{:}", i)).is_none() {
    //         println!("not found key-{:?}", i);
    //     }
    // }
}
