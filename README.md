# Consistent Hash

Consistent Hashing is a distributed system algorithm for assigning values to partitions in a highly dynamic environment.

## Example:

Using the algorithm to LB redis cache. (Crate `redis = "0.17.0"`)

```rust
let mut ring: ConsistentHash<String,redis::Client> = match ConsistentHash::new(100) {
    Ok(ring) => ring,
    Err(err) => panic!(err),
};

ring.add_node("redis://my-redis-node-1".to_string(), redis::Client::open("redis://my-redis-node-1")?).unwrap();
ring.add_node("redis://my-redis-node-2".to_string(), redis::Client::open("redis://my-redis-node-2")?).unwrap();
ring.add_node("redis://my-redis-node-3".to_string(), redis::Client::open("redis://my-redis-node-3")?).unwrap();
ring.add_node("redis://my-redis-node-4".to_string(), redis::Client::open("redis://my-redis-node-4")?).unwrap();

let client = match ring.get_node(String::from("some-key")){
    Some(node) => node,
    None => panic!("Not found!"),
};

let mut con = client.get_connection()?;
con.get("some-key");
```