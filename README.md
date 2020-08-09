# Consistent Hash Algo 

## Example:

```rust
let mut consistent_hash: ConsistentHash<String,i32> = match ConsistentHash::new(100) {
    Ok(ring) => ring,
    Err(err) => panic!(err),
};

consistent_hash.add_node("Node-1".to_string(), 0).unwrap();
consistent_hash.add_node("Node-2".to_string(), 0).unwrap();
consistent_hash.add_node("Node-3".to_string(), 0).unwrap();
consistent_hash.add_node("Node-4".to_string(), 0).unwrap();

let counter = match consistent_hash.get_node(&rand_string){
    Some(val) => {
        val
    },
    None => { panic!("None Recieved") },
};
```