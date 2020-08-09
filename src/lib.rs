#![feature(map_first_last)]

use std::collections::BTreeMap;
use std::sync::Arc;
use md5::compute;

trait Hash {
    fn hash(&self, weight: i32) -> Self;
}

impl Hash for String {
    fn hash(&self, weight: i32) -> Self {
        let hash_string = format!("{}-{}", self, weight);
        let digest = compute(hash_string);
        return format!("{:x}", digest);
    }
}

#[test]
fn test_string_md5_hash() {
    let hash = String::from("test").hash(0);
    assert_eq!(hash, "86639701cdcc5b39438a5f009bd74cb1");
}

struct ConsistentHash<K, V> {
    ring: BTreeMap<K, Arc<V>>,
    replicas: i32,
} 

impl<K: Hash + Ord, V> ConsistentHash<K, V> {
    fn new(replicas: i32) -> Result<ConsistentHash<K, V>, String> {
        if replicas <= 0 {
            return Err(String::from("replcia count must be greater than 0"));
        }
        
        Ok(ConsistentHash {
            ring: BTreeMap::new(),
            replicas,
        })
    }

    fn add_node(&mut self, key: K, value: V) -> Result<(), String> {
        let value = Arc::new(value);
        for i in 0..self.replicas {
            let value = value.clone();
            if self.ring.contains_key(&key.hash(i)) {
                return Err(String::from("Key already in ring"));
            }
            self.ring.insert(key.hash(i), value);
        }
        Ok(())
    }

    fn get_node(&self, name: &K) -> Option<&Arc<V>> {
        if let Some(key) = self.search_nearest(name) {
            return self.ring.get(key)
        }
        None
    }

    fn delete_node(&mut self, name: &K) -> Result<(), String> {
        // TODO: Rebalance
        for i in 0..self.replicas {
            if !self.ring.contains_key(&name.hash(i)) {
                return Err(String::from("Key does not exist in ring"));
            }
            self.ring.remove(&name.hash(i));
        }
        Ok(())
    }

    fn search_nearest(&self, name: &K) -> Option<&K> {
        // TODO: Binary search
        let mut map_iter = self.ring.iter().peekable();
        let first_entry = match map_iter.peek() {
            Some(entry) => entry.0,
            None => { return None; },
        };
        
        for (key, _) in map_iter.clone() {
            match map_iter.peek() {

                Some(next_entry) => {
                    if *next_entry.0 > *name {
                        return Some(key);
                    } 
                },
                None => {
                    return Some(first_entry);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_consistent_hash() {
        let mut consistent_hash: ConsistentHash<String,i32> = match ConsistentHash::new(10) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };

        consistent_hash.add_node(String::from("node1"), 1).unwrap_err();
        consistent_hash.add_node(String::from("node2"), 2).unwrap_err();
        consistent_hash.add_node(String::from("node3"), 3).unwrap_err();
        consistent_hash.add_node(String::from("node4"), 4).unwrap_err();
    }

    #[test]
    #[should_panic]
    fn fail_if_bad_replicas() {
        let consistent_hash: ConsistentHash<String,i32> = match ConsistentHash::new(-20) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };
    }
}