#![feature(map_first_last)]

use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::fmt::Display;
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
    ring: BTreeMap<K, Arc<Mutex<V>>>,
    replicas: i32,
} 

impl<K: Hash + Ord + Display, V: Display> ConsistentHash<K, V> {
    fn new(replicas: i32) -> Result<ConsistentHash<K, V>, String> {
        if replicas <= 0 {
            return Err(String::from("replcia count must be greater than 0"));
        }
        
        Ok(ConsistentHash {
            ring: BTreeMap::new(),
            replicas,
        })
    }

    fn print_node(&self) {
        for (key, value) in self.ring.iter() {
            
            let value = value.lock().unwrap();
            println!("{}: {}", key, value);
        }
    }

    fn add_node(&mut self, key: K, value: V) -> Result<(), String> {
        let value = Arc::new(Mutex::new(value));
        for i in 0..self.replicas {
            let value = value.clone();
            if self.ring.contains_key(&key.hash(i)) {
                return Err(String::from("Key already in ring"));
            }
            self.ring.insert(key.hash(i), value);
        }
        Ok(())
    }

    fn get_node(&self, name: &K) -> Option<&Arc<Mutex<V>>> {
        if let Some(key) = self.search_nearest(name) {
            return self.ring.get(key);
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

        for _ in 0..self.ring.len() {
            let cur = match map_iter.next() {
                Some(cur) => cur,
                None => { return None; },
            };

            if let Some(next) = map_iter.peek() {
                if *next.0 > *name {
                    return Some(cur.0);
                } 
            }
        }
        Some(first_entry)
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
    }

    #[test]
    #[should_panic]
    fn fail_if_bad_replicas() {
        let consistent_hash: ConsistentHash<String,i32> = match ConsistentHash::new(-20) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };
    }

    #[test]
    fn property_based() {
        let mut consistent_hash: ConsistentHash<String,i32> = match ConsistentHash::new(100) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };

        consistent_hash.add_node("Node-1".to_string(), 0).unwrap();
        consistent_hash.add_node("Node-2".to_string(), 0).unwrap();
        consistent_hash.add_node("Node-3".to_string(), 0).unwrap();
        consistent_hash.add_node("Node-4".to_string(), 0).unwrap();

        for _ in 0..=100 {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .collect();

            let counter = match consistent_hash.get_node(&rand_string){
                Some(val) => {
                    val
                },
                None => { panic!("None Recieved") },
            };

            let mut counter_lock = counter.lock().unwrap();
            *counter_lock = *counter_lock + 1;
        }
        consistent_hash.print_node();
    }
}