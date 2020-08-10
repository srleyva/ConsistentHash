#![feature(map_first_last)]
#![feature(test)]

use rand::Rng;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::fmt::Display;
use md5::compute;

extern crate test;

trait Hash {
    fn hash(&self, weight: i32) -> u128;
}

impl Hash for String {
    fn hash(&self, weight: i32) -> u128 {
        let hash_string = format!("{}-{}", self, weight);
        let digest = compute(hash_string);
        return u128::from_be_bytes(*digest);
    }
}

#[test]
fn test_string_md5_hash() {
    let hash = String::from("test").hash(0);
    assert_eq!(hash, 178633651610943467493091302425572625585);
}

struct ConsistentHash<K, V> {
    ring: BTreeMap<u128, Arc<Mutex<V>>>,
    keys: Vec<K>,
    replicas: i32,
} 

impl<K: Hash + Ord + Display, V: Display> ConsistentHash<K, V> {
    fn new(replicas: i32) -> Result<ConsistentHash<K, V>, String> {
        if replicas <= 0 {
            return Err(String::from("replcia count must be greater than 0"));
        }
        
        Ok(ConsistentHash {
            ring: BTreeMap::new(),
            keys: Vec::new(),
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
        self.keys.push(key);
        Ok(())
    }

    fn get_node(&self, name: &K) -> Option<&Arc<Mutex<V>>> {
        if let Some(key) = self.search_nearest(name.hash(0)) {
            return self.ring.get(&key);
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

    fn search_nearest(&self, name: u128) -> Option<u128> {
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
                if *next.0 > name {
                    return Some(*cur.0);
                } 
            }
        }
        Some(*first_entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

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
        let mut ring: ConsistentHash<String,String> = match ConsistentHash::new(1000) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };

        let num_nodes = 10;
        let num_hits = 1000;
        let num_values = 10000;

        for i in 1..num_nodes+1 {
            let node_name = format!("node{}", i);
            let node_val = format!("node_value{}", i);
            match ring.add_node(node_name, node_val) {
                Err(err) => panic!(err),
                Ok(()) => (),
            };
        }

        let mut distributions: HashMap<String, i32> = HashMap::new();

        let mut rng = rand::thread_rng();
        for _ in 0..num_hits {
            let rand_num: u16 = rng.gen_range(0, num_values);
            let node = match ring.get_node(&rand_num.to_string()){
                Some(node) => node,
                None => panic!("Not found!"),
            };

            let distribution_key = node.lock().unwrap();

            let mut count = match distributions.get(&*distribution_key) {
                Some(result) => *result,
                None => 0,
            };

            count += 1;
            distributions.insert(String::from(&*distribution_key), count);
        }

        assert_eq!(distributions.values().sum::<i32>(), num_hits);
        
        let min = distributions.values().min().unwrap();
        let max = distributions.values().max().unwrap();
        if (*max - *min) > 40 {
            for (key, value) in distributions.iter() {
                // Check Deviation for 10 node 100 virtual node partition
               println!("{}: {}", key, value);
            };
            panic!("Too much deviation in my partitions");
        }
    }

    #[bench]
    fn bench_consistent_search(b: &mut Bencher) {
        let mut ring: ConsistentHash<String,String> = match ConsistentHash::new(1000) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };

        for i in 1..11 {
            let node_name = format!("node{}", i);
            let node_val = format!("node_value{}", i);
            match ring.add_node(node_name, node_val) {
                Err(err) => panic!(err),
                Ok(()) => (),
            };
        }

        let mut rng = rand::thread_rng();
        let num_values = 10000;
        
        b.iter(|| {
            let rand_num: u16 = rng.gen_range(0, num_values);
            match ring.get_node(&rand_num.to_string()){
                Some(_) => {},
                None => panic!("Not found!"),
            };
        })
    }
}