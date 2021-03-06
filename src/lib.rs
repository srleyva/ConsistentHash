#![feature(test)]

use std::collections::HashMap;
use std::fmt::Display;
use md5::compute;

extern crate test;

pub trait Hash {
    fn hash(&self, weight: i32) -> u128;
}

impl Hash for String {
    fn hash(&self, weight: i32) -> u128 {
        let hash_string = format!("{}-{}", self, weight);
        let digest = compute(hash_string);
        return u128::from_be_bytes(*digest);
    }
}

pub trait Evict<RHS=Self> {
    fn evict(self) -> Self;
    fn merge(&mut self, item: RHS) -> Result<(), String>;
}

impl Evict for i32 {
    fn evict(self) -> Self {
        return self;
    }

    fn merge(&mut self, item: i32) -> Result<(), String> {
        *self += item;
        Ok(())
    }
}

impl Evict for String {
    fn evict(self) -> Self {
        return self;
    }

    fn merge(&mut self, item: String) -> Result<(), String> {
        *self += &item;
        Ok(())
    }
}

pub struct ConsistentHash<K, V> {
    ring: HashMap<u128, V>,
    keys: Vec<u128>,
    replicas: i32,
    pub user_keys: Vec<K>,
} 

impl<K: Hash + Ord, V: Evict + Clone> ConsistentHash<K, V> {
    pub fn new(replicas: i32) -> Result<ConsistentHash<K, V>, String> {
        if replicas <= 0 {
            return Err(String::from("replcia count must be greater than 0"));
        }
        
        Ok(ConsistentHash {
            ring: HashMap::new(),
            keys: Vec::new(),
            user_keys: Vec::new(),
            replicas,
        })
    }

    pub fn print_node(&self) where K: Display, V: Display {
        for key in self.keys.iter() {
            let value = self.ring.get(key).unwrap();
            println!("{}: {}", key, value);
        }
    }

    pub fn add_node(&mut self, key: K, value: V) -> Result<(), String> {
        for i in 0..self.replicas {
            let hash_key = key.hash(i);
            if self.ring.contains_key(&hash_key) {
                return Err(String::from("Key already in ring"));
            }
            self.ring.insert(hash_key, value.clone());
            match self.keys.binary_search(&hash_key) {
                Ok(_) => {} // element already in vector @ `pos` 
                Err(pos) => self.keys.insert(pos, hash_key),
            }
        }
        self.user_keys.push(key);
        Ok(())
    }

    pub fn get_node(&self, name: &K) -> Option<&V> {
        if let Some(key) = self.search_nearest(name.hash(0)) {
            return self.ring.get(&key);
        }
        None
    }

    pub fn get_mut_node(&mut self, name: &K) -> Option<&mut V> {
        if let Some(key) = self.search_nearest(name.hash(0)) {
            return self.ring.get_mut(&key);
        }
        None
    }

    pub fn delete_node(&mut self, name: &K) -> Result<(), String> {
        let hash_key = name.hash(0);
        let replica_value = self.ring.get(&hash_key).unwrap().clone().evict();

        for i in 0..self.replicas {
            let hash_key = &name.hash(i);
            match self.keys.binary_search(&hash_key) {
                Ok(pos) => self.keys.remove(pos),
                Err(_) => panic!("key not found in keys"),
            };

            if !self.ring.contains_key(&hash_key) {
                return Err(String::from("Key does not exist in ring"));
            }
            self.ring.remove(&hash_key).unwrap();
        }

        // Get nearest node for merge
        let new_node = self.get_mut_node(name).unwrap();

        new_node.merge(replica_value).unwrap();
        Ok(())
    }

    pub fn search_nearest(&self, name: u128) -> Option<u128> {
        if self.keys.is_empty() {
            return None;
        }

        if name > *self.keys.last().unwrap() {
            return Some(*self.keys.first().unwrap());
        }

        return match self.keys.binary_search(&name) {
            Ok(pos) => Some(self.keys[pos]),
            Err(pos) => {
                match self.keys.get(pos + 1) {
                    Some(value) => Some(*value),
                    None => Some(self.keys[0]),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::collections::HashMap;
    use super::*;
    use test::Bencher;

    #[test]
    fn test_string_md5_hash() {
        let hash = String::from("test").hash(0);
        assert_eq!(hash, 178633651610943467493091302425572625585);
    }

    #[test]
    fn new_consistent_hash() {
        let _: ConsistentHash<String,i32> = match ConsistentHash::new(10) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };
    }

    #[test]
    #[should_panic]
    fn fail_if_bad_replicas() {
        let _: ConsistentHash<String,i32> = match ConsistentHash::new(-20) {
            Ok(ring) => ring,
            Err(err) => panic!(err),
        };
    }

    #[test]
    fn test_delete_rebalance() {
        let mut ring: ConsistentHash<String, i32> = ConsistentHash::new(1).unwrap();
        
        for i in 1..=4 {
            let node_name = format!("node{}", i);
            let node_val = 12;
            match ring.add_node(node_name, node_val) {
                Err(err) => panic!(err),
                Ok(()) => (),
            };
        }

        ring.delete_node(&format!("node1")).unwrap();
        let new_value = ring.get_node(&format!("node1")).unwrap();

        assert_eq!(*new_value, 24);

    }

    #[test]
    fn property_based() {
        let mut ring: ConsistentHash<String,String> = match ConsistentHash::new(10000) {
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

            let mut count = match distributions.get(&*node) {
                Some(result) => *result,
                None => 0,
            };

            count += 1;
            distributions.insert(String::from(&*node), count);
        }

        assert_eq!(distributions.values().sum::<i32>(), num_hits);
        
        let min = distributions.values().min().unwrap();
        let max = distributions.values().max().unwrap();
        if (*max - *min) > 100 {
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