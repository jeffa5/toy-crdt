use std::collections::{BTreeSet, HashSet};

use stateright::actor::Id;

use crate::map::Map;
use crate::map::Timestamp;

impl Map for BrokenMap {
    fn new(actor_id: Id) -> Self {
        Self::new(actor_id)
    }

    fn get(&self, k: &char) -> Option<&char> {
        self.get(k)
    }

    fn set(&mut self, key: char, v: char) -> (Vec<Timestamp>, Timestamp) {
        self.set(key, v)
    }

    fn delete(&mut self, key: &char) -> Option<Vec<Timestamp>> {
        self.delete(key)
    }

    fn receive_set(
        &mut self,
        context: Vec<Timestamp>,
        timestamp: Timestamp,
        key: char,
        value: char,
    ) {
        self.receive_set(context, timestamp, key, value)
    }

    fn receive_delete(&mut self, context: Vec<Timestamp>) {
        self.receive_delete(context)
    }

    fn values(&self) -> Vec<(Timestamp, char, char)> {
        self.values.iter().cloned().collect()
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct BrokenMap {
    actor_id: Id,
    max_op: u32,
    pub(crate) values: BTreeSet<(Timestamp, char, char)>,
}

impl BrokenMap {
    pub(crate) fn new(actor_id: Id) -> Self {
        Self {
            actor_id,
            max_op: 0,
            values: BTreeSet::new(),
        }
    }

    pub(crate) fn get(&self, k: &char) -> Option<&char> {
        self.values
            .iter()
            .find(|(_, kp, _)| k == kp)
            .map(|(_, _, v)| v)
    }

    pub(crate) fn set(&mut self, key: char, v: char) -> (Vec<Timestamp>, Timestamp) {
        let t = self.new_timestamp();
        // remove the old value from ourselves if there was one
        if let Some(previous) = self.values.iter().find(|(_t, k, _v)| k == &key).cloned() {
            self.values.remove(&previous);
        }
        // add it to ourselves
        self.values.insert((t, key, v));
        (vec![], t)
    }

    pub(crate) fn delete(&mut self, key: &char) -> Option<Vec<Timestamp>> {
        if let Some((t, k, v)) = self.values.iter().find(|(_, kp, _)| key == kp).cloned() {
            // add it to ourselves
            self.values.remove(&(t, k, v));
            Some(vec![t])
        } else {
            None
        }
    }

    pub(crate) fn receive_set(
        &mut self,
        _context: Vec<Timestamp>,
        timestamp: Timestamp,
        key: char,
        value: char,
    ) {
        self.update_max_op(timestamp);
        let previous = self
            .values
            .iter()
            .filter(|(_t, k, _v)| k == &key)
            .cloned()
            .collect::<HashSet<_>>();

        if previous.is_empty() || previous.iter().all(|(t, _k, _v)| t < &timestamp) {
            for p in previous {
                self.values.remove(&p);
            }
            self.values.insert((timestamp, key, value));
        }
    }

    pub(crate) fn receive_delete(&mut self, timestamp: Vec<Timestamp>) {
        self.update_max_op(timestamp[0]);
        if let Some(tuple) = self
            .values
            .iter()
            .find(|(t, _k, _v)| t == &timestamp[0])
            .cloned()
        {
            self.values.remove(&tuple);
        }
    }

    fn update_max_op(&mut self, timestamp: Timestamp) {
        self.max_op = std::cmp::max(self.max_op, timestamp.0);
    }

    // globally unique
    fn new_timestamp(&mut self) -> Timestamp {
        self.max_op += 1;
        let id: usize = self.actor_id.into();
        (self.max_op, id)
    }
}
