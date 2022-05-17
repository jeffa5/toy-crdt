use std::collections::{BTreeSet, HashSet};

use stateright::actor::Id;

use crate::map::Map;

use crate::map::Timestamp;

impl Map for FixedMap {
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
pub(crate) struct FixedMap {
    actor_id: Id,
    max_op: u32,
    pub(crate) values: BTreeSet<(Timestamp, char, char)>,
}

impl FixedMap {
    pub(crate) fn new(actor_id: Id) -> Self {
        Self {
            actor_id,
            max_op: 0,
            values: BTreeSet::new(),
        }
    }

    pub(crate) fn get(&self, key: &char) -> Option<&char> {
        let big_t = self
            .values
            .iter()
            .filter_map(|(t, k, _)| if k == key { Some(t) } else { None })
            .collect::<Vec<_>>();

        if big_t.is_empty() {
            None
        } else {
            let max_t = big_t.iter().max().unwrap();
            self.values
                .iter()
                .find(|(t, kp, _)| key == kp && &t == max_t)
                .map(|(_, _, v)| v)
        }
    }

    pub(crate) fn set(&mut self, key: char, value: char) -> (Vec<Timestamp>, Timestamp) {
        let big_t = self
            .values
            .iter()
            .filter_map(|(t, k, _)| if k == &key { Some(t) } else { None })
            .cloned()
            .collect::<Vec<_>>();

        let t = self.new_timestamp();

        // retain all values that aren't in the context
        self.values.retain(|(t, _k, _v)| !big_t.contains(t));
        // then insert the new one
        self.values.insert((t, key, value));

        (big_t, t)
    }

    pub(crate) fn delete(&mut self, key: &char) -> Option<Vec<Timestamp>> {
        let big_t = self
            .values
            .iter()
            .filter_map(|(t, k, _)| if k == key { Some(t) } else { None })
            .cloned()
            .collect::<Vec<_>>();

        // retain all values that aren't in the context
        self.values.retain(|(t, _k, _v)| !big_t.contains(t));
        Some(big_t)
    }

    pub(crate) fn receive_set(
        &mut self,
        context: Vec<Timestamp>,
        timestamp: Timestamp,
        key: char,
        value: char,
    ) {
        self.update_max_op(timestamp);

        // retain all values that aren't in the context
        self.values.retain(|(t, _k, _v)| !context.contains(t));
        // then insert the new one
        self.values.insert((timestamp, key, value));
    }

    pub(crate) fn receive_delete(&mut self, context: Vec<Timestamp>) {
        if let Some(t) = context.iter().max() {
            self.update_max_op(*t)
        }

        // retain all values that aren't in the context
        self.values.retain(|(t, _k, _v)| !context.contains(t));
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
