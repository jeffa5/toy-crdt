use stateright::actor::Envelope;
use stateright::{actor::Id, Model};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

pub type Timestamp = (usize, u32);

#[derive(Debug, Clone)]
pub enum Op {
    Set {
        timestamp: Timestamp,
        key: String,
        value: String,
    },
    Delete {
        timestamp: Timestamp,
    },
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct TC {
    actor_id: Id,
    counter: u32,
    values: BTreeSet<(Timestamp, String, String)>,
}

impl TC {
    pub fn new(actor_id: Id) -> Self {
        Self {
            actor_id,
            counter: 0,
            values: BTreeSet::new(),
        }
    }

    pub fn get(&self, k: &str) -> Option<&String> {
        self.values
            .iter()
            .find(|(_, kp, _)| k == kp)
            .map(|(_, _, v)| v)
    }

    pub fn set(&mut self, k: String, v: String) -> Timestamp {
        let t = self.new_timestamp();
        // add it to ourselves
        self.values.insert((t, k, v));
        t
    }

    pub fn delete(&mut self, key: &str) -> Option<Timestamp> {
        if let Some((t, k, v)) = self.values.iter().find(|(_, kp, _)| key == kp).cloned() {
            // add it to ourselves
            self.values.remove(&(t, k, v));
            Some(t)
        } else {
            None
        }
    }

    pub fn receive_set(&mut self, timestamp: Timestamp, key: String, value: String) {
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

    pub fn receive_delete(&mut self, timestamp: Timestamp) {
        if let Some(tuple) = self
            .values
            .iter()
            .find(|(t, _k, _v)| t == &timestamp)
            .cloned()
        {
            self.values.remove(&tuple);
        }
    }

    // globally unique
    fn new_timestamp(&mut self) -> Timestamp {
        self.counter += 1;
        let id: usize = self.actor_id.into();
        (id, self.counter)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Msg {
    SetFromClient {
        key: String,
        value: String,
    },
    SetFromPeer {
        timestamp: Timestamp,
        key: String,
        value: String,
    },
    DeleteFromClient {
        key: String,
    },
    DeleteFromPeer {
        timestamp: Timestamp,
    },
}

pub struct Peer {
    peers: Vec<Id>,
}

impl stateright::actor::Actor for Peer {
    type Msg = Msg;

    type State = TC;

    fn on_start(&self, id: Id, o: &mut stateright::actor::Out<Self>) -> Self::State {
        Self::State::new(id)
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut std::borrow::Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut stateright::actor::Out<Self>,
    ) {
        match msg {
            Msg::SetFromClient { key, value } => {
                // apply the op locally
                let timestamp = state.to_mut().set(key.clone(), value.clone());

                o.broadcast(
                    &self.peers,
                    &Msg::SetFromPeer {
                        timestamp,
                        key,
                        value,
                    },
                )
            }
            Msg::DeleteFromClient { key } => {
                // apply the op locally
                let timestamp = state.to_mut().delete(&key);

                if let Some(timestamp) = timestamp {
                    o.broadcast(&self.peers, &Msg::DeleteFromPeer { timestamp })
                }
            }
            Msg::SetFromPeer {
                timestamp,
                key,
                value,
            } => state.to_mut().receive_set(timestamp, key, value),
            Msg::DeleteFromPeer { timestamp } => state.to_mut().receive_delete(timestamp),
        }
    }
}

fn main() {
    let checker = stateright::actor::ActorModel::new((), ())
        .actor(Peer {
            peers: vec![Id::from(1)],
        })
        .actor(Peer {
            peers: vec![Id::from(0)],
        })
        .property(
            stateright::Expectation::Eventually,
            "all actors have the same value for all keys",
            |_, state| all_same_state(&state.actor_states),
        )
        .property(
            stateright::Expectation::Always,
            "all actors have the same value for all keys",
            |_, state| all_same_state(&state.actor_states),
        )
        .init_network(stateright::actor::Network::new_ordered(vec![
            Envelope {
                src: Id::from(2),
                dst: Id::from(1),
                msg: Msg::SetFromClient {
                    key: "blah".to_owned(),
                    value: "bloop".to_owned(),
                },
            },
            Envelope {
                src: Id::from(2),
                dst: Id::from(1),
                msg: Msg::DeleteFromClient {
                    key: "blah".to_owned(),
                },
            },
        ]))
        .checker()
        .serve("127.0.0.1:8080");
}

fn all_same_state(actors: &[Arc<TC>]) -> bool {
    actors.windows(2).all(|w| w[0].values == w[1].values)
}
