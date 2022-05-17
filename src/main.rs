use stateright::actor::register::RegisterActor;
use stateright::actor::register::RegisterActorState;
use stateright::actor::register::RegisterMsg;
use stateright::actor::Actor;
use stateright::actor::ActorModel;
use stateright::actor::Network;
use stateright::actor::Out;
use stateright::{actor::Id, Model};
use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

pub type Timestamp = (u32, usize);

#[derive(Debug, Clone)]
pub enum Op {
    Set {
        timestamp: Timestamp,
        key: char,
        value: char,
    },
    Delete {
        timestamp: Timestamp,
    },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TC {
    actor_id: Id,
    max_op: u32,
    values: BTreeSet<(Timestamp, char, char)>,
}

impl TC {
    pub fn new(actor_id: Id) -> Self {
        Self {
            actor_id,
            max_op: 0,
            values: BTreeSet::new(),
        }
    }

    pub fn get(&self, k: &char) -> Option<&char> {
        self.values
            .iter()
            .find(|(_, kp, _)| k == kp)
            .map(|(_, _, v)| v)
    }

    pub fn set(&mut self, key: char, v: char) -> Timestamp {
        let t = self.new_timestamp();
        // remove the old value from ourselves if there was one
        if let Some(previous) = self.values.iter().find(|(_t, k, _v)| k == &key).cloned() {
            self.values.remove(&previous);
        }
        // add it to ourselves
        self.values.insert((t, key, v));
        t
    }

    pub fn delete(&mut self, key: &char) -> Option<Timestamp> {
        if let Some((t, k, v)) = self.values.iter().find(|(_, kp, _)| key == kp).cloned() {
            // add it to ourselves
            self.values.remove(&(t, k, v));
            Some(t)
        } else {
            None
        }
    }

    pub fn receive_set(&mut self, timestamp: Timestamp, key: char, value: char) {
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

    pub fn receive_delete(&mut self, timestamp: Timestamp) {
        self.update_max_op(timestamp);
        if let Some(tuple) = self
            .values
            .iter()
            .find(|(t, _k, _v)| t == &timestamp)
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

pub struct Peer {
    peers: Vec<Id>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum PeerMsg {
    PutSync {
        timestamp: Timestamp,
        key: char,
        value: char,
    },
    DeleteSync {
        timestamp: Timestamp,
    },
}

impl Actor for Peer {
    type Msg = RegisterMsg<u64, char, PeerMsg>;

    type State = TC;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        Self::State::new(id)
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut std::borrow::Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        match msg {
            RegisterMsg::Put(id, value) => {
                let key = 'b';
                // apply the op locally
                let timestamp = state.to_mut().set(key.clone(), value.clone());

                o.broadcast(
                    &self.peers,
                    &RegisterMsg::Internal(PeerMsg::PutSync {
                        timestamp,
                        key,
                        value,
                    }),
                )
            }
            RegisterMsg::Get(id) => {

                if let Some(value) =
            state.get(&'b') {
                o.send(src, RegisterMsg::GetOk(id, *value))
                }

            }
            ,
            // RegisterMsg::Delete { key } => {
            //     // apply the op locally
            //     let timestamp = state.to_mut().delete(&key);

            //     if let Some(timestamp) = timestamp {
            //         o.broadcast(&self.peers, &RegisterMsg::DeleteSync { timestamp })
            //     }
            // }
            RegisterMsg::Internal(PeerMsg::PutSync {
                timestamp,
                key,
                value,
            }) => state.to_mut().receive_set(timestamp, key, value),
            RegisterMsg::Internal(PeerMsg::DeleteSync { timestamp }) => state.to_mut().receive_delete(timestamp),
            RegisterMsg::PutOk(_id) => {}
            RegisterMsg::GetOk(_id, _value) => {}
        }
    }
}

fn main() {
    let checker = ActorModel::new((), ())
        .actor(RegisterActor::Server(Peer {
            peers: vec![Id::from(1)],
        }))
        .actor(RegisterActor::Server(Peer {
            peers: vec![Id::from(0)],
        }))
        .actor(RegisterActor::Client {
            put_count: 5,
            server_count: 2,
        })
        .actor(RegisterActor::Client {
            put_count: 5,
            server_count: 2,
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
        .property(
            stateright::Expectation::Always,
            "only have one value for each key",
            |_, state| only_one_of_each_key(&state.actor_states),
        )
        .init_network(Network::new_ordered(vec![]))
        .checker()
        .serve("127.0.0.1:8080");
}

fn all_same_state(actors: &[Arc<RegisterActorState<TC, u64>>]) -> bool {
    actors.windows(2).all(|w| match (&*w[0], &*w[1]) {
        (RegisterActorState::Client { .. }, RegisterActorState::Client { .. }) => true,
        (RegisterActorState::Client { .. }, RegisterActorState::Server(_)) => true,
        (RegisterActorState::Server(_), RegisterActorState::Client { .. }) => true,
        (RegisterActorState::Server(a), RegisterActorState::Server(b)) => a.values == b.values,
    })
}

fn only_one_of_each_key(actors: &[Arc<RegisterActorState<TC, u64>>]) -> bool {
    for actor in actors {
        if let RegisterActorState::Server(actor) = &**actor {
            let keys = actor
                .values
                .iter()
                .map(|(_, k, _)| k)
                .collect::<HashSet<_>>();
            if keys.len() != actor.values.len() {
                return false;
            }
        }
    }
    true
}
