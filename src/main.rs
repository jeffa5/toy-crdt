use clap::Parser;
use stateright::actor::model_peers;
use stateright::actor::Actor;
use stateright::actor::ActorModel;
use stateright::actor::Network;
use stateright::actor::Out;
use stateright::Checker;
use stateright::{actor::Id, Model};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

type Timestamp = (u32, usize);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Map {
    actor_id: Id,
    max_op: u32,
    values: BTreeSet<(Timestamp, char, char)>,
}

impl Map {
    fn new(actor_id: Id) -> Self {
        Self {
            actor_id,
            max_op: 0,
            values: BTreeSet::new(),
        }
    }

    fn get(&self, k: &char) -> Option<&char> {
        self.values
            .iter()
            .find(|(_, kp, _)| k == kp)
            .map(|(_, _, v)| v)
    }

    fn set(&mut self, key: char, v: char) -> Timestamp {
        let t = self.new_timestamp();
        // remove the old value from ourselves if there was one
        if let Some(previous) = self.values.iter().find(|(_t, k, _v)| k == &key).cloned() {
            self.values.remove(&previous);
        }
        // add it to ourselves
        self.values.insert((t, key, v));
        t
    }

    fn delete(&mut self, key: &char) -> Option<Timestamp> {
        if let Some((t, k, v)) = self.values.iter().find(|(_, kp, _)| key == kp).cloned() {
            // add it to ourselves
            self.values.remove(&(t, k, v));
            Some(t)
        } else {
            None
        }
    }

    fn receive_set(&mut self, timestamp: Timestamp, key: char, value: char) {
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

    fn receive_delete(&mut self, timestamp: Timestamp) {
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Peer {
    peers: Vec<Id>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum PeerMsg {
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
    type Msg = MyRegisterMsg;

    type State = Map;

    fn on_start(&self, id: Id, _o: &mut Out<Self>) -> Self::State {
        Self::State::new(id)
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut std::borrow::Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        match msg {
            MyRegisterMsg::Put(id, value) => {
                let key = 'b';
                // apply the op locally
                let timestamp = state.to_mut().set(key, value);

                o.send(src, MyRegisterMsg::PutOk(id));

                o.broadcast(
                    &self.peers,
                    &MyRegisterMsg::Internal(PeerMsg::PutSync {
                        timestamp,
                        key,
                        value,
                    }),
                )
            }
            MyRegisterMsg::Get(id) => {
                if let Some(value) = state.get(&'b') {
                    o.send(src, MyRegisterMsg::GetOk(id, *value))
                }
            }
            MyRegisterMsg::Delete(id) => {
                let key = 'b';
                // apply the op locally
                let timestamp = state.to_mut().delete(&key);

                o.send(src, MyRegisterMsg::DeleteOk(id));

                if let Some(timestamp) = timestamp {
                    o.broadcast(
                        &self.peers,
                        &MyRegisterMsg::Internal(PeerMsg::DeleteSync { timestamp }),
                    )
                }
            }
            MyRegisterMsg::Internal(PeerMsg::PutSync {
                timestamp,
                key,
                value,
            }) => state.to_mut().receive_set(timestamp, key, value),
            MyRegisterMsg::Internal(PeerMsg::DeleteSync { timestamp }) => {
                state.to_mut().receive_delete(timestamp)
            }
            MyRegisterMsg::PutOk(_id) => {}
            MyRegisterMsg::GetOk(_id, _value) => {}
            MyRegisterMsg::DeleteOk(_id) => {}
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MyRegisterActor {
    Client {
        put_count: usize,
        delete_count: usize,
        server_count: usize,
    },
    Server(Peer),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum MyRegisterActorState {
    Client {
        awaiting: Option<RequestId>,
        op_count: usize,
    },
    Server(<Peer as Actor>::State),
}

type RequestId = usize;
type Value = char;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum MyRegisterMsg {
    /// A message specific to the register system's internal protocol.
    Internal(PeerMsg),

    /// Indicates that a value should be written.
    Put(RequestId, Value),
    /// Indicates that a value should be retrieved.
    Get(RequestId),
    /// Indicates that a value should be deleted.
    Delete(RequestId),

    /// Indicates a successful `Put`. Analogous to an HTTP 2XX.
    PutOk(RequestId),
    /// Indicates a successful `Get`. Analogous to an HTTP 2XX.
    GetOk(RequestId, Value),
    /// Indicates a successful `Delete`. Analogous to an HTTP 2XX.
    DeleteOk(RequestId),
}

impl Actor for MyRegisterActor {
    type Msg = MyRegisterMsg;

    type State = MyRegisterActorState;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        match self {
            MyRegisterActor::Client {
                put_count,
                delete_count,
                server_count,
            } => {
                let server_count = *server_count;

                let index: usize = id.into();
                if index < server_count {
                    panic!("MyRegisterActor clients must be added to the model after servers.");
                }

                if *put_count > 0 {
                    let unique_request_id = 1 * index; // next will be 2 * index
                    let value = (b'A' + (index - server_count) as u8) as char;
                    o.send(
                        Id::from((index + 0) % server_count),
                        MyRegisterMsg::Put(unique_request_id, value),
                    );
                    MyRegisterActorState::Client {
                        awaiting: Some(unique_request_id),
                        op_count: 1,
                    }
                } else if *delete_count > 0 {
                    let unique_request_id = 1 * index; // next will be 2 * index
                    o.send(
                        Id::from((index + 0) % server_count),
                        MyRegisterMsg::Delete(unique_request_id),
                    );
                    MyRegisterActorState::Client {
                        awaiting: Some(unique_request_id),
                        op_count: 1,
                    }
                } else {
                    MyRegisterActorState::Client {
                        awaiting: None,
                        op_count: 0,
                    }
                }
            }
            MyRegisterActor::Server(server_actor) => {
                let mut server_out = Out::new();
                let state =
                    MyRegisterActorState::Server(server_actor.on_start(id, &mut server_out));
                o.append(&mut server_out);
                state
            }
        }
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        use MyRegisterActor as A;
        use MyRegisterActorState as S;

        match (self, &**state) {
            (
                A::Client {
                    put_count,
                    delete_count,
                    server_count,
                },
                S::Client {
                    awaiting: Some(awaiting),
                    op_count,
                },
            ) => {
                let server_count = *server_count;
                match msg {
                    MyRegisterMsg::PutOk(request_id) if &request_id == awaiting => {
                        let index: usize = id.into();
                        let unique_request_id = (op_count + 1) * index;
                        if *op_count < *put_count {
                            let value = (b'Z' - (index - server_count) as u8) as char;
                            o.send(
                                Id::from((index + op_count) % server_count),
                                MyRegisterMsg::Put(unique_request_id, value),
                            );
                        } else {
                            o.send(
                                Id::from((index + op_count) % server_count),
                                MyRegisterMsg::Get(unique_request_id),
                            );
                        }
                        *state = Cow::Owned(MyRegisterActorState::Client {
                            awaiting: Some(unique_request_id),
                            op_count: op_count + 1,
                        });
                    }
                    MyRegisterMsg::GetOk(request_id, _value) if &request_id == awaiting => {
                        // done with puts but we may still want to run some deletes
                        if *delete_count > 0 {
                            let index: usize = id.into();
                            let unique_request_id = (op_count + 1) * index;
                            if *op_count < *delete_count {
                                o.send(
                                    Id::from((index + op_count) % server_count),
                                    MyRegisterMsg::Delete(unique_request_id),
                                );
                            } else {
                                o.send(
                                    Id::from((index + op_count) % server_count),
                                    MyRegisterMsg::Get(unique_request_id),
                                );
                            }
                            *state = Cow::Owned(MyRegisterActorState::Client {
                                awaiting: Some(unique_request_id),
                                op_count: op_count + 1,
                            });
                        } else {
                            *state = Cow::Owned(MyRegisterActorState::Client {
                                awaiting: None,
                                op_count: op_count + 1,
                            });
                        }
                    }
                    MyRegisterMsg::DeleteOk(request_id) if &request_id == awaiting => {
                        let index: usize = id.into();
                        let unique_request_id = (op_count + 1) * index;
                        if *op_count < *delete_count {
                            o.send(
                                Id::from((index + op_count) % server_count),
                                MyRegisterMsg::Delete(unique_request_id),
                            );
                        } else {
                            o.send(
                                Id::from((index + op_count) % server_count),
                                MyRegisterMsg::Get(unique_request_id),
                            );
                        }
                        *state = Cow::Owned(MyRegisterActorState::Client {
                            awaiting: Some(unique_request_id),
                            op_count: op_count + 1,
                        });
                    }
                    MyRegisterMsg::PutOk(_) => {}
                    MyRegisterMsg::GetOk(_, _) => {}
                    MyRegisterMsg::DeleteOk(_) => {}
                    MyRegisterMsg::Put(_, _) => {}
                    MyRegisterMsg::Get(_) => {}
                    MyRegisterMsg::Delete(_) => {}
                    MyRegisterMsg::Internal(_) => {}
                }
            }
            (A::Server(server_actor), S::Server(server_state)) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();
                server_actor.on_msg(id, &mut server_state, src, msg, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(MyRegisterActorState::Server(server_state))
                }
                o.append(&mut server_out);
            }
            (A::Server(_), S::Client { .. }) => {}
            (A::Client { .. }, S::Server(_)) => {}
            (
                A::Client {
                    put_count: _,
                    delete_count: _,
                    server_count: _,
                },
                S::Client {
                    awaiting: None,
                    op_count: _,
                },
            ) => {}
        }
    }

    fn on_timeout(&self, id: Id, state: &mut Cow<Self::State>, o: &mut Out<Self>) {
        use MyRegisterActor as A;
        use MyRegisterActorState as S;
        match (self, &**state) {
            (A::Client { .. }, S::Client { .. }) => {}
            (A::Server(server_actor), S::Server(server_state)) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();
                server_actor.on_timeout(id, &mut server_state, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(MyRegisterActorState::Server(server_state))
                }
                o.append(&mut server_out);
            }
            (A::Server(_), S::Client { .. }) => {}
            (A::Client { .. }, S::Server(_)) => {}
        }
    }
}

struct ModelCfg {
    clients: usize,
    servers: usize,
}

impl ModelCfg {
    fn into_actor_model(self) -> ActorModel<MyRegisterActor, (), ()> {
        let mut model = ActorModel::new((), ());
        for i in 0..self.servers {
            model = model.actor(MyRegisterActor::Server(Peer {
                peers: model_peers(i, self.servers),
            }))
        }

        for _ in 0..self.clients {
            model = model.actor(MyRegisterActor::Client {
                put_count: 1,
                delete_count: 1,
                server_count: self.servers,
            })
        }

        model
            .property(
                stateright::Expectation::Eventually,
                "all actors have the same value for all keys",
                |_, state| all_same_state(&state.actor_states),
            )
            .property(
                stateright::Expectation::Always,
                "only have one value for each key",
                |_, state| only_one_of_each_key(&state.actor_states),
            )
            .init_network(Network::new_ordered(vec![]))
    }
}

fn all_same_state(actors: &[Arc<MyRegisterActorState>]) -> bool {
    actors.windows(2).all(|w| match (&*w[0], &*w[1]) {
        (MyRegisterActorState::Client { .. }, MyRegisterActorState::Client { .. }) => true,
        (MyRegisterActorState::Client { .. }, MyRegisterActorState::Server(_)) => true,
        (MyRegisterActorState::Server(_), MyRegisterActorState::Client { .. }) => true,
        (MyRegisterActorState::Server(a), MyRegisterActorState::Server(b)) => a.values == b.values,
    })
}

fn only_one_of_each_key(actors: &[Arc<MyRegisterActorState>]) -> bool {
    for actor in actors {
        if let MyRegisterActorState::Server(actor) = &**actor {
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

#[derive(Parser)]
struct Opts {
    #[clap(subcommand)]
    command: SubCmd,

    #[clap(long, short, global = true, default_value = "2")]
    clients: usize,

    #[clap(long, short, global = true, default_value = "2")]
    servers: usize,
}

#[derive(clap::Subcommand)]
enum SubCmd {
    Serve,
    Check,
}

fn main() {
    let opts = Opts::parse();

    let model = ModelCfg {
        clients: opts.clients,
        servers: opts.servers,
    }
    .into_actor_model()
    .checker()
    .threads(num_cpus::get());

    match opts.command {
        SubCmd::Serve => {
            println!("Serving web ui on http://127.0.0.1:8080");
            model.serve("127.0.0.1:8080");
        }
        SubCmd::Check => {
            model
                .spawn_dfs()
                .report(&mut std::io::stdout())
                .join()
                .assert_properties();
        }
    }
}
