// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// We use `default` method a lot to be support prost and rust-protobuf at the
// same time. And reassignment can be optimized by compiler.
#![allow(clippy::field_reassign_with_default)]
use axum::extract::{Path, State};
use axum::{
    routing::{put, get},
    Json, Router,
};
use slog::Drain;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{str, thread};


use protobuf::Message as PbMessage;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use regex::Regex;
use serde_json::Value;

use serde::{Deserialize, Serialize};
use slog::{error, info, o};

#[derive(Serialize, Deserialize)]
struct DTO {
    key: String,
    value: Value,
}
#[derive(Debug, Clone)]
pub struct Database {
    pub proposals: Arc<Mutex<VecDeque<Proposal>>>,
    pub fetching: Arc<Mutex<HashMap<String, Value>>>,
}

impl Database {
    pub async fn init() -> Self {
        let proposals: Arc<Mutex<VecDeque<Proposal>>> =
            Arc::new(Mutex::new(VecDeque::<Proposal>::new()));
        let fetching = Arc::new(Mutex::new(HashMap::new()));
        Database {
            proposals,
            fetching,
        }
    }
    pub async fn create(&mut self, id: String, value: Value) -> bool {
        let (proposal, rx) = Proposal::normal("put".to_owned(), id, value);
        self.proposals.lock().unwrap().push_back(proposal);
        // After we got a response from `rx`, we can assume the put succeeded and following
        // `get` operations can find the key-value pair.
        rx.recv().unwrap()
    }

    pub async fn update(&mut self, id: String, value: Value) -> bool {
        let (proposal, rx) = Proposal::normal("update".to_owned(), id, value);
        self.proposals.lock().unwrap().push_back(proposal);
        // After we got a response from `rx`, we can assume the put succeeded and following
        // `get` operations can find the key-value pair.
        rx.recv().unwrap()
    }

    pub async fn delete(&mut self, id: String) -> bool {
        let (proposal, rx) = Proposal::normal("delete".to_owned(), id, Value::Null);
        self.proposals.lock().unwrap().push_back(proposal);
        // After we got a response from `rx`, we can assume the put succeeded and following
        // `get` operations can find the key-value pair.
        rx.recv().unwrap()
    }

    pub async fn fetch(&mut self, id: String) -> DTO {
        match self.fetching.lock().unwrap().get(&id) {
            Some(value) => DTO {
                key: id,
                value: value.clone(),
            },
            None => DTO {
                key: id,
                value: Value::Null,
            },
        }
    }
}

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::fmt::init();
    let db = Database::init().await;

    const NUM_NODES: u32 = 5;
    // Create 5 mailboxes to send/receive messages. Every node holds a `Receiver` to receive
    // messages from others, and uses the respective `Sender` to send messages to others.
    let (mut tx_vec, mut rx_vec) = (Vec::new(), Vec::new());
    for _ in 0..NUM_NODES {
        let (tx, rx) = mpsc::channel();
        tx_vec.push(tx);
        rx_vec.push(rx);
    }

    let (_tx_stop, rx_stop) = mpsc::channel();
    let rx_stop = Arc::new(Mutex::new(rx_stop));

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    let logger = slog::Logger::root(drain, o!());

    let mut handles = Vec::new();
    for (i, rx) in rx_vec.into_iter().enumerate() {
        // A map[peer_id -> sender]. In the example we create 5 nodes, with ids in [1, 5].
        let mailboxes = (1..6u64).zip(tx_vec.iter().cloned()).collect();
        let mut node = match i {
            // Peer 1 is the leader.
            0 => Node::create_raft_leader(1, rx, mailboxes, &logger, db.fetching.clone()),
            // Other peers are followers.
            _ => Node::create_raft_follower(rx, mailboxes),
        };
        let proposals = Arc::clone(&db.proposals);

        // Tick the raft node per 100ms. So use an `Instant` to trace it.
        let mut t = Instant::now();

        // Clone the stop receiver
        let rx_stop_clone = Arc::clone(&rx_stop);
        let logger = logger.clone();
        // Here we spawn the node on a new thread and keep a handle so we can join on them later.
        let handle = thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(10));
            loop {
                // Step raft messages.
                match node.my_mailbox.try_recv() {
                    Ok(msg) => node.step(msg, &logger),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            let raft_group = match node.raft_group {
                Some(ref mut r) => r,
                // When Node::raft_group is `None` it means the node is not initialized.
                _ => continue,
            };

            if t.elapsed() >= Duration::from_millis(100) {
                // Tick the raft.
                raft_group.tick();
                t = Instant::now();
            }

            // Let the leader pick pending PROPOSALS from the global queue.
            if raft_group.raft.state == StateRole::Leader {
                // Handle new PROPOSALS.
                let mut proposals = proposals.lock().unwrap();
                for p in proposals.iter_mut().skip_while(|p| p.proposed > 0) {
                    propose(raft_group, p);
                }
            }

            // Handle readies from the raft.
            on_ready(
                raft_group,
                node.kv_pairs.clone(),
                &node.mailboxes,
                &proposals,
                &logger,
            );

            // Check control signals from
            if check_signals(&rx_stop_clone) {
                return;
            };
        });
        handles.push(handle);
    }

    // Propose some conf changes so that followers can be initialized.
    add_all_followers(db.proposals.as_ref());

    let port = 49494; //rand::thread_rng().gen_range(49152..65535);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());

    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/:id", get(fetch_entry).delete(delete_entry))
        .route("/", put(create_entry).post(update_entry))
        .with_state(db);

    // run our app with hyper
    axum::serve(listener, app).await.unwrap();
}

async fn create_entry(mut db: State<Database>, Json(payload): Json<DTO>) -> Json<bool> {
    Json(db.create(payload.key, payload.value).await)
}
async fn update_entry(mut db: State<Database>, Json(payload): Json<DTO>) -> Json<bool> {
    Json(db.update(payload.key, payload.value).await)
}
async fn delete_entry(mut db: State<Database>, Path(id): Path<String>) -> Json<bool> {
    Json(db.delete(id).await)
}
async fn fetch_entry(mut db: State<Database>, Path(id): Path<String>) -> Json<DTO> {
    //String::from("your mom")
    Json(db.fetch(id).await)
}

enum Signal {
    Terminate,
}

fn check_signals(receiver: &Arc<Mutex<mpsc::Receiver<Signal>>>) -> bool {
    match receiver.lock().unwrap().try_recv() {
        Ok(Signal::Terminate) => true,
        Err(TryRecvError::Empty) => false,
        Err(TryRecvError::Disconnected) => true,
    }
}

struct Node {
    // None if the raft is not initialized.
    raft_group: Option<RawNode<MemStorage>>,
    my_mailbox: Receiver<Message>,
    mailboxes: HashMap<u64, Sender<Message>>,
    // Key-value pairs after applied. `MemStorage` only contains raft logs,
    // so we need an additional storage engine.
    kv_pairs: Arc<Mutex<HashMap<String, Value>>>,
}

impl Node {
    // Create a raft leader only with itself in its configuration.
    fn create_raft_leader(
        id: u64,
        my_mailbox: Receiver<Message>,
        mailboxes: HashMap<u64, Sender<Message>>,
        logger: &slog::Logger,
        kv_pairs: Arc<Mutex<HashMap<String, Value>>>,
    ) -> Self {
        let mut cfg = example_config();
        cfg.id = id;
        let logger = logger.new(o!("tag" => format!("peer_{}", id)));
        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];
        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();
        let raft_group = Some(RawNode::new(&cfg, storage, &logger).unwrap());
        Node {
            raft_group,
            my_mailbox,
            mailboxes,
            kv_pairs,
        }
    }

    // Create a raft follower.
    fn create_raft_follower(
        my_mailbox: Receiver<Message>,
        mailboxes: HashMap<u64, Sender<Message>>,
    ) -> Self {
        Node {
            raft_group: None,
            my_mailbox,
            mailboxes,
            kv_pairs: Default::default(),
        }
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message, logger: &slog::Logger) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.to;
        let logger = logger.new(o!("tag" => format!("peer_{}", msg.to)));
        let storage = MemStorage::new();
        self.raft_group = Some(RawNode::new(&cfg, storage, &logger).unwrap());
    }

    // Step a raft message, initialize the raft if need.
    fn step(&mut self, msg: Message, logger: &slog::Logger) {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg, logger);
            } else {
                return;
            }
        }
        let raft_group = self.raft_group.as_mut().unwrap();
        let _ = raft_group.step(msg);
    }
}

fn on_ready(
    raft_group: &mut RawNode<MemStorage>,
    kv_pairs: Arc<Mutex<HashMap<String, Value>>>,
    mailboxes: &HashMap<u64, Sender<Message>>,
    proposals: &Mutex<VecDeque<Proposal>>,
    logger: &slog::Logger,
) {
    if !raft_group.has_ready() {
        return;
    }
    let store = raft_group.raft.raft_log.store.clone();

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();

    let handle_messages = |msgs: Vec<Message>| {
        for msg in msgs {
            let to = msg.to;
            if mailboxes[&to].send(msg).is_err() {
                error!(
                    logger,
                    "send raft message to {} fail, let Raft retry it", to
                );
            }
        }
    };

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.
        handle_messages(ready.take_messages());
    }

    // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
    if *ready.snapshot() != Snapshot::default() {
        let s = ready.snapshot().clone();
        if let Err(e) = store.wl().apply_snapshot(s) {
            error!(
                logger,
                "apply snapshot fail: {:?}, need to retry or panic", e
            );
            return;
        }
    }

    let handle_committed_entries = |rn: &mut RawNode<MemStorage>, committed_entries: Vec<Entry>| {
        for entry in committed_entries {
            if entry.data.is_empty() {
                // From new elected leaders.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                // For conf change messages, make them effective.
                let mut cc = ConfChange::default();
                cc.merge_from_bytes(&entry.data).unwrap();
                let cs = rn.apply_conf_change(&cc).unwrap();
                store.wl().set_conf_state(cs);
            } else {
                // For normal PROPOSALS, extract the key-value pair and then
                // insert them into the kv engine.
                let data = str::from_utf8(&entry.data).unwrap();
                let reg = Regex::new("(\\w+) ([0-9]+) (.+)").unwrap();
                if let Some(caps) = reg.captures(data) {
                    match caps[1].to_string().as_str() {
                        "put" => {
                            info!(logger, "insert received");
                            kv_pairs
                                .lock()
                                .unwrap()
                                .insert(caps[2].parse().unwrap(), Value::String(caps[3].to_string()))
                        }
                        "update" => {
                            info!(logger, "update received");
                            kv_pairs
                                .lock()
                                .unwrap()
                                .insert(caps[2].parse().unwrap(), Value::String(caps[3].to_string()))
                        }
                        "delete" => {
                            info!(logger, "Value persisted");
                            kv_pairs
                                .lock()
                                .unwrap()
                                .remove(&caps[2].to_string())
                        }
                        matched @ _ => {
                            panic!("Unknown keyword '{}' received", matched);
                        }
                    };
                }
            }
            if rn.raft.state == StateRole::Leader {
                // The leader should response to the clients, tell them if their PROPOSALS
                // succeeded or not.
                let proposal = proposals.lock().unwrap().pop_front().unwrap();
                proposal.propose_success.send(true).unwrap();
            }
        }
    };
    // Apply all committed entries.
    handle_committed_entries(raft_group, ready.take_committed_entries());

    // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
    // raft logs to the latest position.
    if let Err(e) = store.wl().append(ready.entries()) {
        error!(
            logger,
            "persist raft log fail: {:?}, need to retry or panic", e
        );
        return;
    }

    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        store.wl().set_hardstate(hs.clone());
    }

    if !ready.persisted_messages().is_empty() {
        // Send out the persisted messages come from the node.
        handle_messages(ready.take_persisted_messages());
    }

    // Call `RawNode::advance` interface to update position flags in the raft.
    let mut light_rd = raft_group.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }
    // Send out the messages.
    handle_messages(light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(raft_group, light_rd.take_committed_entries());
    // Advance the apply index.
    raft_group.advance_apply();
}

fn example_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    }
}

// The message can be used to initialize a raft node or not.
fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)
}

#[derive(Debug)]
pub struct Proposal {
    normal: Option<(String, String, Value)>, // key is a String integer, and value is a json value.
    conf_change: Option<ConfChange>,       // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    proposed: u64,
    propose_success: SyncSender<bool>,
}

impl Proposal {
    fn conf_change(cc: &ConfChange) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }

    fn normal(command: String, key: String, value: Value) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: Some((command, key, value)),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }
}

fn propose(raft_group: &mut RawNode<MemStorage>, proposal: &mut Proposal) {
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some((ref command, ref key, ref value)) = proposal.normal {
        let data = format!("{} {} {}", command, key, value).into_bytes();
        let _ = raft_group.propose(vec![], data);
    } else if let Some(ref cc) = proposal.conf_change {
        let _ = raft_group.propose_conf_change(vec![], cc.clone());
    } else if let Some(_transferee) = proposal.transfer_leader {
        // TODO: implement transfer leader.
        unimplemented!();
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    if last_index2 == last_index1 {
        // Propose failed, don't forget to respond to the client.
        proposal.propose_success.send(false).unwrap();
    } else {
        proposal.proposed = last_index1;
    }
}

// Proposes some conf change for peers [2, 5].
fn add_all_followers(proposals: &Mutex<VecDeque<Proposal>>) {
    for i in 2..6u64 {
        let mut conf_change = ConfChange::default();
        conf_change.node_id = i;
        conf_change.set_change_type(ConfChangeType::AddNode);
        loop {
            let (proposal, rx) = Proposal::conf_change(&conf_change);
            proposals.lock().unwrap().push_back(proposal);
            if rx.recv().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}
