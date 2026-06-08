use burst_communication_middleware::{Middleware, MiddlewareActorHandle};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const ROOT_WORKER: u32 = 0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Input {
    pub rows: usize,
    pub cols: usize,
    pub num_threads: u32,
    pub source: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Output {
    pub worker_id: u32,
    pub timestamps: Vec<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timestamp {
    key: String,
    value: String,
}

fn timestamp(key: String) -> Timestamp {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH).unwrap();
    let milliseconds_timestamp = duration_since_epoch.as_millis();
    Timestamp {
        key,
        value: milliseconds_timestamp.to_string(),
    }
}

pub struct Graph {
    pub adj: Vec<Vec<usize>>,
}

impl Graph {
    pub fn new_grid(rows: usize, cols: usize) -> Self {
        let mut adj = vec![vec![]; rows * cols];
        for r in 0..rows {
            for c in 0..cols {
                let u = r * cols + c;
                if r > 0 {
                    adj[u].push((r - 1) * cols + c);
                }
                if r < rows - 1 {
                    adj[u].push((r + 1) * cols + c);
                }
                if c > 0 {
                    adj[u].push(r * cols + c - 1);
                }
                if c < cols - 1 {
                    adj[u].push(r * cols + c + 1);
                }
            }
        }
        Graph { adj }
    }
}

#[derive(Clone, Debug)]
pub enum BfsMessage {
    Node(usize),
    Flush,
    WorkStatus(u32),
}

impl From<Bytes> for BfsMessage {
    fn from(bytes: Bytes) -> Self {
        if bytes.len() == 1 && bytes[0] == 0xFF {
            return BfsMessage::Flush;
        } else if bytes.len() == 5 && bytes[0] == 0xFE {
            let work = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
            return BfsMessage::WorkStatus(work);
        }
        let node = usize::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        BfsMessage::Node(node)
    }
}

impl From<BfsMessage> for Bytes {
    fn from(msg: BfsMessage) -> Self {
        match msg {
            BfsMessage::Node(node) => Bytes::copy_from_slice(&node.to_be_bytes()),
            BfsMessage::Flush => Bytes::from_static(&[0xFF]),
            BfsMessage::WorkStatus(work) => {
                let mut bytes = Vec::with_capacity(5);
                bytes.push(0xFE);
                bytes.extend_from_slice(&work.to_be_bytes());
                Bytes::from(bytes)
            }
        }
    }
}

fn bfs(params: Input, actor: &MiddlewareActorHandle<BfsMessage>) -> Output {
    let mut timestamps = Vec::new();
    timestamps.push(timestamp("worker_start".to_string()));

    let worker_id = actor.info.worker_id;
    let num_threads = params.num_threads;
    let num_nodes = params.rows * params.cols;

    let graph = Graph::new_grid(params.rows, params.cols);
    timestamps.push(timestamp("graph_generated".to_string()));

    let distances: Vec<AtomicUsize> = (0..num_nodes).map(|_| AtomicUsize::new(usize::MAX)).collect();
    distances[params.source].store(0, Ordering::Relaxed);

    let mut current_frontier: Vec<usize> = Vec::new();
    let mut next_frontier: Vec<usize> = Vec::new();
    let mut current_level = 0;

    if params.source as u32 % num_threads == worker_id {
        current_frontier.push(params.source);
    }

    loop {
        // ---------------------------------------------------------
        // Phase 1: Compute & Send
        // ---------------------------------------------------------
        for &u in &current_frontier {
            for &v in &graph.adj[u] {
                let owner = (v as u32) % num_threads;
                if owner == worker_id {
                    if distances[v].load(Ordering::Relaxed) == usize::MAX {
                        distances[v].store(current_level + 1, Ordering::Relaxed);
                        next_frontier.push(v);
                    }
                } else {
                    actor.send(owner, BfsMessage::Node(v)).unwrap();
                }
            }
        }
        current_frontier.clear();

        // Send Flush token to all other workers to mark end of sending
        for other in 0..num_threads {
            if other != worker_id {
                actor.send(other, BfsMessage::Flush).unwrap();
            }
        }

        // ---------------------------------------------------------
        // Phase 2: Receive until we get Flush from all other workers
        // ---------------------------------------------------------
        let mut flushes_received = 0;
        while flushes_received < num_threads - 1 {
            for other in 0..num_threads {
                if other == worker_id {
                    continue;
                }

                loop {
                    let msg = actor.recv(other).unwrap();
                    match msg {
                        BfsMessage::Node(v) => {
                            if distances[v].load(Ordering::Relaxed) == usize::MAX {
                                distances[v].store(current_level + 1, Ordering::Relaxed);
                                next_frontier.push(v);
                            }
                        }
                        BfsMessage::Flush => {
                            flushes_received += 1;
                            break; // Go to next worker
                        }
                        _ => panic!("Unexpected message type during Phase 2"),
                    }
                }
            }
        }

        timestamps.push(timestamp(format!("iter_{}_sync", current_level)));

        // ---------------------------------------------------------
        // Phase 3: Termination Check via Middleware Reduce
        // ---------------------------------------------------------
        let has_work = if next_frontier.is_empty() { 0 } else { 1 };

        let reduced_status_opt = actor
            .reduce(BfsMessage::WorkStatus(has_work), |a, b| {
                if let (BfsMessage::WorkStatus(wa), BfsMessage::WorkStatus(wb)) = (a, b) {
                    BfsMessage::WorkStatus(wa + wb)
                } else {
                    panic!("Reduce operation received invalid message types");
                }
            })
            .unwrap();

        // `reduce` returns Some on the root worker (worker 0 usually, based on tree reduction logic)
        let should_terminate;
        if worker_id == ROOT_WORKER {
            let final_status = reduced_status_opt.unwrap();
            if let BfsMessage::WorkStatus(work) = final_status {
                should_terminate = work == 0;
                let signal = BfsMessage::WorkStatus(if should_terminate { 0 } else { 1 });
                actor.broadcast(Some(signal), ROOT_WORKER).unwrap();
            } else {
                panic!("Invalid reduce output");
            }
        } else {
            let bcast = actor.broadcast(None, ROOT_WORKER).unwrap();
            if let BfsMessage::WorkStatus(work) = bcast {
                should_terminate = work == 0;
            } else {
                panic!("Invalid broadcast output");
            }
        }

        timestamps.push(timestamp(format!("iter_{}_end", current_level)));

        if should_terminate {
            break;
        }

        std::mem::swap(&mut current_frontier, &mut next_frontier);
        current_level += 1;
    }

    timestamps.push(timestamp("worker_end".to_string()));
    Output {
        worker_id,
        timestamps,
    }
}

pub fn main(args: Value, burst_middleware: Middleware<BfsMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    let burst_middleware = burst_middleware.get_actor_handle();

    let result = bfs(input, &burst_middleware);

    serde_json::to_value(result)
}
