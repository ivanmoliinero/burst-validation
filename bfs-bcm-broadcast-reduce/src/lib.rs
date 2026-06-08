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
pub struct BfsMessage(pub Vec<usize>);

impl From<Bytes> for BfsMessage {
    fn from(bytes: Bytes) -> Self {
        let mut vecu8 = bytes.to_vec();
        let vec_usize = unsafe {
            let ratio = std::mem::size_of::<usize>() / std::mem::size_of::<u8>();
            let length = vecu8.len() / ratio;
            let capacity = vecu8.capacity() / ratio;
            let ptr = vecu8.as_mut_ptr() as *mut usize;

            std::mem::forget(vecu8);

            Vec::from_raw_parts(ptr, length, capacity)
        };
        BfsMessage(vec_usize)
    }
}

impl From<BfsMessage> for Bytes {
    fn from(mut val: BfsMessage) -> Self {
        let vec8 = unsafe {
            let ratio = std::mem::size_of::<usize>() / std::mem::size_of::<u8>();
            let length = val.0.len() * ratio;
            let capacity = val.0.capacity() * ratio;
            let ptr = val.0.as_mut_ptr() as *mut u8;

            std::mem::forget(val.0);

            Vec::from_raw_parts(ptr, length, capacity)
        };
        Bytes::from(vec8)
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
        // Phase 1: Local Compute
        // ---------------------------------------------------------
        for &u in &current_frontier {
            for &v in &graph.adj[u] {
                // If it's owned by us, process it immediately
                if (v as u32) % num_threads == worker_id {
                    if distances[v].load(Ordering::Relaxed) == usize::MAX {
                        distances[v].store(current_level + 1, Ordering::Relaxed);
                        next_frontier.push(v);
                    }
                } else {
                    // Otherwise, add to the external frontier to be reduced
                    next_frontier.push(v);
                }
            }
        }
        current_frontier.clear();
        timestamps.push(timestamp(format!("iter_{}_compute", current_level)));

        // ---------------------------------------------------------
        // Phase 2: Reduce Local Frontiers into Global Frontier
        // ---------------------------------------------------------
        let global_frontier_msg = actor
            .reduce(BfsMessage(next_frontier.clone()), |mut vec1, vec2| {
                vec1.0.extend(vec2.0);
                vec1
            })
            .unwrap();
        timestamps.push(timestamp(format!("iter_{}_reduce", current_level)));

        // ---------------------------------------------------------
        // Phase 3: Root Evaluates & Broadcasts Global Frontier
        // ---------------------------------------------------------
        let global_frontier = if worker_id == ROOT_WORKER {
            let combined = global_frontier_msg.unwrap().0;
            // The root worker broadcasts the combined global frontier
            let bcast_msg = BfsMessage(combined);
            actor.broadcast(Some(bcast_msg.clone()), ROOT_WORKER).unwrap();
            bcast_msg.0
        } else {
            actor.broadcast(None, ROOT_WORKER).unwrap().0
        };
        timestamps.push(timestamp(format!("iter_{}_broadcast", current_level)));

        if global_frontier.is_empty() {
            break;
        }

        // ---------------------------------------------------------
        // Phase 4: Assign External Nodes from Global Frontier
        // ---------------------------------------------------------
        next_frontier.clear();
        for &v in &global_frontier {
            if (v as u32) % num_threads == worker_id {
                if distances[v].load(Ordering::Relaxed) == usize::MAX {
                    distances[v].store(current_level + 1, Ordering::Relaxed);
                    current_frontier.push(v);
                }
            }
        }

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
