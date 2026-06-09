use burst_communication_middleware::{Middleware, MiddlewareActorHandle};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod gapbs_parser;
pub use gapbs_parser::Graph;

const ROOT_WORKER: u32 = 0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Input {
    pub rows: usize,
    pub cols: usize,
    pub num_threads: u32,
    pub sources: Vec<usize>,
    pub graph_ptr: usize,
    pub graph_load_start: String,
    pub graph_generated: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Output {
    pub worker_id: u32,
    pub timestamps: Vec<Timestamp>,
    pub local_distances: Vec<(usize, usize)>,
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

#[derive(Clone, Debug)]
pub struct BfsMessage {
    pub has_work: u32,
    pub nodes: Bytes,
}

impl From<Bytes> for BfsMessage {
    fn from(bytes: Bytes) -> Self {
        let has_work = if bytes.len() >= std::mem::size_of::<usize>() {
            let slice = unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const usize, 1) };
            slice[0] as u32
        } else {
            0
        };
        BfsMessage { has_work, nodes: bytes }
    }
}

impl From<BfsMessage> for Bytes {
    fn from(val: BfsMessage) -> Self {
        val.nodes
    }
}

fn bfs(params: Input, actor: &MiddlewareActorHandle<BfsMessage>) -> Output {
    let mut timestamps = Vec::new();
    timestamps.push(Timestamp { key: "worker_start".to_string(), value: params.graph_load_start.clone() });
    timestamps.push(Timestamp { key: "graph_generated".to_string(), value: params.graph_generated.clone() });

    let worker_id = actor.info.worker_id;
    let num_threads = params.num_threads;

    // Safety: we assume graph_ptr is a valid pointer to an immutable Graph allocated in main.rs
    let graph: &Graph = unsafe { &*(params.graph_ptr as *const Graph) };
    let num_nodes = graph.num_nodes();

    let local_distances_size = (num_nodes / num_threads as usize) + 1;
    let mut local_distances_out = Vec::new();
    let mut distances: Vec<usize> = vec![usize::MAX; local_distances_size];

    for (trial, &source) in params.sources.iter().enumerate() {
        timestamps.push(timestamp(format!("trial_{}_start", trial)));

        // Reset distances
        for d in &mut distances {
            *d = usize::MAX;
        }

        let mut current_frontier: Vec<usize> = Vec::new();
        let mut next_frontier: Vec<usize> = Vec::new();
        let mut current_level = 0;

        if source as u32 % num_threads == worker_id {
            let local_source = source / num_threads as usize;
            distances[local_source] = 0;
            current_frontier.push(source);
        }

        let mut iter_start = SystemTime::now();

        loop {
            // Phase 1: Local Compute & Prepare Chunks (index 0 is reserved for has_work)
            let mut out_chunks = vec![vec![0usize]; num_threads as usize];
            let mut local_discoveries = 0;

            for &u in &current_frontier {
                for &v in graph.get_neighbors(u) {
                    if (v as u32) % num_threads == worker_id {
                        let local_v = v / num_threads as usize;
                        if distances[local_v] == usize::MAX {
                            distances[local_v] = current_level + 1;
                            next_frontier.push(v);
                            local_discoveries += 1;
                        }
                    } else {
                        let owner = (v as u32) % num_threads;
                        out_chunks[owner as usize].push(v);
                        local_discoveries += 1;
                    }
                }
            }
            current_frontier.clear();
            timestamps.push(timestamp(format!("trial_{}_iter_{}_compute", trial, current_level)));

            let has_work = if local_discoveries > 0 { 1 } else { 0 };

            let send_messages = out_chunks
                .into_iter()
                .map(|mut chunk| {
                    chunk[0] = has_work as usize;
                    
                    let vec8 = unsafe {
                        let length = chunk.len() * std::mem::size_of::<usize>();
                        let capacity = chunk.capacity() * std::mem::size_of::<usize>();
                        let ptr = chunk.as_mut_ptr() as *mut u8;
                        std::mem::forget(chunk);
                        Vec::from_raw_parts(ptr, length, capacity)
                    };

                    BfsMessage {
                        has_work,
                        nodes: Bytes::from(vec8),
                    }
                })
                .collect::<Vec<_>>();

            // Phase 2: All-to-All Exchange
            let recv_messages = actor.all_to_all(send_messages).unwrap();
            timestamps.push(timestamp(format!("trial_{}_iter_{}_alltoall", trial, current_level)));

            // Phase 3: Consensus & Assign External Nodes
            let mut any_worker_had_work = false;
            let mut incoming_nodes_for_root = 0;

            for msg in recv_messages {
                if msg.has_work > 0 {
                    any_worker_had_work = true;
                }
                
                let nodes_slice = unsafe {
                    std::slice::from_raw_parts(
                        msg.nodes.as_ptr() as *const usize,
                        msg.nodes.len() / std::mem::size_of::<usize>()
                    )
                };

                let payload = if nodes_slice.len() > 1 { &nodes_slice[1..] } else { &[] };

                if worker_id == 0 {
                    incoming_nodes_for_root += payload.len();
                }

                for &v in payload {
                    let local_v = v / num_threads as usize;
                    if distances[local_v] == usize::MAX {
                        distances[local_v] = current_level + 1;
                        next_frontier.push(v);
                    }
                }
            }
            
            timestamps.push(timestamp(format!("trial_{}_iter_{}_process", trial, current_level)));

            if worker_id == 0 {
                if let Ok(elapsed) = iter_start.elapsed() {
                    println!("[Monitor] Trial {} | Iter {} | Sync passed in {:.3}ms | Worker 0 rx: {} nodes", 
                        trial, current_level, elapsed.as_secs_f64() * 1000.0, incoming_nodes_for_root);
                }
                iter_start = SystemTime::now();
            }

            if !any_worker_had_work {
                break;
            }

            std::mem::swap(&mut current_frontier, &mut next_frontier);
            current_level += 1;
        }

        timestamps.push(timestamp(format!("trial_{}_end", trial)));

        // Extract local distances for validation (only for trial 0 to save memory)
        if trial == 0 {
            for (local_node, &d) in distances.iter().enumerate() {
                if d != usize::MAX {
                    let global_node = local_node * (num_threads as usize) + (worker_id as usize);
                    if global_node < num_nodes {
                        local_distances_out.push((global_node, d));
                    }
                }
            }
        }
    }

    timestamps.push(timestamp("worker_end".to_string()));

    Output {
        worker_id,
        timestamps,
        local_distances: local_distances_out,
    }
}

pub fn main(args: Value, burst_middleware: Middleware<BfsMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    let burst_middleware = burst_middleware.get_actor_handle();

    let result = bfs(input, &burst_middleware);

    serde_json::to_value(result)
}
