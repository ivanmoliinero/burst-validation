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
    pub nodes: Vec<usize>,
}

impl From<Bytes> for BfsMessage {
    fn from(mut bytes: Bytes) -> Self {
        let has_work_bytes = bytes.split_to(4);
        let has_work = u32::from_be_bytes([
            has_work_bytes[0],
            has_work_bytes[1],
            has_work_bytes[2],
            has_work_bytes[3],
        ]);

        let mut vecu8 = bytes.to_vec();
        let vec_usize = unsafe {
            let ratio = std::mem::size_of::<usize>() / std::mem::size_of::<u8>();
            let length = vecu8.len() / ratio;
            let capacity = vecu8.capacity() / ratio;
            let ptr = vecu8.as_mut_ptr() as *mut usize;

            std::mem::forget(vecu8);

            Vec::from_raw_parts(ptr, length, capacity)
        };
        BfsMessage { has_work, nodes: vec_usize }
    }
}

impl From<BfsMessage> for Bytes {
    fn from(mut val: BfsMessage) -> Self {
        let vec8 = unsafe {
            let ratio = std::mem::size_of::<usize>() / std::mem::size_of::<u8>();
            let length = val.nodes.len() * ratio;
            let capacity = val.nodes.capacity() * ratio;
            let ptr = val.nodes.as_mut_ptr() as *mut u8;

            std::mem::forget(val.nodes);

            Vec::from_raw_parts(ptr, length, capacity)
        };

        let mut final_bytes = Vec::with_capacity(4 + vec8.len());
        final_bytes.extend_from_slice(&val.has_work.to_be_bytes());
        final_bytes.extend_from_slice(&vec8);
        Bytes::from(final_bytes)
    }
}

fn bfs(params: Input, actor: &MiddlewareActorHandle<BfsMessage>) -> Output {
    let mut timestamps = Vec::new();
    timestamps.push(Timestamp { key: "worker_start".to_string(), value: params.graph_load_start });
    timestamps.push(Timestamp { key: "graph_generated".to_string(), value: params.graph_generated });

    let worker_id = actor.info.worker_id;
    let num_threads = params.num_threads;

    // Safety: we assume graph_ptr is a valid pointer to an immutable Graph allocated in main.rs
    let graph: &Graph = unsafe { &*(params.graph_ptr as *const Graph) };
    let num_nodes = graph.num_nodes();

    let mut local_distances_out = Vec::new();
    let mut distances: Vec<AtomicUsize> = (0..num_nodes).map(|_| AtomicUsize::new(usize::MAX)).collect();

    for (trial, &source) in params.sources.iter().enumerate() {
        timestamps.push(timestamp(format!("trial_{}_start", trial)));

        // Reset distances
        for d in &distances {
            d.store(usize::MAX, Ordering::Relaxed);
        }
        distances[source].store(0, Ordering::Relaxed);

        let mut current_frontier: Vec<usize> = Vec::new();
        let mut next_frontier: Vec<usize> = Vec::new();
        let mut current_level = 0;

        if source as u32 % num_threads == worker_id {
            current_frontier.push(source);
        }

        loop {
            // Phase 1: Local Compute & Prepare Chunks
            let mut out_chunks = vec![Vec::new(); num_threads as usize];
            let mut local_discoveries = 0;

            for &u in &current_frontier {
                for &v in graph.get_neighbors(u) {
                    if (v as u32) % num_threads == worker_id {
                        if distances[v].load(Ordering::Relaxed) == usize::MAX {
                            distances[v].store(current_level + 1, Ordering::Relaxed);
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
                .map(|chunk| BfsMessage {
                    has_work,
                    nodes: chunk,
                })
                .collect::<Vec<_>>();

            // Phase 2: All-to-All Exchange
            let recv_messages = actor.all_to_all(send_messages).unwrap();
            timestamps.push(timestamp(format!("trial_{}_iter_{}_alltoall", trial, current_level)));

            // Phase 3: Consensus & Assign External Nodes
            let mut any_worker_had_work = false;

            for msg in recv_messages {
                if msg.has_work > 0 {
                    any_worker_had_work = true;
                }
                
                for &v in &msg.nodes {
                    if distances[v].load(Ordering::Relaxed) == usize::MAX {
                        distances[v].store(current_level + 1, Ordering::Relaxed);
                        next_frontier.push(v);
                    }
                }
            }
            
            timestamps.push(timestamp(format!("trial_{}_iter_{}_process", trial, current_level)));

            if !any_worker_had_work {
                break;
            }

            std::mem::swap(&mut current_frontier, &mut next_frontier);
            current_level += 1;
        }

        timestamps.push(timestamp(format!("trial_{}_end", trial)));

        // Extract local distances for validation (only for trial 0 to save memory)
        if trial == 0 {
            for (node, dist_atomic) in distances.iter().enumerate() {
                if (node as u32) % num_threads == worker_id {
                    let d = dist_atomic.load(Ordering::Relaxed);
                    if d != usize::MAX {
                        local_distances_out.push((node, d));
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
