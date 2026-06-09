use burst_communication_middleware::MiddlewareActorHandle;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
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
    pub comm_mode: String,
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

impl BfsMessage {
    /// Empaqueta un vector de `usize` y la bandera `has_work` en un `BfsMessage` usando Zero-Copy
    pub fn from_vec(mut chunk: Vec<usize>, has_work: u32) -> Self {
        chunk.push(has_work as usize);
        chunk.shrink_to_fit();

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
    }
}

impl From<Bytes> for BfsMessage {
    fn from(bytes: Bytes) -> Self {
        let mut has_work = 0;

        if bytes.len() >= std::mem::size_of::<usize>() {
            let slice = unsafe {
                std::slice::from_raw_parts(
                    bytes.as_ptr() as *const usize,
                    bytes.len() / std::mem::size_of::<usize>(),
                )
            };
            has_work = slice[slice.len() - 1] as u32;
        }

        BfsMessage {
            has_work,
            nodes: bytes,
        }
    }
}

impl From<BfsMessage> for Bytes {
    fn from(val: BfsMessage) -> Self {
        val.nodes
    }
}

pub trait BfsStrategy {
    fn execute(
        &self,
        params: &Input,
        actor: &MiddlewareActorHandle<BfsMessage>,
        graph: &Graph,
    ) -> Output;
}

pub struct AllToAllStrategy;

impl BfsStrategy for AllToAllStrategy {
    fn execute(
        &self,
        params: &Input,
        actor: &MiddlewareActorHandle<BfsMessage>,
        graph: &Graph,
    ) -> Output {
        let mut timestamps = Vec::new();
        timestamps.push(Timestamp {
            key: "worker_start".to_string(),
            value: params.graph_load_start.clone(),
        });
        timestamps.push(Timestamp {
            key: "graph_generated".to_string(),
            value: params.graph_generated.clone(),
        });

        let worker_id = actor.info.worker_id;
        let num_threads = params.num_threads;
        let num_nodes = graph.num_nodes();

        let local_distances_size = (num_nodes / num_threads as usize) + 1;
        let mut local_distances_out = Vec::new();
        let mut distances: Vec<usize> = vec![usize::MAX; local_distances_size];

        for (trial, &source) in params.sources.iter().enumerate() {
            timestamps.push(timestamp(format!("trial_{}_start", trial)));

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
                // Phase 1: Local Compute & Prepare Chunks
                let mut out_chunks = vec![Vec::new(); num_threads as usize];
                let mut local_discoveries = 0;
                let mut sent_bitvec = vec![0u64; (num_nodes / 64) + 1];

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
                            let word_idx = v / 64;
                            let mask = 1u64 << (v % 64);
                            // Check whether it has been already assigned.
                            if (sent_bitvec[word_idx] & mask) == 0 {
                                sent_bitvec[word_idx] |= mask;
                                let owner = (v as u32) % num_threads;
                                out_chunks[owner as usize].push(v);
                                local_discoveries += 1;
                            }
                        }
                    }
                }
                current_frontier.clear();
                timestamps.push(timestamp(format!(
                    "trial_{}_iter_{}_compute",
                    trial, current_level
                )));

                let has_work = if local_discoveries > 0 { 1 } else { 0 };

                let send_messages = out_chunks
                    .into_iter()
                    .map(|chunk| {
                        BfsMessage::from_vec(chunk, has_work)
                    })
                    .collect::<Vec<_>>();

                // Phase 2: All-to-All Exchange
                let recv_messages = actor.all_to_all(send_messages).unwrap();
                timestamps.push(timestamp(format!(
                    "trial_{}_iter_{}_alltoall",
                    trial, current_level
                )));

                // Phase 3: Consensus & Assign External Nodes
                let mut any_worker_had_work = false;
                let mut incoming_nodes_for_root = 0;

                for msg in recv_messages {
                    if msg.has_work > 0 {
                        any_worker_had_work = true;
                    }

                    let payload_with_work = unsafe {
                        std::slice::from_raw_parts(
                            msg.nodes.as_ptr() as *const usize,
                            msg.nodes.len() / std::mem::size_of::<usize>(),
                        )
                    };

                    let payload = if payload_with_work.len() > 0 {
                        &payload_with_work[0..payload_with_work.len() - 1]
                    } else {
                        &[]
                    };

                    if worker_id == ROOT_WORKER {
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

                timestamps.push(timestamp(format!(
                    "trial_{}_iter_{}_process",
                    trial, current_level
                )));

                if worker_id == ROOT_WORKER {
                    if let Ok(elapsed) = iter_start.elapsed() {
                        println!(
                            "[Monitor AllToAll] Trial {} | Iter {} | Sync passed in {:.3}ms | Worker 0 rx: {} nodes",
                            trial,
                            current_level,
                            elapsed.as_secs_f64() * 1000.0,
                            incoming_nodes_for_root
                        );
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

            if trial == 0 {
                for (local_node, &d) in distances.iter().enumerate() {
                    if d != usize::MAX {
                        let global_node =
                            local_node * (num_threads as usize) + (worker_id as usize);
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
}

pub struct BroadcastReduceStrategy;

impl BfsStrategy for BroadcastReduceStrategy {
    fn execute(
        &self,
        params: &Input,
        actor: &MiddlewareActorHandle<BfsMessage>,
        graph: &Graph,
    ) -> Output {
        let mut timestamps = Vec::new();
        timestamps.push(Timestamp {
            key: "worker_start".to_string(),
            value: params.graph_load_start.clone(),
        });
        timestamps.push(Timestamp {
            key: "graph_generated".to_string(),
            value: params.graph_generated.clone(),
        });

        let worker_id = actor.info.worker_id;
        let num_threads = params.num_threads;
        let num_nodes = graph.num_nodes();

        // Partitioned local distances just like AllToAll for max memory efficiency
        let local_distances_size = (num_nodes / num_threads as usize) + 1;
        let mut local_distances_out = Vec::new();
        let mut distances: Vec<usize> = vec![usize::MAX; local_distances_size];

        for (trial, &source) in params.sources.iter().enumerate() {
            timestamps.push(timestamp(format!("trial_{}_start", trial)));

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
                // Phase 1: Local Compute
                let mut local_discoveries = 0;
                let mut next_frontier_external: Vec<usize> = Vec::new();
                let mut sent_bitvec = vec![0u64; (num_nodes / 64) + 1];

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
                            let word_idx = v / 64;
                            let mask = 1u64 << (v % 64);
                            if (sent_bitvec[word_idx] & mask) == 0 {
                                sent_bitvec[word_idx] |= mask;
                                next_frontier_external.push(v);
                            }
                        }
                    }
                }
                current_frontier.clear();
                timestamps.push(timestamp(format!(
                    "trial_{}_iter_{}_compute",
                    trial, current_level
                )));

                // Phase 2: Reduce Local Frontiers into Global Frontier
                let has_work = if local_discoveries > 0 { 1 } else { 0 };
                let msg = BfsMessage::from_vec(next_frontier_external, has_work);

                let global_frontier_msg = actor
                    .reduce(msg, |mut msg1, msg2| {
                        let payload1_with_work = unsafe {
                            std::slice::from_raw_parts(
                                msg1.nodes.as_ptr() as *const usize,
                                msg1.nodes.len() / std::mem::size_of::<usize>(),
                            )
                        };
                        let payload2_with_work = unsafe {
                            std::slice::from_raw_parts(
                                msg2.nodes.as_ptr() as *const usize,
                                msg2.nodes.len() / std::mem::size_of::<usize>(),
                            )
                        };

                        let p1 = if payload1_with_work.len() > 0 {
                            &payload1_with_work[0..payload1_with_work.len() - 1]
                        } else {
                            &[]
                        };
                        let p2 = if payload2_with_work.len() > 0 {
                            &payload2_with_work[0..payload2_with_work.len() - 1]
                        } else {
                            &[]
                        };

                        let hw = msg1.has_work | msg2.has_work;

                        let mut merged = Vec::with_capacity(p1.len() + p2.len() + 1);
                        merged.extend_from_slice(p1);
                        merged.extend_from_slice(p2);
                        merged.sort_unstable();
                        merged.dedup();

                        BfsMessage::from_vec(merged, hw)
                    })
                    .unwrap();

                timestamps.push(timestamp(format!(
                    "trial_{}_iter_{}_reduce",
                    trial, current_level
                )));

                // Phase 3: Root Evaluates & Broadcasts Global Frontier
                let bcast_msg = if worker_id == ROOT_WORKER {
                    let combined_msg = global_frontier_msg.unwrap();
                    actor
                        .broadcast(Some(combined_msg.clone()), ROOT_WORKER)
                        .unwrap();
                    combined_msg
                } else {
                    actor.broadcast(None, ROOT_WORKER).unwrap()
                };

                timestamps.push(timestamp(format!(
                    "trial_{}_iter_{}_broadcast",
                    trial, current_level
                )));

                let payload_with_work = unsafe {
                    std::slice::from_raw_parts(
                        bcast_msg.nodes.as_ptr() as *const usize,
                        bcast_msg.nodes.len() / std::mem::size_of::<usize>(),
                    )
                };
                let global_frontier = if payload_with_work.len() > 0 {
                    &payload_with_work[0..payload_with_work.len() - 1]
                } else {
                    &[]
                };
                let any_worker_had_work = bcast_msg.has_work > 0;

                if worker_id == ROOT_WORKER {
                    if let Ok(elapsed) = iter_start.elapsed() {
                        println!(
                            "[Monitor BcastReduce] Trial {} | Iter {} | Sync passed in {:.3}ms | Global Frontier: {} nodes",
                            trial,
                            current_level,
                            elapsed.as_secs_f64() * 1000.0,
                            global_frontier.len()
                        );
                    }
                    iter_start = SystemTime::now();
                }

                if !any_worker_had_work && global_frontier.is_empty() {
                    break;
                }

                // Phase 4: Assign External Nodes from Global Frontier
                for &v in global_frontier {
                    if (v as u32) % num_threads == worker_id {
                        let local_v = v / num_threads as usize;
                        if distances[local_v] == usize::MAX {
                            distances[local_v] = current_level + 1;
                            next_frontier.push(v);
                        }
                    }
                }

                std::mem::swap(&mut current_frontier, &mut next_frontier);
                current_level += 1;
            }

            timestamps.push(timestamp(format!("trial_{}_end", trial)));

            if trial == 0 {
                for (local_node, &d) in distances.iter().enumerate() {
                    if d != usize::MAX {
                        let global_node =
                            local_node * (num_threads as usize) + (worker_id as usize);
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
}

fn bfs(params: Input, actor: &MiddlewareActorHandle<BfsMessage>) -> Output {
    // Safety: we assume graph_ptr is a valid pointer to an immutable Graph allocated in main.rs
    let graph: &Graph = unsafe { &*(params.graph_ptr as *const Graph) };

    let strategy: Box<dyn BfsStrategy> = match params.comm_mode.as_str() {
        "all-to-all" => Box::new(AllToAllStrategy),
        "broadcast-reduce" => Box::new(BroadcastReduceStrategy),
        _ => panic!("Unknown communication mode: {}", params.comm_mode),
    };

    strategy.execute(&params, actor, graph)
}

pub fn main(
    args: Value,
    burst_middleware: burst_communication_middleware::Middleware<BfsMessage>,
) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    let burst_middleware = burst_middleware.get_actor_handle();

    let result = bfs(input, &burst_middleware);

    serde_json::to_value(result)
}
