use burst_communication_middleware::MiddlewareActorHandle;
use std::time::SystemTime;
use crate::{BfsMessage, Input, Output, Graph, timestamp, Timestamp, ROOT_WORKER};
use super::BfsStrategy;

pub struct AllToAllNumaStrategy;

impl BfsStrategy for AllToAllNumaStrategy {
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

        let chunk_size = (num_nodes + num_threads as usize - 1) / num_threads as usize;
        let local_distances_size = chunk_size;
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

            if (source as u32 / chunk_size as u32) == worker_id {
                let local_source = source - (worker_id as usize * chunk_size);
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
                        let owner = (v as u32) / chunk_size as u32;
                        if owner == worker_id {
                            let local_v = v - (worker_id as usize * chunk_size);
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
                        let local_v = v - (worker_id as usize * chunk_size);
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
                        let global_node = local_node + (worker_id as usize * chunk_size);
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
