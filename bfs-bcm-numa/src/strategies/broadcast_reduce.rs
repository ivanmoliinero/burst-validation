use burst_communication_middleware::MiddlewareActorHandle;
use std::time::SystemTime;
use crate::{BfsMessage, Input, Output, Graph, timestamp, Timestamp, ROOT_WORKER};
use super::BfsStrategy;

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

                // Phase 3: Root Evaluates & Broadcasts Global Frontier (USING ORIGINAL BROADCAST API)
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
