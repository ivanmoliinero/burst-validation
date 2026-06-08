```rust
use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Middleware, RedisListImpl, RedisListOptions, TokioChannelImpl, TokioChannelOptions,
};
use bytes::Bytes;
use log::info;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

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

// BfsMessage will wrap either a standard Node payload or a special flush message
#[derive(Clone, Debug)]
enum BfsMessage {
    Node(usize),
    Flush, // Used as End-of-Transmission marker per worker
}

impl From<Bytes> for BfsMessage {
    fn from(bytes: Bytes) -> Self {
        if bytes.len() == 1 && bytes[0] == 0xFF {
            return BfsMessage::Flush;
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
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WorkStatusMsg {
    work: u32,
}

impl From<Bytes> for WorkStatusMsg {
    fn from(bytes: Bytes) -> Self {
        let work = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        WorkStatusMsg { work }
    }
}

impl From<WorkStatusMsg> for Bytes {
    fn from(val: WorkStatusMsg) -> Self {
        let mut bytes = Vec::with_capacity(4);
        bytes.extend_from_slice(&val.work.to_be_bytes());
        Bytes::from(bytes)
    }
}

fn main() {
    env_logger::init();

    let num_threads: u32 = 4;
    let rows = 100;
    let cols = 100;
    let source = 0;

    info!("Building synthetic grid graph ({} x {})...", rows, cols);
    let graph = Graph::new_grid(rows, cols);
    let num_nodes = rows * cols;
    info!("Graph has {} nodes.", num_nodes);

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let group_ranges = vec![("0".to_string(), (0..num_threads).collect::<HashSet<u32>>())]
        .into_iter()
        .collect::<HashMap<String, HashSet<u32>>>();

    let channel_options = TokioChannelOptions::new().build();
    let backend_options = RedisListOptions::new("redis://127.0.0.1".to_string()).build();

    // Actor Group 1: General Message Passing
    let burst_options_msg = BurstOptions::new(num_threads, group_ranges.clone(), "0".to_string())
        .burst_id("bfs_msg".to_string())
        .build();

    let fut = tokio_runtime.spawn(BurstMiddleware::create_proxies::<
        TokioChannelImpl,
        RedisListImpl,
        _,
        _,
    >(
        burst_options_msg,
        channel_options.clone(),
        backend_options.clone(),
    ));

    let mut actors = tokio_runtime.block_on(fut).unwrap().unwrap()
        .into_iter()
        .map(|(worker_id, middleware)| {
            (
                worker_id,
                Middleware::new(middleware, tokio_runtime.handle().clone()),
            )
        })
        .collect::<HashMap<u32, Middleware<BfsMessage>>>();
        
    // Actor Group 2: Reduce/Broadcast sync channel
    let burst_options_sync = BurstOptions::new(num_threads, group_ranges, "0".to_string())
        .burst_id("bfs_sync".to_string())
        .build();
        
    let fut2 = tokio_runtime.spawn(BurstMiddleware::create_proxies::<
        TokioChannelImpl,
        RedisListImpl,
        _,
        _,
    >(
        burst_options_sync,
        channel_options,
        backend_options,
    ));
    
    let mut actors_sync = tokio_runtime.block_on(fut2).unwrap().unwrap()
        .into_iter()
        .map(|(worker_id, middleware)| {
            (
                worker_id,
                Middleware::new(middleware, tokio_runtime.handle().clone()),
            )
        })
        .collect::<HashMap<u32, Middleware<WorkStatusMsg>>>();


    let distances: Arc<Vec<AtomicUsize>> =
        Arc::new((0..num_nodes).map(|_| AtomicUsize::new(usize::MAX)).collect());
    distances[source].store(0, Ordering::Relaxed);
    let graph_arc = Arc::new(graph);

    let mut threads = Vec::with_capacity(num_threads as usize);

    info!("Running Parallel BSP BFS with {} threads...", num_threads);
    let start_par = Instant::now();

    for worker_id in 0..num_threads {
        let actor = actors.remove(&worker_id).unwrap().get_actor_handle();
        let actor_sync = actors_sync.remove(&worker_id).unwrap().get_actor_handle();
        
        let distances = Arc::clone(&distances);
        let graph = Arc::clone(&graph_arc);

        let thread = thread::spawn(move || {
            let mut current_frontier: Vec<usize> = Vec::new();
            let mut next_frontier: Vec<usize> = Vec::new();
            let mut current_level = 0;

            if source as u32 % num_threads == worker_id {
                current_frontier.push(source);
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
                        if other == worker_id { continue; }
                        
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
                            }
                        }
                    }
                }

                // ---------------------------------------------------------
                // Phase 3: Termination Check via Middleware Reduce
                // ---------------------------------------------------------
                let has_work = if next_frontier.is_empty() { 0 } else { 1 };
                
                let local_status = WorkStatusMsg {
                    work: has_work,
                };

                let reduced_status_opt = actor_sync.reduce(local_status, |a, b| {
                    WorkStatusMsg {
                        work: a.work + b.work,
                    }
                }).unwrap();

                // `reduce` returns Some on the root worker (worker 0 usually, based on tree reduction logic)
                // We broadcast the decision to everyone using `actor_sync.broadcast()`
                let should_terminate;
                if worker_id == 0 {
                    let final_status = reduced_status_opt.unwrap();
                    should_terminate = final_status.work == 0;
                    
                    let signal = WorkStatusMsg {
                        work: if should_terminate { 0 } else { 1 },
                    };
                    actor_sync.broadcast(Some(signal), 0).unwrap();
                } else {
                    let bcast = actor_sync.broadcast(None, 0).unwrap();
                    should_terminate = bcast.work == 0;
                }

                if should_terminate {
                    break;
                }

                std::mem::swap(&mut current_frontier, &mut next_frontier);
                current_level += 1;
            }
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let elapsed_par = start_par.elapsed();
    info!("Parallel BFS took {:?}", elapsed_par);
}
```