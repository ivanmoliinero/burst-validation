use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Middleware, RedisListImpl, RedisListOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use bytes::Bytes;
use clap::Parser;
use log::info;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of worker threads
    #[arg(short, long, default_value_t = 4)]
    threads: u32,

    /// Number of rows in the grid graph
    #[arg(short, long, default_value_t = 100)]
    rows: usize,

    /// Number of columns in the grid graph
    #[arg(short, long, default_value_t = 100)]
    cols: usize,

    /// Number of times to run the BFS benchmark
    #[arg(short, long, default_value_t = 5)]
    iterations: usize,
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

fn run_bfs_iteration(
    num_threads: u32,
    graph: Arc<Graph>,
    source: usize,
    num_nodes: usize,
) -> Duration {
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

    let mut actors = tokio_runtime
        .block_on(fut)
        .unwrap()
        .unwrap()
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

    let mut actors_sync = tokio_runtime
        .block_on(fut2)
        .unwrap()
        .unwrap()
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

    let mut threads = Vec::with_capacity(num_threads as usize);

    let start_par = Instant::now();

    for worker_id in 0..num_threads {
        let actor = actors.remove(&worker_id).unwrap().get_actor_handle();
        let actor_sync = actors_sync.remove(&worker_id).unwrap().get_actor_handle();

        let distances = Arc::clone(&distances);
        let graph = Arc::clone(&graph);

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
                            }
                        }
                    }
                }

                // ---------------------------------------------------------
                // Phase 3: Termination Check via Middleware Reduce
                // ---------------------------------------------------------
                let has_work = if next_frontier.is_empty() { 0 } else { 1 };

                let local_status = WorkStatusMsg { work: has_work };

                let reduced_status_opt = actor_sync
                    .reduce(local_status, |a, b| WorkStatusMsg {
                        work: a.work + b.work,
                    })
                    .unwrap();

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

    start_par.elapsed()
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let num_threads = args.threads;
    let rows = args.rows;
    let cols = args.cols;
    let source = 0;
    let num_nodes = rows * cols;

    println!(
        "Building synthetic grid graph ({} x {} = {} nodes)...",
        rows, cols, num_nodes
    );
    let graph = Arc::new(Graph::new_grid(rows, cols));

    println!(
        "Running Parallel BSP BFS with {} threads for {} iterations...",
        num_threads, args.iterations
    );

    let mut times = Vec::with_capacity(args.iterations);

    for i in 1..=args.iterations {
        let graph_clone = Arc::clone(&graph);
        let elapsed = run_bfs_iteration(num_threads, graph_clone, source, num_nodes);
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        times.push(elapsed_ms);
        println!("  Iteration {}: {:.2} ms", i, elapsed_ms);
    }

    // Statistical calculations
    if !times.is_empty() {
        let min_time = times.iter().copied().fold(f64::INFINITY, f64::min);
        let max_time = times.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = times.iter().sum();
        let mean = sum / times.len() as f64;

        let variance = if times.len() > 1 {
            times.iter().map(|value| {
                let diff = mean - *value;
                diff * diff
            }).sum::<f64>() / (times.len() - 1) as f64
        } else {
            0.0
        };
        let std_dev = variance.sqrt();

        println!("\n--- Benchmark Results ---");
        println!("Total Runs:  {}", times.len());
        println!("Mean Time:   {:.2} ms", mean);
        println!("Std Dev:     {:.2} ms", std_dev);
        println!("Min Time:    {:.2} ms", min_time);
        println!("Max Time:    {:.2} ms", max_time);
        println!("-------------------------");
    }
}
