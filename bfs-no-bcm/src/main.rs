use clap::Parser;
use crossbeam_channel::unbounded;
use log::info;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
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

#[derive(Clone, Copy, Debug)]
enum BfsMessage {
    Node(usize),
    Flush,
}

fn run_bfs_iteration(
    num_threads: u32,
    graph: Arc<Graph>,
    source: usize,
    num_nodes: usize,
) -> Duration {
    let mut senders = Vec::with_capacity(num_threads as usize);
    let mut receivers = Vec::with_capacity(num_threads as usize);

    for _ in 0..num_threads {
        let (tx, rx) = unbounded::<BfsMessage>();
        senders.push(tx);
        receivers.push(rx);
    }

    let distances: Arc<Vec<AtomicUsize>> =
        Arc::new((0..num_nodes).map(|_| AtomicUsize::new(usize::MAX)).collect());
    distances[source].store(0, Ordering::Relaxed);

    let global_work: Arc<Vec<AtomicUsize>> =
        Arc::new((0..num_threads).map(|_| AtomicUsize::new(0)).collect());
    let terminate = Arc::new(AtomicBool::new(false));

    // We need 2 barriers per loop: one to wait for everyone to publish 'has_work',
    // and another to wait for the root to decide if we should terminate.
    let compute_barrier = Arc::new(Barrier::new(num_threads as usize));
    let decision_barrier = Arc::new(Barrier::new(num_threads as usize));

    let start_par = Instant::now();

    thread::scope(|s| {
        for worker_id in 0..num_threads {
            let senders = senders.clone();
            let receiver = receivers[worker_id as usize].clone();
            
            let distances = Arc::clone(&distances);
            let graph = Arc::clone(&graph);
            let global_work = Arc::clone(&global_work);
            let terminate = Arc::clone(&terminate);
            let compute_barrier = Arc::clone(&compute_barrier);
            let decision_barrier = Arc::clone(&decision_barrier);

            s.spawn(move || {
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
                                senders[owner as usize]
                                    .send(BfsMessage::Node(v))
                                    .unwrap();
                            }
                        }
                    }
                    current_frontier.clear();

                    // Send Flush token to all other workers
                    for other in 0..num_threads {
                        if other != worker_id {
                            senders[other as usize].send(BfsMessage::Flush).unwrap();
                        }
                    }

                    // ---------------------------------------------------------
                    // Phase 2: Receive until we get Flush from all other workers
                    // ---------------------------------------------------------
                    let mut flushes_received = 0;
                    while flushes_received < num_threads - 1 {
                        // In a purely local implementation using a single crossbeam channel receiver,
                        // we do not need to poll specific workers. The channel interleaves messages.
                        // We just receive until we hit N-1 flushes!
                        let msg = receiver.recv().unwrap();
                        match msg {
                            BfsMessage::Node(v) => {
                                if distances[v].load(Ordering::Relaxed) == usize::MAX {
                                    distances[v].store(current_level + 1, Ordering::Relaxed);
                                    next_frontier.push(v);
                                }
                            }
                            BfsMessage::Flush => {
                                flushes_received += 1;
                            }
                        }
                    }

                    // ---------------------------------------------------------
                    // Phase 3: Termination Check via Fast Local Atomics
                    // ---------------------------------------------------------
                    let has_work = if next_frontier.is_empty() { 0 } else { 1 };
                    global_work[worker_id as usize].store(has_work, Ordering::SeqCst);

                    compute_barrier.wait();

                    if worker_id == 0 {
                        let total_work: usize = global_work
                            .iter()
                            .map(|a| a.load(Ordering::SeqCst))
                            .sum();
                        terminate.store(total_work == 0, Ordering::SeqCst);
                    }

                    decision_barrier.wait();

                    if terminate.load(Ordering::SeqCst) {
                        break;
                    }

                    std::mem::swap(&mut current_frontier, &mut next_frontier);
                    current_level += 1;
                }
            });
        }
    });

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
        "Running Synchronous BSP BFS with {} threads for {} iterations...",
        num_threads, args.iterations
    );

    let mut times = Vec::with_capacity(args.iterations);

    for i in 1..=args.iterations {
        let elapsed = run_bfs_iteration(num_threads, Arc::clone(&graph), source, num_nodes);
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
            times.iter()
                .map(|value| {
                    let diff = mean - *value;
                    diff * diff
                })
                .sum::<f64>()
                / (times.len() - 1) as f64
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
