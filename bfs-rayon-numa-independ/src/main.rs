pub mod gapbs_parser;
pub use gapbs_parser::Graph;

pub mod numa;

use clap::Parser;
use crossbeam_channel::bounded;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'r', long, default_value_t = 100)]
    rows: usize,

    #[arg(short = 'c', long, default_value_t = 100)]
    cols: usize,

    #[arg(short = 't', long, default_value_t = 64)]
    trials: u32,

    #[arg(long, default_value_t = 27491095)]
    seed: u64,

    #[arg(short = 'f', long)]
    graph_file: Option<String>,

    #[arg(long, default_value_t = 4)]
    threads: usize,

    #[arg(long, default_value_t = false)]
    numa_divide: bool,
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

pub fn timestamp(key: String) -> Timestamp {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH).unwrap();
    let microseconds_timestamp = duration_since_epoch.as_micros();
    Timestamp {
        key,
        value: microseconds_timestamp.to_string(),
    }
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let start_load = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros().to_string();

    let (num_nodes, is_grid) = if let Some(ref path) = args.graph_file {
        println!("Preliminary scan of graph from file: {}", path);
        let temp_graph = Graph::from_file(path);
        let n = temp_graph.num_nodes();
        drop(temp_graph);
        (n, false)
    } else {
        println!("Generating grid graph {}x{}", args.rows, args.cols);
        (args.rows * args.cols, true)
    };

    let end_load = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros().to_string();
    println!("Graph scanned! Nodes: {}", num_nodes);

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut sources = Vec::new();
    while sources.len() < args.trials as usize {
        let u = (rng.next_u64() as usize) % num_nodes;
        sources.push(u); 
    }

    if args.numa_divide {
        println!("Running in INDEPENDENT NUMA MODE");
        run_independent_mode(num_nodes, is_grid, args, start_load, end_load, sources);
    } else {
        println!("Running in MONOLITHIC MODE");
        run_monolithic_mode(num_nodes, is_grid, args, start_load, end_load, sources);
    }
}

fn run_independent_mode(num_nodes: usize, is_grid: bool, args: Args, start_load: String, end_load: String, sources: Vec<usize>) {
    let split_point = num_nodes / 2;
    let threads_per_node = std::cmp::max(1, args.threads / 2);

    let mut timestamps_global = Vec::new();
    timestamps_global.push(Timestamp { key: "worker_start".to_string(), value: start_load });
    timestamps_global.push(Timestamp { key: "graph_generated".to_string(), value: end_load });

    let start_par = Instant::now();

    let mut local_distances_out = Vec::new();

    std::thread::scope(|s| {
        let mut handlers = Vec::new();

        let (tx_01, rx_01) = bounded::<Vec<usize>>(1);
        let (tx_10, rx_10) = bounded::<Vec<usize>>(1);
        let (tx_sync_01, rx_sync_01) = bounded::<usize>(1);
        let (tx_sync_10, rx_sync_10) = bounded::<usize>(1);
        let (tx_ts, rx_ts) = bounded::<Vec<Timestamp>>(1);
        let (tx_dist, rx_dist) = bounded::<Vec<(usize, usize)>>(1);

        // --- DELEGADO NODO 0 ---
        handlers.push(s.spawn({
            let tx_01 = tx_01.clone();
            let rx_10 = rx_10.clone();
            let tx_sync_01 = tx_sync_01.clone();
            let rx_sync_10 = rx_sync_10.clone();
            let tx_ts = tx_ts.clone();
            let tx_dist = tx_dist.clone();
            let sources = sources.clone();
            let graph_file = args.graph_file.clone();
            move || {
                numa::bind_thread_to_node(0);
                
                let graph_0 = if !is_grid {
                    Graph::from_file_partitioned(graph_file.as_ref().unwrap(), 0, split_point)
                } else {
                    Graph::new_grid(args.rows, args.cols)
                };

                let distances_0: Vec<AtomicUsize> = (0..num_nodes)
                    .map(|_| AtomicUsize::new(usize::MAX))
                    .collect();

                let builder = rayon::ThreadPoolBuilder::new().num_threads(threads_per_node);
                #[cfg(target_os = "linux")]
                {
                    builder = builder.start_handler(move |_| {
                        numa::bind_thread_to_node(0);
                    });
                }
                let pool0 = builder.build().unwrap();

                let mut local_ts = Vec::new();
                let mut dist_out = Vec::new();
                
                for (trial, &source) in sources.iter().enumerate() {
                    local_ts.push(timestamp(format!("trial_{}_start", trial)));

                    pool0.install(|| {
                        distances_0.par_iter().for_each(|d| {
                            d.store(usize::MAX, Ordering::Relaxed);
                        });
                    });

                    if source < split_point {
                        distances_0[source].store(0, Ordering::Relaxed);
                    }

                    let mut current_frontier = if source < split_point { vec![source] } else { vec![] };
                    let mut current_level = 0;
                    let mut iter_start = SystemTime::now();

                    loop {
                        local_ts.push(timestamp(format!("trial_{}_iter_{}_compute", trial, current_level)));

                        let (mut next_local, export_to_node1) = pool0.install(|| {
                            current_frontier.par_iter().fold(
                                || (Vec::new(), Vec::new()),
                                |mut local_acc, &u| {
                                    for &v in graph_0.get_neighbors(u) {
                                        if distances_0[v].load(Ordering::Relaxed) == usize::MAX {
                                            if distances_0[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                                if v < split_point {
                                                    local_acc.0.push(v);
                                                } else {
                                                    local_acc.1.push(v);
                                                }
                                            }
                                        }
                                    }
                                    local_acc
                                }
                            ).reduce(
                                || (Vec::new(), Vec::new()),
                                |mut a, mut b| {
                                    a.0.append(&mut b.0);
                                    a.1.append(&mut b.1);
                                    a
                                }
                            )
                        });

                        local_ts.push(timestamp(format!("trial_{}_iter_{}_crossbeam", trial, current_level)));

                        tx_01.send(export_to_node1).unwrap();
                        let imported_from_node1 = rx_10.recv().unwrap();

                        for &v in &imported_from_node1 {
                            if distances_0[v].load(Ordering::Relaxed) == usize::MAX {
                                if distances_0[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                    next_local.push(v);
                                }
                            }
                        }

                        tx_sync_01.send(next_local.len()).unwrap();
                        let remote_len = rx_sync_10.recv().unwrap();

                        local_ts.push(timestamp(format!("trial_{}_iter_{}_process", trial, current_level)));

                        if let Ok(elapsed) = iter_start.elapsed() {
                            println!("[Node 0] Trial {} | Iter {} | Sync {:.3}ms | L-Frontier: {} | R-Frontier: {}",
                                trial, current_level, elapsed.as_secs_f64() * 1000.0, next_local.len(), remote_len);
                        }

                        if next_local.is_empty() && remote_len == 0 {
                            break;
                        }

                        std::mem::swap(&mut current_frontier, &mut next_local);
                        current_level += 1;
                        iter_start = SystemTime::now();
                    }

                    local_ts.push(timestamp(format!("trial_{}_end", trial)));

                    if trial == 0 {
                        for node in 0..split_point {
                            let dist = distances_0[node].load(Ordering::Relaxed);
                            if dist != usize::MAX {
                                dist_out.push((node, dist));
                            }
                        }
                    }
                }
                tx_ts.send(local_ts).unwrap();
                tx_dist.send(dist_out).unwrap();
            }
        }));

        // --- DELEGADO NODO 1 ---
        handlers.push(s.spawn({
            let tx_10 = tx_10.clone();
            let rx_01 = rx_01.clone();
            let tx_sync_10 = tx_sync_10.clone();
            let rx_sync_01 = rx_sync_01.clone();
            let tx_dist = tx_dist.clone();
            let sources = sources.clone();
            let graph_file = args.graph_file.clone();
            move || {
                numa::bind_thread_to_node(1);
                
                let graph_1 = if !is_grid {
                    Graph::from_file_partitioned(graph_file.as_ref().unwrap(), split_point, num_nodes)
                } else {
                    Graph::new_grid(args.rows, args.cols)
                };

                let distances_1: Vec<AtomicUsize> = (0..num_nodes)
                    .map(|_| AtomicUsize::new(usize::MAX))
                    .collect();

                let builder = rayon::ThreadPoolBuilder::new().num_threads(threads_per_node);
                #[cfg(target_os = "linux")]
                {
                    builder = builder.start_handler(move |_| {
                        numa::bind_thread_to_node(1);
                    });
                }
                let pool1 = builder.build().unwrap();

                let mut dist_out = Vec::new();
                
                for (trial, &source) in sources.iter().enumerate() {
                    pool1.install(|| {
                        distances_1.par_iter().for_each(|d| {
                            d.store(usize::MAX, Ordering::Relaxed);
                        });
                    });

                    if source >= split_point {
                        distances_1[source].store(0, Ordering::Relaxed);
                    }

                    let mut current_frontier = if source >= split_point { vec![source] } else { vec![] };
                    let mut current_level = 0;
                    let mut iter_start = SystemTime::now();

                    loop {
                        let (mut next_local, export_to_node0) = pool1.install(|| {
                            current_frontier.par_iter().fold(
                                || (Vec::new(), Vec::new()),
                                |mut local_acc, &u| {
                                    for &v in graph_1.get_neighbors(u) {
                                        if distances_1[v].load(Ordering::Relaxed) == usize::MAX {
                                            if distances_1[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                                if v >= split_point {
                                                    local_acc.0.push(v);
                                                } else {
                                                    local_acc.1.push(v);
                                                }
                                            }
                                        }
                                    }
                                    local_acc
                                }
                            ).reduce(
                                || (Vec::new(), Vec::new()),
                                |mut a, mut b| {
                                    a.0.append(&mut b.0);
                                    a.1.append(&mut b.1);
                                    a
                                }
                            )
                        });

                        tx_10.send(export_to_node0).unwrap();
                        let imported_from_node0 = rx_01.recv().unwrap();

                        for &v in &imported_from_node0 {
                            if distances_1[v].load(Ordering::Relaxed) == usize::MAX {
                                if distances_1[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                    next_local.push(v);
                                }
                            }
                        }

                        tx_sync_10.send(next_local.len()).unwrap();
                        let remote_len = rx_sync_01.recv().unwrap();

                        if let Ok(elapsed) = iter_start.elapsed() {
                            println!("[Node 1] Trial {} | Iter {} | Sync {:.3}ms | L-Frontier: {} | R-Frontier: {}",
                                trial, current_level, elapsed.as_secs_f64() * 1000.0, next_local.len(), remote_len);
                        }

                        if next_local.is_empty() && remote_len == 0 {
                            break;
                        }

                        std::mem::swap(&mut current_frontier, &mut next_local);
                        current_level += 1;
                        iter_start = SystemTime::now();
                    }

                    if trial == 0 {
                        for node in split_point..num_nodes {
                            let dist = distances_1[node].load(Ordering::Relaxed);
                            if dist != usize::MAX {
                                dist_out.push((node, dist));
                            }
                        }
                    }
                }
                tx_dist.send(dist_out).unwrap();
            }
        }));

        timestamps_global.extend(rx_ts.recv().unwrap());
        
        let dist0 = rx_dist.recv().unwrap();
        let dist1 = rx_dist.recv().unwrap();
        local_distances_out.extend(dist0);
        local_distances_out.extend(dist1);
    });

    timestamps_global.push(timestamp("worker_end".to_string()));

    let output = Output {
        worker_id: 0,
        timestamps: timestamps_global,
        local_distances: local_distances_out,
    };

    let elapsed_par = start_par.elapsed();
    println!("Execution completed in {:.2} ms", elapsed_par.as_secs_f64() * 1000.0);

    use std::io::BufWriter;
    let output_filename = format!("output_bfs_group-0.json");
    let output_file = std::fs::File::create(output_filename).unwrap();
    let writer = BufWriter::with_capacity(8 * 1024 * 1024, output_file);
    serde_json::to_writer(writer, &vec![output]).unwrap();
}

fn run_monolithic_mode(num_nodes: usize, _is_grid: bool, args: Args, start_load: String, end_load: String, sources: Vec<usize>) {
    // Explicit mempolicy to bind strictly to Node 0
    numa::bind_thread_to_node(0);

    let graph = if let Some(ref path) = args.graph_file {
        Graph::from_file(path)
    } else {
        Graph::new_grid(args.rows, args.cols)
    };

    let distances: Vec<AtomicUsize> = (0..num_nodes)
        .map(|_| AtomicUsize::new(usize::MAX))
        .collect();

    let split_point = num_nodes / 2;
    let threads_per_node = std::cmp::max(1, args.threads / 2);

    let builder0 = rayon::ThreadPoolBuilder::new().num_threads(threads_per_node);
    let builder1 = rayon::ThreadPoolBuilder::new().num_threads(threads_per_node);
    #[cfg(target_os = "linux")]
    {
        builder0 = builder0.start_handler(move |_| {
            numa::bind_thread_to_node(0);
        });
        builder1 = builder1.start_handler(move |_| {
            numa::bind_thread_to_node(1);
        });
    }
    let pool0 = builder0.build().unwrap();
    let pool1 = builder1.build().unwrap();

    let mut timestamps_global = Vec::new();
    timestamps_global.push(Timestamp { key: "worker_start".to_string(), value: start_load });
    timestamps_global.push(Timestamp { key: "graph_generated".to_string(), value: end_load });

    let start_par = Instant::now();
    let mut local_distances_out = Vec::new();

    let (tx_ts, rx_ts) = bounded::<Vec<Timestamp>>(1);

    for (trial, &source) in sources.iter().enumerate() {
        timestamps_global.push(timestamp(format!("trial_{}_start", trial)));

        std::thread::scope(|s| {
            s.spawn(|| {
                pool0.install(|| {
                    distances[0..split_point].par_iter().for_each(|d| {
                        d.store(usize::MAX, Ordering::Relaxed);
                    });
                });
            });
            s.spawn(|| {
                pool1.install(|| {
                    distances[split_point..num_nodes].par_iter().for_each(|d| {
                        d.store(usize::MAX, Ordering::Relaxed);
                    });
                });
            });
        });

        distances[source].store(0, Ordering::Relaxed);

        let (tx_01, rx_01) = bounded::<Vec<usize>>(1);
        let (tx_10, rx_10) = bounded::<Vec<usize>>(1);
        
        let (tx_sync_01, rx_sync_01) = bounded::<usize>(1);
        let (tx_sync_10, rx_sync_10) = bounded::<usize>(1);

        let mut initial_frontier_0 = Vec::new();
        let mut initial_frontier_1 = Vec::new();
        if source < split_point {
            initial_frontier_0.push(source);
        } else {
            initial_frontier_1.push(source);
        }

        std::thread::scope(|s| {
            // --- DELEGADO NODO 0 ---
            s.spawn(|| {
                numa::bind_thread_to_node(0);
                let mut local_ts = Vec::new();
                let mut current_frontier = initial_frontier_0;
                let mut current_level = 0;
                let mut iter_start = SystemTime::now();
                let sent_bitvec: Vec<AtomicU64> = (0..((num_nodes / 64) + 1)).map(|_| AtomicU64::new(0)).collect();

                loop {
                    local_ts.push(timestamp(format!("trial_{}_iter_{}_compute", trial, current_level)));

                    pool0.install(|| {
                        sent_bitvec.par_iter().for_each(|x| x.store(0, Ordering::Relaxed));
                    });
                    
                    let (mut next_local, export_to_node1) = pool0.install(|| {
                        current_frontier.par_iter().fold(
                            || (Vec::new(), Vec::new()),
                            |mut local_acc, &u| {
                                for &v in graph.get_neighbors(u) {
                                    if distances[v].load(Ordering::Relaxed) == usize::MAX {
                                        if v < split_point {
                                            if distances[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                                local_acc.0.push(v);
                                            }
                                        } else {
                                            let word_idx = v / 64;
                                            let mask = 1u64 << (v % 64);
                                            if (sent_bitvec[word_idx].fetch_or(mask, Ordering::Relaxed) & mask) == 0 {
                                                local_acc.1.push(v);
                                            }
                                        }
                                    }
                                }
                                local_acc
                            }
                        ).reduce(
                            || (Vec::new(), Vec::new()),
                            |mut a, mut b| {
                                a.0.append(&mut b.0);
                                a.1.append(&mut b.1);
                                a
                            }
                        )
                    });

                    local_ts.push(timestamp(format!("trial_{}_iter_{}_crossbeam", trial, current_level)));

                    tx_01.send(export_to_node1).unwrap();
                    let imported_from_node1 = rx_10.recv().unwrap();

                    for &v in &imported_from_node1 {
                        if distances[v].load(Ordering::Relaxed) == usize::MAX {
                            if distances[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                next_local.push(v);
                            }
                        }
                    }

                    tx_sync_01.send(next_local.len()).unwrap();
                    let remote_len = rx_sync_10.recv().unwrap();

                    local_ts.push(timestamp(format!("trial_{}_iter_{}_process", trial, current_level)));

                    if let Ok(elapsed) = iter_start.elapsed() {
                        println!("[Node 0] Trial {} | Iter {} | Sync {:.3}ms | L-Frontier: {} | R-Frontier: {}",
                            trial, current_level, elapsed.as_secs_f64() * 1000.0, next_local.len(), remote_len);
                    }

                    if next_local.is_empty() && remote_len == 0 {
                        break;
                    }

                    std::mem::swap(&mut current_frontier, &mut next_local);
                    current_level += 1;
                    iter_start = SystemTime::now();
                }
                tx_ts.send(local_ts).unwrap();
            });

            // --- DELEGADO NODO 1 ---
            s.spawn(|| {
                numa::bind_thread_to_node(1);
                let mut current_frontier = initial_frontier_1;
                let mut current_level = 0;
                let mut iter_start = SystemTime::now();
                let sent_bitvec: Vec<AtomicU64> = (0..((num_nodes / 64) + 1)).map(|_| AtomicU64::new(0)).collect();

                loop {
                    pool1.install(|| {
                        sent_bitvec.par_iter().for_each(|x| x.store(0, Ordering::Relaxed));
                    });
                    
                    let (mut next_local, export_to_node0) = pool1.install(|| {
                        current_frontier.par_iter().fold(
                            || (Vec::new(), Vec::new()),
                            |mut local_acc, &u| {
                                for &v in graph.get_neighbors(u) {
                                    if distances[v].load(Ordering::Relaxed) == usize::MAX {
                                        if v >= split_point {
                                            if distances[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                                local_acc.0.push(v);
                                            }
                                        } else {
                                            let word_idx = v / 64;
                                            let mask = 1u64 << (v % 64);
                                            if (sent_bitvec[word_idx].fetch_or(mask, Ordering::Relaxed) & mask) == 0 {
                                                local_acc.1.push(v);
                                            }
                                        }
                                    }
                                }
                                local_acc
                            }
                        ).reduce(
                            || (Vec::new(), Vec::new()),
                            |mut a, mut b| {
                                a.0.append(&mut b.0);
                                a.1.append(&mut b.1);
                                a
                            }
                        )
                    });

                    tx_10.send(export_to_node0).unwrap();
                    let imported_from_node0 = rx_01.recv().unwrap();

                    for &v in &imported_from_node0 {
                        if distances[v].load(Ordering::Relaxed) == usize::MAX {
                            if distances[v].compare_exchange(usize::MAX, current_level + 1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                                next_local.push(v);
                            }
                        }
                    }

                    tx_sync_10.send(next_local.len()).unwrap();
                    let remote_len = rx_sync_01.recv().unwrap();

                    if let Ok(elapsed) = iter_start.elapsed() {
                        println!("[Node 1] Trial {} | Iter {} | Sync {:.3}ms | L-Frontier: {} | R-Frontier: {}",
                            trial, current_level, elapsed.as_secs_f64() * 1000.0, next_local.len(), remote_len);
                    }

                    if next_local.is_empty() && remote_len == 0 {
                        break;
                    }

                    std::mem::swap(&mut current_frontier, &mut next_local);
                    current_level += 1;
                    iter_start = SystemTime::now();
                }
            });
        });

        timestamps_global.extend(rx_ts.recv().unwrap());
        timestamps_global.push(timestamp(format!("trial_{}_end", trial)));

        if trial == 0 {
            for (node, d) in distances.iter().enumerate() {
                let dist = d.load(Ordering::Relaxed);
                if dist != usize::MAX {
                    local_distances_out.push((node, dist));
                }
            }
        }
    }

    timestamps_global.push(timestamp("worker_end".to_string()));

    let output = Output {
        worker_id: 0,
        timestamps: timestamps_global,
        local_distances: local_distances_out,
    };

    let elapsed_par = start_par.elapsed();
    println!("Execution completed in {:.2} ms", elapsed_par.as_secs_f64() * 1000.0);

    drop(graph);
    println!("Graph memory freed.");

    use std::io::BufWriter;
    let output_filename = format!("output_bfs_group-0.json");
    let output_file = std::fs::File::create(output_filename).unwrap();
    let writer = BufWriter::with_capacity(8 * 1024 * 1024, output_file);
    serde_json::to_writer(writer, &vec![output]).unwrap();
}
