pub mod gapbs_parser;
pub use gapbs_parser::Graph;

use clap::Parser;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
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

    #[arg(long)]
    numa_node: Option<usize>,

    #[arg(long, default_value_t = 4)]
    threads: usize,
}

#[cfg(target_os = "linux")]
fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in s.trim().split(',') {
        let bounds: Vec<&str> = part.split('-').collect();
        if bounds.len() == 1 {
            if let Ok(c) = bounds[0].parse::<usize>() {
                cpus.push(c);
            }
        } else if bounds.len() == 2 {
            if let (Ok(start), Ok(end)) = (bounds[0].parse::<usize>(), bounds[1].parse::<usize>()) {
                for c in start..=end {
                    cpus.push(c);
                }
            }
        }
    }
    cpus
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
    let milliseconds_timestamp = duration_since_epoch.as_millis();
    Timestamp {
        key,
        value: milliseconds_timestamp.to_string(),
    }
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let mut cpus_for_numa: Option<Vec<usize>> = None;

    if let Some(numa_node) = args.numa_node {
        #[cfg(target_os = "linux")]
        unsafe {
            println!("Pinning memory to NUMA node {}", numa_node);
            let mut nodemask: libc::c_ulong = 1 << numa_node;
            // MPOL_BIND = 2
            let ret = libc::set_mempolicy(2, &mut nodemask, 64);
            if ret != 0 {
                eprintln!("Warning: failed to set mempolicy (ret={})", ret);
            }

            let cpulist_path = format!("/sys/devices/system/node/node{}/cpulist", numa_node);
            if let Ok(cpulist) = std::fs::read_to_string(&cpulist_path) {
                let parsed = parse_cpulist(&cpulist);
                if !parsed.is_empty() {
                    println!(
                        "Discovered {} CPUs for NUMA node {}",
                        parsed.len(),
                        numa_node
                    );
                    cpus_for_numa = Some(parsed);
                } else {
                    eprintln!("Warning: could not parse CPUs from {}", cpulist_path);
                }
            } else {
                eprintln!("Warning: could not read {}", cpulist_path);
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            println!("Warning: NUMA affinity requested but not supported on this OS");
        }
    }

    let mut builder = rayon::ThreadPoolBuilder::new().num_threads(args.threads);

    #[cfg(target_os = "linux")]
    if let Some(cpus) = cpus_for_numa {
        builder = builder.start_handler(move |thread_idx| unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for &cpu in &cpus {
                libc::CPU_SET(cpu, &mut set);
            }
            let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
            if ret != 0 {
                eprintln!(
                    "Warning: failed to set CPU affinity for thread {}",
                    thread_idx
                );
            }
        });
    }

    builder.build_global().unwrap();

    println!("Threads configured: {}", args.threads);

    let start_load = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();

    let graph = if let Some(ref path) = args.graph_file {
        println!("Loading graph from file: {}", path);
        Graph::from_file(path)
    } else {
        println!("Generating grid graph {}x{}", args.rows, args.cols);
        Graph::new_grid(args.rows, args.cols)
    };

    let end_load = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();

    let num_nodes = graph.num_nodes();
    println!("Graph generated/loaded! Nodes: {}", num_nodes);

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut sources = Vec::new();
    while sources.len() < args.trials as usize {
        let u = (rng.next_u64() as usize) % num_nodes;
        if graph.degree(u) > 0 {
            sources.push(u);
        }
    }

    let mut timestamps = Vec::new();
    timestamps.push(Timestamp {
        key: "worker_start".to_string(),
        value: start_load,
    });
    timestamps.push(Timestamp {
        key: "graph_generated".to_string(),
        value: end_load,
    });

    let mut distances: Vec<AtomicUsize> = (0..num_nodes)
        .map(|_| AtomicUsize::new(usize::MAX))
        .collect();

    let start_par = Instant::now();
    let mut local_distances_out = Vec::new();

    for (trial, &source) in sources.iter().enumerate() {
        timestamps.push(timestamp(format!("trial_{}_start", trial)));

        // Reset distances in parallel
        distances.par_iter().for_each(|d| {
            d.store(usize::MAX, Ordering::Relaxed);
        });

        distances[source].store(0, Ordering::Relaxed);
        let mut current_frontier: Vec<usize> = vec![source];
        let mut current_level = 0;

        let mut iter_start = SystemTime::now();

        loop {
            timestamps.push(timestamp(format!(
                "trial_{}_iter_{}_compute",
                trial, current_level
            )));

            // Container centric expansion using par_iter and thread-local vectors (fold)
            let mut next_frontier: Vec<usize> = current_frontier
                .par_iter()
                .fold(
                    || Vec::new(),
                    |mut local_next, &u| {
                        for &v in graph.get_neighbors(u) {
                            if distances[v].load(Ordering::Relaxed) == usize::MAX {
                                // Compare and exchange to ensure only one thread claims the node
                                if distances[v]
                                    .compare_exchange(
                                        usize::MAX,
                                        current_level + 1,
                                        Ordering::SeqCst,
                                        Ordering::Relaxed,
                                    )
                                    .is_ok()
                                {
                                    local_next.push(v);
                                }
                            }
                        }
                        local_next
                    },
                )
                .reduce(
                    || Vec::new(),
                    |mut a, mut b| {
                        if a.len() > b.len() {
                            a.append(&mut b);
                            a
                        } else {
                            b.append(&mut a);
                            b
                        }
                    },
                );

            timestamps.push(timestamp(format!(
                "trial_{}_iter_{}_process",
                trial, current_level
            )));

            if let Ok(elapsed) = iter_start.elapsed() {
                println!(
                    "[Monitor Rayon] Trial {} | Iter {} | Sync passed in {:.3}ms | Global Frontier: {} nodes",
                    trial,
                    current_level,
                    elapsed.as_secs_f64() * 1000.0,
                    next_frontier.len()
                );
            }
            iter_start = SystemTime::now();

            if next_frontier.is_empty() {
                break;
            }

            std::mem::swap(&mut current_frontier, &mut next_frontier);
            current_level += 1;
        }

        timestamps.push(timestamp(format!("trial_{}_end", trial)));

        // Record output for trial 0 to match validation behavior
        if trial == 0 {
            for (node, d) in distances.iter().enumerate() {
                let dist = d.load(Ordering::Relaxed);
                if dist != usize::MAX {
                    local_distances_out.push((node, dist));
                }
            }
        }
    }

    timestamps.push(timestamp("worker_end".to_string()));

    let output = Output {
        worker_id: 0, // Mimic single worker output for the visualization script
        timestamps,
        local_distances: local_distances_out,
    };

    let elapsed_par = start_par.elapsed();
    println!(
        "Execution completed in {:.2} ms",
        elapsed_par.as_secs_f64() * 1000.0
    );

    // Free the massive graph memory before generating the JSON output
    drop(graph);
    println!("Graph memory freed.");

    use std::io::BufWriter;
    let output_filename = format!("output_bfs_group-0.json");
    let output_file = std::fs::File::create(output_filename).unwrap();
    let writer = BufWriter::with_capacity(8 * 1024 * 1024, output_file);
    serde_json::to_writer(writer, &vec![output]).unwrap();
}
