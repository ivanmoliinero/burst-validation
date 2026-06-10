use bfs_bcm_numa::{main as ow_main, BfsMessage, Graph, Input};
use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Middleware, RedisListImpl, RedisListOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use clap::Parser;
use log::info;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'i', long, default_value = "bfs")]
    burst_id: String,

    #[arg(short = 'b', long, default_value_t = 4)]
    burst_size: u32,

    #[arg(short = 'g', long, default_value_t = 0)]
    group_id: u32,

    #[arg(short = 'G', long, default_value_t = 4)]
    granularity: u32,

    #[arg(long, default_value = "redis://127.0.0.1")]
    redis_url: String,

    #[arg(short = 'e', long, default_value_t = false)]
    enable_chunking: bool,

    #[arg(short = 'm', long, default_value_t = 1048576)]
    message_chunk_size: usize,

    // Specific to our Graph execution
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

    #[arg(short = 'C', long = "comm-mode", default_value = "all-to-all")]
    comm_mode: String,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    if args.burst_size % args.granularity != 0 {
        panic!(
            "BURST_SIZE {} must be divisible by GRANULARITY {}",
            args.burst_size, args.granularity
        );
    }

    let num_groups = args.burst_size / args.granularity;
    println!("num_groups: {}", num_groups);

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("tokio-worker")
        .build()
        .unwrap();

    let group_ranges = (0..num_groups)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((args.granularity * group_id)..((args.granularity * group_id) + args.granularity))
                    .collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    let burst_options = BurstOptions::new(
        args.burst_size,
        group_ranges.clone(),
        args.group_id.to_string(),
    )
    .burst_id(args.burst_id.clone())
    .enable_message_chunking(args.enable_chunking)
    .message_chunk_size(args.message_chunk_size)
    .build();

    let channel_options = TokioChannelOptions::new()
        .broadcast_channel_size(256)
        .build();
    let backend_options = RedisListOptions::new(args.redis_url.clone()).build();

    // Create proxies, first generating a promise (future) and then blocking until it is finished.
    let fut = tokio_runtime.spawn(BurstMiddleware::create_proxies::<
        TokioChannelImpl,
        RedisListImpl,
        _,
        _,
    >(burst_options, channel_options, backend_options));

    let proxies = tokio_runtime.block_on(fut).unwrap().unwrap();

    let actors = proxies
        .into_iter()
        .map(|(worker_id, middleware)| {
            (
                worker_id,
                Middleware::new(middleware, tokio_runtime.handle().clone()),
            )
        })
        .collect::<HashMap<u32, Middleware<BfsMessage>>>();

    let mut actors_vec = actors.into_iter().collect::<Vec<_>>();
    actors_vec.sort_by(|(a, _), (b, _)| a.cmp(b));

    use std::time::{SystemTime, UNIX_EPOCH};
    let start_load = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();

    // Load graph once in the main thread
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

    println!("Graph generated/loaded! Starting workers creation...");

    #[cfg(target_os = "linux")]
    unsafe {
        println!("Applying mbind to physically partition the Graph RAM...");
        let target_node = 1;
        let mut nodemask: libc::c_ulong = 1 << target_node;

        let offsets_len = graph.offsets.len();
        let offsets_mid = offsets_len / 2;
        // Find the page-aligned starting address for mbind
        // mbind requires the address to be page-aligned (usually 4096 bytes)
        let raw_ptr = graph.offsets.as_ptr().add(offsets_mid) as usize;
        let aligned_ptr = raw_ptr & !(4096 - 1);
        let alignment_offset = raw_ptr - aligned_ptr;
        let offsets_bytes_to_move =
            (offsets_len - offsets_mid) * std::mem::size_of::<usize>() + alignment_offset;

        // MPOL_BIND = 2, MPOL_MF_MOVE = 2
        let ret1 = libc::syscall(
            libc::SYS_mbind,
            aligned_ptr as *mut libc::c_void,
            offsets_bytes_to_move,
            2,
            &mut nodemask,
            64,
            2,
        );

        let edges_mid = graph.offsets[offsets_mid];
        let edges_len = graph.edges.len();

        let raw_edges_ptr = graph.edges.as_ptr().add(edges_mid) as usize;
        let aligned_edges_ptr = raw_edges_ptr & !(4096 - 1);
        let edges_alignment_offset = raw_edges_ptr - aligned_edges_ptr;
        let edges_bytes_to_move =
            (edges_len - edges_mid) * std::mem::size_of::<usize>() + edges_alignment_offset;

        let ret2 = libc::syscall(
            libc::SYS_mbind,
            aligned_edges_ptr as *mut libc::c_void,
            edges_bytes_to_move,
            2,
            &mut nodemask,
            64,
            2,
        );

        println!("mbind offsets ret: {}, mbind edges ret: {}", ret1, ret2);
    }

    let graph_ptr = &graph as *const Graph as usize;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(args.seed);
    let num_nodes = graph.num_nodes();
    let mut sources = Vec::new();
    while sources.len() < args.trials as usize {
        let u = (rng.next_u64() as usize) % num_nodes;
        if graph.degree(u) > 0 {
            sources.push(u);
        }
    }

    // For testing locally without passing individual JSON files, we will create the parameters programmatically
    // mirroring what the OpenWhisk loader would do, passing identical config to each worker.
    let mut params = Vec::with_capacity(args.burst_size as usize);
    for _ in 0..args.burst_size {
        params.push(
            serde_json::to_value(Input {
                rows: args.rows,
                cols: args.cols,
                num_threads: args.burst_size,
                sources: sources.clone(),
                graph_ptr,
                graph_load_start: start_load.clone(),
                graph_generated: end_load.clone(),
                comm_mode: args.comm_mode.clone(),
            })
            .unwrap(),
        );
    }

    let start_par = Instant::now();

    let threads = actors_vec
        .into_iter()
        .zip(params)
        .map(|(proxies, param)| {
            thread::spawn(move || {
                let (worker_id, proxy) = proxies;

                #[cfg(target_os = "linux")]
                unsafe {
                    let target_node = worker_id % 2; // worker 0 -> Node 0, worker 1 -> Node 1

                    // 1. Pin Memory (MPOL_BIND)
                    let mut nodemask: libc::c_ulong = 1 << target_node;
                    libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);

                    // 2. Pin CPU (sched_setaffinity)
                    let cpulist_path =
                        format!("/sys/devices/system/node/node{}/cpulist", target_node);
                    if let Ok(cpulist) = std::fs::read_to_string(&cpulist_path) {
                        let mut cpus = Vec::new();
                        for part in cpulist.trim().split(',') {
                            let bounds: Vec<&str> = part.split('-').collect();
                            if bounds.len() == 1 {
                                if let Ok(c) = bounds[0].parse::<usize>() {
                                    cpus.push(c);
                                }
                            } else if bounds.len() == 2 {
                                if let (Ok(start), Ok(end)) =
                                    (bounds[0].parse::<usize>(), bounds[1].parse::<usize>())
                                {
                                    for c in start..=end {
                                        cpus.push(c);
                                    }
                                }
                            }
                        }
                        if !cpus.is_empty() {
                            let mut set: libc::cpu_set_t = std::mem::zeroed();
                            for cpu in cpus {
                                libc::CPU_SET(cpu, &mut set);
                            }
                            libc::sched_setaffinity(
                                0,
                                std::mem::size_of::<libc::cpu_set_t>(),
                                &set,
                            );
                        } // else, do not pin
                    }
                }

                info!("thread start: id={}", worker_id);
                let result = ow_main(param, proxy);
                info!("thread end: id={}", worker_id);
                result
            })
        })
        .collect::<Vec<_>>();

    let mut results = Vec::with_capacity(threads.len());

    println!("Workers created! Waiting the threads...");

    for thread in threads {
        let worker_result = thread.join().unwrap().unwrap();
        results.push(worker_result);
    }

    let elapsed_par = start_par.elapsed();
    let elapsed_ms = elapsed_par.as_secs_f64() * 1000.0;
    println!("Execution completed in {:.2} ms", elapsed_ms);

    // Free the massive graph memory before generating the JSON output
    drop(graph);
    println!("Graph memory freed.");

    use std::io::BufWriter;
    let output_filename = format!("output_{}_group-{}.json", args.burst_id, args.group_id);
    let output_file = std::fs::File::create(output_filename).unwrap();
    let writer = BufWriter::with_capacity(8 * 1024 * 1024, output_file);
    serde_json::to_writer(writer, &results).unwrap();
}
