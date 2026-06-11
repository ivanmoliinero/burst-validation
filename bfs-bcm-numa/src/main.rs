use bfs_bcm_numa::{BfsMessage, Graph, Input, main as ow_main};
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

    #[arg(long, default_value_t = false)]
    numa_divide: bool,
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

    use bfs_bcm_numa::numa::{
        NumaPolicy, divided::DividedNumaPolicy, monolithic::MonolithicNumaPolicy,
    };
    use std::sync::Arc;
    let numa_policy: Arc<dyn NumaPolicy> = if args.numa_divide {
        Arc::new(DividedNumaPolicy)
    } else {
        Arc::new(MonolithicNumaPolicy)
    };

    numa_policy.apply_memory_policy(&graph);

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
    let actual_comm_mode = if args.numa_divide {
        "all-to-all-numa".to_string()
    } else {
        "all-to-all".to_string()
    };

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
                comm_mode: actual_comm_mode.clone(),
            })
            .unwrap(),
        );
    }

    let start_par = Instant::now();
    let burst_size = args.burst_size;

    let threads = actors_vec
        .into_iter()
        .zip(params)
        .map(move |(proxies, param)| {
            let numa_policy_clone = numa_policy.clone();
            thread::spawn(move || {
                let (worker_id, proxy) = proxies;
                numa_policy_clone.apply_thread_policy(worker_id, burst_size);

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
