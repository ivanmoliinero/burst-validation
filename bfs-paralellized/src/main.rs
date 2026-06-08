use bfs_paralellized::{main as ow_main, Input, BfsMessage};
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
    #[arg(short, long, default_value = "bfs")]
    burst_id: String,

    #[arg(short, long, default_value_t = 4)]
    burst_size: u32,

    #[arg(short, long, default_value_t = 0)]
    group_id: u32,

    #[arg(short, long, default_value_t = 4)]
    granularity: u32,

    #[arg(long, default_value = "redis://127.0.0.1")]
    redis_url: String,

    #[arg(short, long, default_value_t = false)]
    enable_chunking: bool,

    #[arg(short, long, default_value_t = 1048576)]
    message_chunk_size: usize,

    // Specific to our Graph execution
    #[arg(short, long, default_value_t = 100)]
    rows: usize,

    #[arg(short, long, default_value_t = 100)]
    cols: usize,

    #[arg(short, long, default_value_t = 0)]
    source: usize,
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
    >(
        burst_options,
        channel_options,
        backend_options,
    ));

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

    // For testing locally without passing individual JSON files, we will create the parameters programmatically
    // mirroring what the OpenWhisk loader would do, passing identical config to each worker.
    let mut params = Vec::with_capacity(args.burst_size as usize);
    for _ in 0..args.burst_size {
        params.push(serde_json::to_value(Input {
            rows: args.rows,
            cols: args.cols,
            num_threads: args.burst_size,
            source: args.source,
        }).unwrap());
    }

    let start_par = Instant::now();

    let threads = actors_vec
        .into_iter()
        .zip(params)
        .map(|(proxies, param)| {
            thread::spawn(move || {
                let (worker_id, proxy) = proxies;
                info!("thread start: id={}", worker_id);
                let result = ow_main(param, proxy);
                info!("thread end: id={}", worker_id);
                result
            })
        })
        .collect::<Vec<_>>();

    let mut results = Vec::with_capacity(threads.len());
    for thread in threads {
        let worker_result = thread.join().unwrap().unwrap();
        results.push(worker_result);
    }

    let elapsed_par = start_par.elapsed();
    let elapsed_ms = elapsed_par.as_secs_f64() * 1000.0;
    println!("Execution completed in {:.2} ms", elapsed_ms);

    let output_filename = format!("output_{}_group-{}.json", args.burst_id, args.group_id);
    let output_file = std::fs::File::create(output_filename).unwrap();
    serde_json::to_writer(output_file, &results).unwrap();
}