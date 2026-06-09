# Parallel BSP Breadth-First Search (OpenWhisk Emulation)

This is a Rust application that computes the Breadth-First Search (BFS) distances on a grid graph using a fully parallelized **Bulk Synchronous Parallel (BSP)** approach. The program uses the internal `@burst-communication-middleware` to distribute messages and establish the iteration boundaries via network collective operations.

This version is structured to mimic an OpenWhisk Serverless execution environment, decoupling the core logic (`src/lib.rs`) from the local testing wrapper (`src/main.rs`).

## Requirements

- **Rust / Cargo**: You need Rust and Cargo installed to compile the application.
- **Docker & Docker-Compose (Optional)**: The Burst Middleware uses Redis for cross-node communication. If you are simulating a distributed cluster locally across multiple groups (e.g., `burst-size > granularity`), you must have a Redis instance running. For purely local, single-node multi-threading (where `burst-size == granularity`), **Docker and Redis are NOT required**.
```rust 
#[async_trait]
impl RemoteSendProxy for RedisListSendProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        let con = self.redis_pool.get().await?; // HERE IT WILL FAIL IN CASE A REMOTE MESSAGE IS NEEDED!!!
        Ok(send_direct(
            con,
            msg,
            self.worker_id,
            dest,
            &self.redis_options,
            &self.burst_options,
        )
        .await?)
    }
}
```

## Setup

First, navigate to the `bfs-paralellized` directory (this folder):

```bash
cd bfs-paralellized
```

*(Optional)* Launch the required Redis instance in the background using docker-compose only if you plan to do multi-group distributed testing:

```bash
docker-compose up -d
```

## Compilation

Build the release executable:

```bash
cargo build --release
```

## Execution & Parameters

The application exposes a CLI for dynamic configuration simulating the middleware layout and graph generation parameters.

```bash
cargo run --release -- --help
```

Available arguments:
- `-i, --burst-id <BURST_ID>`: Identifier for the burst execution (default: "bfs").
- `-b, --burst-size <BURST_SIZE>`: Total number of workers globally (default: 4).
- `-g, --group-id <GROUP_ID>`: ID of the local group executing on this node (default: 0).
- `-G, --granularity <GRANULARITY>`: Number of workers per group/node (default: 4).
- `--redis-url <REDIS_URL>`: Connection string for Redis (default: "redis://127.0.0.1").
- `-e, --enable-chunking`: Flag to enable message chunking in the middleware.
- `-m, --message-chunk-size <SIZE>`: Size of chunks in bytes (default: 1048576).
- `-r, --rows <ROWS>`: Sets the number of rows of the generated synthetic grid graph (default: 100).
- `-c, --cols <COLS>`: Sets the number of columns of the generated synthetic grid graph (default: 100).
- `-t, --trials <TRIALS>`: Number of BFS trials to execute (default: 64).
- `--seed <SEED>`: Seed used to select the random source nodes for the trials (default: 27491095).
- `-f, --graph-file <FILE>`: Path to a graph file (.el or .sg) to load instead of generating a grid.
- `-C, --comm-mode <MODE>`: Communication strategy pattern to use. Available options are `all-to-all`, `broadcast-reduce` or `scatter-reduce` (default: `all-to-all`). **Note:** `broadcast-reduce` strictly requires the remote backend to be running even for single-group executions. To evaluate broadcast behavior without Redis, use the `scatter-reduce` strategy instead.
### Execution Example

Run a test simulating 4 workers in a single group (granularity = 4), over a 500x500 grid:

```bash
cargo run --release -- -b 4 -G 4 -r 500 -c 500
```

**Expected Console Output:**

```text
num_groups: 1
Execution completed in 491.68 ms
```

The execution will also generate an `output_bfs_group-0.json` file in the same directory, containing a serialized array of the results and precise step-by-step timestamps for each worker. This structure directly mimics the JSON responses that would be returned by an action to the OpenWhisk controller.

## Validation & Python Visualization

You can use the built-in validator to sequentially run BFS and confirm all distances are correct:
```bash
cargo run --release --bin validator -- -f <path_to_graph> -j output_bfs_group-0.json --seed 27491095
```

To plot the execution times, phase differences, and performance:
```bash
python visualize_bfs.py output_bfs_group-0.json
```

## GAP Benchmark Suite (GAPBS) Integration

This BFS implementation natively supports the standards and file formats (like the highly-efficient binary `.sg`) of the official **GAP Benchmark Suite**, executing `64` iterations with standardized seeds to provide 1-to-1 reproducible performance comparisons.

### Generating GAPBS Graphs
To generate the reference graphs directly on your testing cluster, clone and compile the GAPBS C++ repository:

```bash
git clone https://github.com/sbeamer/gapbs.git
cd gapbs
make
```

Use the compiled `converter` tool to generate synthetic datasets:
- **Kronecker Graph (scale 27)**: `./converter -g 27 -b kron27.sg`
- **Uniform Random Graph (scale 27, avg degree 16)**: `./converter -u 27 -k 16 -b urand27.sg`

Alternatively, the GAPBS Makefile provides targets to automatically download and serialize real-world standard graphs (Warning: These require several gigabytes of storage and bandwidth):
```bash
make twitter.sg
make web.sg
make road.sg
```

### Loading GAPBS Graphs in Rust
Once you have the `.sg` files, simply supply the file path to our binary. Thanks to memory-sharing pointers, the graph will only be loaded into RAM once, even if you spawn hundreds of threaded workers locally:

```bash
cargo run --release --bin bfs-bcm-all-to-all -- -b 4 -G 4 -t 64 -f /path/to/kron27.sg -C all-to-all
```

## Cleanup

When finished, you can safely spin down the Redis container:

```bash
docker-compose down
```