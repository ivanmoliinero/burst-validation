# Parallel BSP Breadth-First Search (OpenWhisk Emulation)

This is a Rust application that computes the Breadth-First Search (BFS) distances on a grid graph using a fully parallelized **Bulk Synchronous Parallel (BSP)** approach. The program uses the internal `@burst-communication-middleware` to distribute messages and establish the iteration boundaries via network collective operations.

This version is structured to mimic an OpenWhisk Serverless execution environment, decoupling the core logic (`src/lib.rs`) from the local testing wrapper (`src/main.rs`).

## Requirements

- **Rust / Cargo**: You need Rust and Cargo installed to compile the application.
- **Docker & Docker-Compose**: The Burst Middleware forces a remote backend verification on startup. For testing locally, this application comes pre-configured with a Redis dependency. You must have a Redis instance running locally (easily provided via the `docker-compose.yml` file included).

## Setup

First, navigate to the `bfs-paralellized` directory (this folder):

```bash
cd bfs-paralellized
```

Launch the required Redis instance in the background using docker-compose:

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
- `-s, --source <SOURCE>`: ID of the source node for the BFS traversal (default: 0).

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

## Cleanup

When finished, you can safely spin down the Redis container:

```bash
docker-compose down
```