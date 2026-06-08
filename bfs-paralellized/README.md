# Parallel BSP Breadth-First Search

This is a Rust application that computes the Breadth-First Search (BFS) distances on a grid graph using a fully parallelized **Bulk Synchronous Parallel (BSP)** approach. The program uses the internal `@burst-communication-middleware` to distribute messages and establish the iteration boundaries via network collective operations.

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

The application exposes a robust CLI for dynamic configuration and benchmarking using `clap`.

```bash
cargo run --release -- --help
```

Available arguments:
- `-t, --threads <THREADS>`: Sets the number of worker threads to spawn (default: 4).
- `-r, --rows <ROWS>`: Sets the number of rows of the generated synthetic grid graph (default: 100).
- `-c, --cols <COLS>`: Sets the number of columns of the generated synthetic grid graph (default: 100).
- `-i, --iterations <ITERATIONS>`: Sets the number of consecutive runs to execute for accurate benchmarking (default: 5).

### Execution Example

Run a benchmark on a 500x500 grid using 2 threads, repeated 3 times:

```bash
cargo run --release -- -t 2 -r 500 -c 500 -i 3
```

**Output format:**

```text
Building synthetic grid graph (500 x 500 = 250000 nodes)...
Running Parallel BSP BFS with 2 threads for 3 iterations...
  Iteration 1: 6391.38 ms
  Iteration 2: 6195.36 ms
  Iteration 3: 6555.01 ms

--- Benchmark Results ---
Total Runs:  3
Mean Time:   6380.58 ms
Std Dev:     180.07 ms
Min Time:    6195.36 ms
Max Time:    6555.01 ms
-------------------------
```

## Cleanup

When finished, you can safely spin down the Redis container:

```bash
docker-compose down
```