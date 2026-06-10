use burst_communication_middleware::MiddlewareActorHandle;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod gapbs_parser;
pub use gapbs_parser::Graph;

pub mod strategies;
use strategies::BfsStrategy;

pub const ROOT_WORKER: u32 = 0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Input {
    pub rows: usize,
    pub cols: usize,
    pub num_threads: u32,
    pub sources: Vec<usize>,
    pub graph_ptr: usize,
    pub graph_load_start: String,
    pub graph_generated: String,
    pub comm_mode: String,
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

#[derive(Clone, Debug)]
pub struct BfsMessage {
    pub has_work: u32,
    pub nodes: Bytes,
}

impl BfsMessage {
    /// Empaqueta un vector de `usize` y la bandera `has_work` en un `BfsMessage` usando Zero-Copy
    pub fn from_vec(mut chunk: Vec<usize>, has_work: u32) -> Self {
        chunk.push(has_work as usize);
        chunk.shrink_to_fit();

        let vec8 = unsafe {
            let length = chunk.len() * std::mem::size_of::<usize>();
            let capacity = chunk.capacity() * std::mem::size_of::<usize>();
            let ptr = chunk.as_mut_ptr() as *mut u8;
            std::mem::forget(chunk);
            Vec::from_raw_parts(ptr, length, capacity)
        };

        BfsMessage {
            has_work,
            nodes: Bytes::from(vec8),
        }
    }
}

impl From<Bytes> for BfsMessage {
    fn from(bytes: Bytes) -> Self {
        let mut has_work = 0;

        if bytes.len() >= std::mem::size_of::<usize>() {
            let slice = unsafe {
                std::slice::from_raw_parts(
                    bytes.as_ptr() as *const usize,
                    bytes.len() / std::mem::size_of::<usize>(),
                )
            };
            has_work = slice[slice.len() - 1] as u32;
        }

        BfsMessage {
            has_work,
            nodes: bytes,
        }
    }
}

impl From<BfsMessage> for Bytes {
    fn from(val: BfsMessage) -> Self {
        val.nodes
    }
}

fn bfs(params: Input, actor: &MiddlewareActorHandle<BfsMessage>) -> Output {
    // Safety: we assume graph_ptr is a valid pointer to an immutable Graph allocated in main.rs
    let graph: &Graph = unsafe { &*(params.graph_ptr as *const Graph) };

    let strategy: Box<dyn BfsStrategy> = match params.comm_mode.as_str() {
        "all-to-all" => Box::new(strategies::all_to_all::AllToAllStrategy),
        "broadcast-reduce" => Box::new(strategies::broadcast_reduce::BroadcastReduceStrategy),
        "scatter-reduce" => Box::new(strategies::scatter_reduce::ScatterReduceStrategy),
        _ => panic!("Unknown communication mode: {}", params.comm_mode),
    };

    strategy.execute(&params, actor, graph)
}

pub fn main(
    args: Value,
    burst_middleware: burst_communication_middleware::Middleware<BfsMessage>,
) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    let burst_middleware = burst_middleware.get_actor_handle();

    let result = bfs(input, &burst_middleware);

    serde_json::to_value(result)
}
