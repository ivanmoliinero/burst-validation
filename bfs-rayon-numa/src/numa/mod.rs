use crate::Graph;

pub mod divided;
pub mod monolithic;

pub trait NumaPolicy: Send + Sync {
    fn apply_memory_policy(&self, graph: &Graph, distances: &mut [std::sync::atomic::AtomicUsize]);
    fn apply_thread_policy(&self, worker_id: u32);
}
