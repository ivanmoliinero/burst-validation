use burst_communication_middleware::MiddlewareActorHandle;
use crate::{BfsMessage, Input, Output, Graph};

pub mod all_to_all;
pub mod all_to_all_numa;
pub mod broadcast_reduce;
pub mod scatter_reduce;

pub trait BfsStrategy {
    fn execute(
        &self,
        params: &Input,
        actor: &MiddlewareActorHandle<BfsMessage>,
        graph: &Graph,
    ) -> Output;
}
