use bfs_bcm_all_to_all::{Graph, Output};
use clap::Parser;
use std::collections::{HashMap, VecDeque};
use std::fs::File;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'f', long)]
    graph_file: String,

    #[arg(short = 'j', long)]
    json_result: String,

    #[arg(long, default_value_t = 27491095)]
    seed: u64,
}

// Sequential implementation that validates the output (i.e. the distances at which each node is
// discovered, the exploration tree).
fn sequential_bfs(graph: &Graph, source: usize) -> Vec<usize> {
    let num_nodes = graph.num_nodes();
    let mut distances = vec![usize::MAX; num_nodes];
    let mut queue = VecDeque::new();

    distances[source] = 0;
    queue.push_back(source);

    while let Some(u) = queue.pop_front() {
        let current_dist = distances[u];
        for &v in graph.get_neighbors(u) {
            if distances[v] == usize::MAX {
                distances[v] = current_dist + 1;
                queue.push_back(v);
            }
        }
    }
    distances
}

fn main() {
    let args = Args::parse();

    println!("Loading Graph from {}...", args.graph_file);
    let graph = Graph::from_file(&args.graph_file);
    let num_nodes = graph.num_nodes();
    println!("Graph loaded: {} nodes.", num_nodes);

    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;
    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut source = 0;
    loop {
        let u = (rng.next_u64() as usize) % num_nodes;
        if graph.degree(u) > 0 {
            source = u;
            break;
        }
    }

    println!("Running Sequential Ground Truth BFS from random source {} (seed {})", source, args.seed);
    let expected_distances = sequential_bfs(&graph, source);
    let reachable_nodes = expected_distances.iter().filter(|&&d| d != usize::MAX).count();
    println!("Ground Truth BFS finished. Reachable nodes: {}", reachable_nodes);

    println!("Loading distributed JSON results from {}...", args.json_result);
    let file = File::open(&args.json_result).expect("Could not open JSON result file");
    let distributed_outputs: Vec<Output> = serde_json::from_reader(file).expect("Could not parse JSON");
    
    let mut distributed_distances = HashMap::new();
    for output in distributed_outputs {
        for (node, dist) in output.local_distances {
            distributed_distances.insert(node, dist);
        }
    }

    println!("Validating distributed results...");
    if distributed_distances.len() != reachable_nodes {
        println!("❌ ERROR: Distributed BFS reached {} nodes, but Ground Truth reached {} nodes.", distributed_distances.len(), reachable_nodes);
        std::process::exit(1);
    }

    let mut errors = 0;
    for (node, dist) in distributed_distances.iter() {
        let expected = expected_distances[*node];
        if *dist != expected {
            println!("❌ ERROR: Node {} -> Expected Dist {}, Distributed Dist {}", node, expected, dist);
            errors += 1;
            if errors > 10 {
                println!("... and more errors. Aborting.");
                break;
            }
        }
    }

    if errors == 0 {
        println!("✅ SUCCESS! Distributed BFS results exactly match the Ground Truth.");
    } else {
        std::process::exit(1);
    }
}
