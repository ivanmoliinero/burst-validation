use clap::Parser;
use rayon::prelude::*;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    numa_node: Option<usize>,
}

#[cfg(target_os = "linux")]
fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in s.trim().split(',') {
        let bounds: Vec<&str> = part.split('-').collect();
        if bounds.len() == 1 {
            if let Ok(c) = bounds[0].parse::<usize>() {
                cpus.push(c);
            }
        } else if bounds.len() == 2 {
            if let (Ok(start), Ok(end)) = (bounds[0].parse::<usize>(), bounds[1].parse::<usize>()) {
                for c in start..=end {
                    cpus.push(c);
                }
            }
        }
    }
    cpus
}

fn main() {
    let args = Args::parse();
    let mut cpus_for_numa: Option<Vec<usize>> = None;

    if let Some(numa_node) = args.numa_node {
        #[cfg(target_os = "linux")]
        unsafe {
            println!("Pinning memory strictly to NUMA node {}", numa_node);
            let mut nodemask: libc::c_ulong = 1 << numa_node;
            // MPOL_BIND = 2
            let ret = libc::set_mempolicy(2, &mut nodemask, 64);
            if ret != 0 {
                eprintln!("Warning: failed to set mempolicy (ret={})", ret);
            }

            let cpulist_path = format!("/sys/devices/system/node/node{}/cpulist", numa_node);
            if let Ok(cpulist) = std::fs::read_to_string(&cpulist_path) {
                let parsed = parse_cpulist(&cpulist);
                if !parsed.is_empty() {
                    println!("Discovered {} CPUs for NUMA node {}", parsed.len(), numa_node);
                    cpus_for_numa = Some(parsed);
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            println!("Warning: NUMA affinity requested but not supported on this OS");
        }
    }

    let mut builder = rayon::ThreadPoolBuilder::new().num_threads(1);

    #[cfg(target_os = "linux")]
    if let Some(cpus) = cpus_for_numa {
        builder = builder.start_handler(move |thread_idx| unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for &cpu in &cpus {
                libc::CPU_SET(cpu, &mut set);
            }
            let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
            if ret == 0 {
                println!("Thread {} successfully pinned to NUMA node.", thread_idx);
            } else {
                eprintln!("Warning: failed to set CPU affinity for thread {}", thread_idx);
            }
        });
    }

    let pool = builder.build().unwrap();

    println!("Starting infinite memory allocation loop using 1 Rayon thread...");
    
    // We launch the memory consumer inside the Rayon thread pool to ensure 
    // it inherits the NUMA affinity restrictions.
    pool.install(|| {
        let mut massive_vector: Vec<Vec<u8>> = Vec::new();
        let chunk_size = 512 * 1024 * 1024; // 512 MB per chunk
        let mut total_allocated_gb = 0.0;

        loop {
            // Allocate 512 MB
            let mut chunk = vec![0u8; chunk_size];
            
            // We must WRITE to the memory to force Linux to actually assign physical pages
            // (bypass the optimistic overcommit / virtual memory lazyness).
            for i in (0..chunk_size).step_by(4096) {
                chunk[i] = 1;
            }

            massive_vector.push(chunk);
            total_allocated_gb += 0.5;

            println!("Allocated total: {:.1} GB", total_allocated_gb);
            std::thread::sleep(Duration::from_millis(100)); // Sleep slightly to allow reading logs
        }
    });
}
