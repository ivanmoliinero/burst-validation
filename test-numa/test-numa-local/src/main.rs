use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    multipool: bool,
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
    
    let mut cpus_node0: Option<Vec<usize>> = None;
    let mut cpus_node1: Option<Vec<usize>> = None;

    #[cfg(target_os = "linux")]
    {
        // We only read the CPULIST in the main thread to pass it to the worker threads
        if let Ok(cpulist) = std::fs::read_to_string("/sys/devices/system/node/node0/cpulist") {
            let parsed = parse_cpulist(&cpulist);
            if !parsed.is_empty() {
                println!("Discovered {} CPUs for NUMA node 0", parsed.len());
                cpus_node0 = Some(parsed);
            }
        }
        if let Ok(cpulist) = std::fs::read_to_string("/sys/devices/system/node/node1/cpulist") {
            let parsed = parse_cpulist(&cpulist);
            if !parsed.is_empty() {
                println!("Discovered {} CPUs for NUMA node 1", parsed.len());
                cpus_node1 = Some(parsed);
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        println!("Warning: NUMA affinity requested but not supported on this OS");
    }

    let allocation_job = |id: usize, limit_gb: f64| {
        let mut massive_vector: Vec<Vec<u8>> = Vec::new();
        let chunk_size = 512 * 1024 * 1024; // 512 MB per chunk
        let mut total_allocated_gb = 0.0;

        loop {
            if total_allocated_gb >= limit_gb {
                println!("[Task {}] Reached {:.1} GB limit! Holding memory without allocating more...", id, limit_gb);
                std::thread::sleep(Duration::from_secs(3600)); // Sleep to hold the memory
                continue;
            }

            // Allocate 512 MB
            let mut chunk = vec![0u8; chunk_size];
            
            // We must WRITE to the memory to force Linux to actually assign physical pages
            for i in (0..chunk_size).step_by(4096) {
                chunk[i] = 1;
            }

            massive_vector.push(chunk);
            total_allocated_gb += 0.5;

            println!("[Task {}] Allocated total: {:.1} GB (Strict Local Node)", id, total_allocated_gb);
            std::thread::sleep(Duration::from_millis(100)); // Sleep slightly to allow reading logs
        }
    };

    if args.multipool {
        println!("Starting MULTIPOOL memory allocation test (2 pools, 1 thread each)...");

        let create_pool = |target_node: usize, target_cpus: Option<Vec<usize>>| {
            let mut builder = rayon::ThreadPoolBuilder::new().num_threads(1);
            #[cfg(target_os = "linux")]
            {
                builder = builder.start_handler(move |thread_idx| unsafe {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    if let Some(ref cpus) = target_cpus {
                        libc::CPU_SET(cpus[0], &mut set);
                        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
                        if ret == 0 {
                            println!("Multipool {}: Thread successfully pinned to CPU {} (NUMA Node {})", target_node, cpus[0], target_node);
                        } else {
                            eprintln!("Warning: failed to set CPU affinity for pool {}", target_node);
                        }
                    } else {
                        eprintln!("Warning: No CPUs found for NUMA Node {}", target_node);
                    }

                    let mut nodemask: libc::c_ulong = 1 << target_node;
                    let ret = libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);
                    if ret == 0 {
                        println!("Multipool {}: Thread successfully locked memory to NUMA Node {}", target_node, target_node);
                    } else {
                        eprintln!("Warning: failed to set mempolicy for pool {} (ret={})", target_node, ret);
                    }
                });
            }
            builder.build().unwrap()
        };

        let pool0 = create_pool(0, cpus_node0.clone());
        let pool1 = create_pool(1, cpus_node1.clone());

        std::thread::scope(|s| {
            s.spawn(|| {
                pool0.install(|| allocation_job(0, 20.0));
            });
            s.spawn(|| {
                pool1.install(|| allocation_job(1, 20.0));
            });
        });

    } else {
        println!("Starting SINGLE POOL dual-node memory allocation test...");

        let mut builder = rayon::ThreadPoolBuilder::new().num_threads(2);

        #[cfg(target_os = "linux")]
        {
            builder = builder.start_handler(move |thread_idx| unsafe {
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                
                let target_node = if thread_idx == 0 { 0 } else { 1 };
                let target_cpus = if target_node == 0 { &cpus_node0 } else { &cpus_node1 };
                
                if let Some(cpus) = target_cpus {
                    libc::CPU_SET(cpus[0], &mut set);
                    let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
                    if ret == 0 {
                        println!("Thread {} successfully pinned to CPU {} (NUMA Node {})", thread_idx, cpus[0], target_node);
                    } else {
                        eprintln!("Warning: failed to set CPU affinity for thread {}", thread_idx);
                    }
                } else {
                    eprintln!("Warning: No CPUs found for NUMA Node {}", target_node);
                }

                let mut nodemask: libc::c_ulong = 1 << target_node;
                let ret = libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);
                if ret == 0 {
                    println!("Thread {} successfully locked its local memory allocations strictly to NUMA Node {}", thread_idx, target_node);
                } else {
                    eprintln!("Warning: failed to set mempolicy for thread {} (ret={})", thread_idx, ret);
                }
            });
        }

        let pool = builder.build().unwrap();

        pool.install(|| {
            rayon::join(|| allocation_job(0, 55.0), || allocation_job(1, 55.0));
        });
    }
}
