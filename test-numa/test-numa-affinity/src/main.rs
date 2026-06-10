use clap::Parser;
use rayon::prelude::*;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    numa_node: Option<usize>,

    #[arg(long)]
    multi_thread: bool,
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
    let mut cpus_for_other_numa: Option<Vec<usize>> = None;

    if let Some(numa_node) = args.numa_node {
        #[cfg(target_os = "linux")]
        unsafe {
            println!("Pinning memory strictly to NUMA node {}", numa_node);
            let mut nodemask: libc::c_ulong = 1 << numa_node;
            // MPOL_BIND = 2
            let ret = libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);
            if ret != 0 {
                eprintln!("Warning: failed to set mempolicy (ret={})", ret);
            }

            let cpulist_path = format!("/sys/devices/system/node/node{}/cpulist", numa_node);
            if let Ok(cpulist) = std::fs::read_to_string(&cpulist_path) {
                let parsed = parse_cpulist(&cpulist);
                if !parsed.is_empty() {
                    println!(
                        "Discovered {} CPUs for target NUMA node {}",
                        parsed.len(),
                        numa_node
                    );
                    cpus_for_numa = Some(parsed);
                }
            }

            if args.multi_thread {
                let other_node = if numa_node == 0 { 1 } else { 0 };
                let other_cpulist_path =
                    format!("/sys/devices/system/node/node{}/cpulist", other_node);
                if let Ok(other_cpulist) = std::fs::read_to_string(&other_cpulist_path) {
                    let parsed = parse_cpulist(&other_cpulist);
                    if !parsed.is_empty() {
                        println!(
                            "Discovered {} CPUs for OTHER NUMA node {}",
                            parsed.len(),
                            other_node
                        );
                        cpus_for_other_numa = Some(parsed);
                    }
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            println!("Warning: NUMA affinity requested but not supported on this OS");
        }
    }

    let num_threads = if args.multi_thread { 2 } else { 1 };
    let mut builder = rayon::ThreadPoolBuilder::new().num_threads(num_threads);

    #[cfg(target_os = "linux")]
    if let Some(cpus) = cpus_for_numa {
        let other_cpus = cpus_for_other_numa.clone();
        let is_multi = args.multi_thread;

        builder = builder.start_handler(move |thread_idx| unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();

            if is_multi && thread_idx == 1 {
                // Thread 1 goes to the OTHER socket
                if let Some(ref o_cpus) = other_cpus {
                    libc::CPU_SET(o_cpus[0], &mut set); // Pin to the first core of the other socket
                } else {
                    libc::CPU_SET(cpus[0], &mut set); // Fallback
                }
                println!("Thread 1 assigning affinity to OTHER socket");
            } else {
                // Thread 0 goes to the target socket
                if is_multi {
                    libc::CPU_SET(cpus[0], &mut set); // Pin strictly to first core
                    println!("Thread 0 assigning affinity to TARGET socket");
                } else {
                    // Single thread mode: pin to all cores of the socket
                    for &cpu in &cpus {
                        libc::CPU_SET(cpu, &mut set);
                    }
                }
            }

            let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
            if ret == 0 {
                println!(
                    "Thread {} successfully pinned to its assigned core(s).",
                    thread_idx
                );
            } else {
                eprintln!(
                    "Warning: failed to set CPU affinity for thread {}",
                    thread_idx
                );
            }
        });
    }

    let pool = builder.build().unwrap();

    println!(
        "Starting memory allocation test with {} Rayon thread(s)...",
        num_threads
    );

    let allocation_job = |id: usize| {
        let mut massive_vector: Vec<Vec<u8>> = Vec::new();
        let chunk_size = 512 * 1024 * 1024; // 512 MB per chunk
        let mut total_allocated_gb = 0.0;

        loop {
            // Allocate 512 MB
            let mut chunk = vec![0u8; chunk_size];

            // We must WRITE to the memory to force Linux to actually assign physical pages
            for i in (0..chunk_size).step_by(4096) {
                chunk[i] = 1;
            }

            massive_vector.push(chunk);
            total_allocated_gb += 0.5;

            println!(
                "[Task {}] Allocated total: {:.1} GB",
                id, total_allocated_gb
            );
            std::thread::sleep(Duration::from_millis(100)); // Sleep slightly to allow reading logs
        }
    };

    pool.install(|| {
        if args.multi_thread {
            rayon::join(|| allocation_job(0), || allocation_job(1));
        } else {
            allocation_job(0);
        }
    });
}
