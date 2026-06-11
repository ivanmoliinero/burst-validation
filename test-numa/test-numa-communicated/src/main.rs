use clap::Parser;
use crossbeam_channel::bounded;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = false)]
    deactivate_autonuma: bool,
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

#[cfg(target_os = "linux")]
fn check_numa_distribution(ptr: *const u8, size: usize) -> (f64, f64) {
    let page_size = 4096;
    let total_pages = size / page_size;
    
    let samples = 1000;
    let step = if total_pages > samples { total_pages / samples } else { 1 };
    
    let mut pages: Vec<*const libc::c_void> = Vec::with_capacity(samples);
    for i in (0..total_pages).step_by(step).take(samples) {
        unsafe {
            pages.push(ptr.add(i * page_size) as *const libc::c_void);
        }
    }
    
    let actual_samples = pages.len();
    let mut status: Vec<libc::c_int> = vec![-1; actual_samples];
    
    unsafe {
        libc::syscall(
            libc::SYS_move_pages,
            0, // pid 0 = self
            actual_samples as libc::c_ulong,
            pages.as_ptr(),
            std::ptr::null::<libc::c_int>(), // nodes = NULL means just query
            status.as_mut_ptr(),
            0,
        );
    }
    
    let mut node0 = 0;
    let mut node1 = 0;
    for &s in status.iter() {
        if s == 0 { node0 += 1; }
        else if s == 1 { node1 += 1; }
    }
    
    let n0_pct = (node0 as f64 / actual_samples as f64) * 100.0;
    let n1_pct = (node1 as f64 / actual_samples as f64) * 100.0;
    (n0_pct, n1_pct)
}

fn main() {
    let args = Args::parse();
    let deactivate_autonuma = args.deactivate_autonuma;

    let mut cpus_node0: Option<Vec<usize>> = None;
    let mut cpus_node1: Option<Vec<usize>> = None;

    #[cfg(target_os = "linux")]
    {
        if let Ok(cpulist) = std::fs::read_to_string("/sys/devices/system/node/node0/cpulist") {
            let parsed = parse_cpulist(&cpulist);
            if !parsed.is_empty() {
                cpus_node0 = Some(parsed);
            }
        }
        if let Ok(cpulist) = std::fs::read_to_string("/sys/devices/system/node/node1/cpulist") {
            let parsed = parse_cpulist(&cpulist);
            if !parsed.is_empty() {
                cpus_node1 = Some(parsed);
            }
        }
    }

    let create_pool = |target_node: usize, target_cpus: Option<Vec<usize>>| {
        let mut builder = rayon::ThreadPoolBuilder::new().num_threads(1);
        #[cfg(target_os = "linux")]
        {
            builder = builder.start_handler(move |thread_idx| unsafe {
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                if let Some(ref cpus) = target_cpus {
                    for &cpu in cpus {
                        libc::CPU_SET(cpu, &mut set);
                    }
                    libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
                }

                let mut nodemask: libc::c_ulong = 1 << target_node;
                libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);
            });
        }
        builder.build().unwrap()
    };

    println!("Initializing Rayon Pools...");
    let pool0 = create_pool(0, cpus_node0);
    let pool1 = create_pool(1, cpus_node1);

    // Channels for message passing
    let (tx_01, rx_01) = bounded::<Vec<u8>>(1);
    let (tx_10, rx_10) = bounded::<Vec<u8>>(1);

    std::thread::scope(|s| {
        // --- WORKER NODO 0 ---
        s.spawn(|| {
            pool0.install(|| {
                let ball_size = 2 * 1024 * 1024 * 1024; // 2 GB
                println!("[Node 0] Generating 2GB Ball...");
                let mut ball = vec![0u8; ball_size];
                for i in (0..ball_size).step_by(4096) {
                    ball[i] = 0;
                }

                #[cfg(target_os = "linux")]
                if deactivate_autonuma {
                    unsafe {
                        println!("[Node 0] Applying mbind to pin the 10GB Ball to Node 0...");
                        let mut nodemask: libc::c_ulong = 1 << 0;
                        let ptr = ball.as_ptr() as usize;
                        let aligned_ptr = ptr & !(4096 - 1);
                        let offset = ptr - aligned_ptr;
                        let size = ball.len() + offset;

                        libc::syscall(
                            libc::SYS_mbind,
                            aligned_ptr as *mut libc::c_void,
                            size,
                            2, // MPOL_BIND
                            &mut nodemask,
                            64,
                            2, // MPOL_MF_MOVE
                        );
                        println!("[Node 0] 2GB Ball physically pinned to Node 0.");
                    }
                }

                println!("[Node 0] Sending ball to Node 1 and waiting passively...");
                // Send the ball
                tx_01.send(ball).unwrap();

                // Receive the ball back after Node 1 finishes 500 iterations
                let _ball = rx_10.recv().unwrap();
                println!("[Node 0] Ball received back. Test complete.");
            });
        });

        // --- WORKER NODO 1 ---
        s.spawn(|| {
            pool1.install(|| {
                println!("[Node 1] Waiting for 2GB Ball from Node 0...");

                // Receive the ball
                let mut ball = rx_01.recv().unwrap();
                println!("[Node 1] Ball received! Starting 500 iteration loop...");

                for step in 1..=500 {
                    if step % 20 == 1 || step == 500 {
                        println!("\n--- ASYMMETRIC ITERATION {} ---", step);
                    }

                    #[cfg(target_os = "linux")]
                    {
                        if step % 20 == 1 || step == 500 {
                            let (n0, n1) = check_numa_distribution(ball.as_ptr(), ball.len());
                            println!("[Node 1] RAM Distribution: {:.1}% Node 0 | {:.1}% Node 1", n0, n1);
                        }
                    }

                    let start = Instant::now();
                    for chunk in ball.chunks_exact_mut(4096) {
                        chunk[0] = chunk[0].wrapping_add(1);
                    }
                    let elapsed = start.elapsed();
                    
                    if step % 20 == 1 || step == 500 {
                        println!("[Node 1] Continuous Write (2GB) took: {:.2} ms", elapsed.as_secs_f64() * 1000.0);
                    }
                }

                // Send the ball back
                println!("[Node 1] 500 iterations complete. Sending back to Node 0...");
                tx_10.send(ball).unwrap();
            });
        });
    });
}
