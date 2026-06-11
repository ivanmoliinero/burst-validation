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
                println!("[Node 0] Reserving 20GB of static padding...");
                let padding_size = 20 * 1024 * 1024 * 1024; // 20 GB
                let mut static_padding = vec![0u8; padding_size];
                for i in (0..padding_size).step_by(4096) {
                    unsafe {
                        std::ptr::write_volatile(&mut static_padding[i], 1);
                    } // Force page fault, block LLVM optimizations
                }
                println!("[Node 0] 20GB static padding isolated.");

                let ball_size = 10 * 1024 * 1024 * 1024; // 10 GB
                println!("[Node 0] Generating 10GB Ball...");
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
                        println!("[Node 0] 10GB Ball physically pinned to Node 0.");
                    }
                }

                for step in 1..=20 {
                    println!("\n--- PING PONG ITERATION {} ---", step);
                    
                    let start = Instant::now();
                    for chunk in ball.chunks_exact_mut(4096) {
                        chunk[0] = chunk[0].wrapping_add(1);
                    }
                    let elapsed = start.elapsed();
                    println!("[Node 0] Local Write (10GB) took: {:.2} ms", elapsed.as_secs_f64() * 1000.0);

                    // Send the ball
                    tx_01.send(ball).unwrap();

                    // Receive the ball back
                    ball = rx_10.recv().unwrap();
                }

                std::thread::sleep(Duration::from_secs(3600)); // Hold memory
            });
        });

        // --- WORKER NODO 1 ---
        s.spawn(|| {
            pool1.install(|| {
                println!("[Node 1] Reserving 20GB of static padding...");
                let padding_size = 20 * 1024 * 1024 * 1024; // 20 GB
                let mut static_padding = vec![0u8; padding_size];
                for i in (0..padding_size).step_by(4096) {
                    unsafe {
                        std::ptr::write_volatile(&mut static_padding[i], 1);
                    } // Force page fault, block LLVM optimizations
                }
                println!("[Node 1] 20GB static padding isolated.");

                for _step in 1..=20 {
                    // Receive the ball
                    let mut ball = rx_01.recv().unwrap();

                    let start = Instant::now();
                    for chunk in ball.chunks_exact_mut(4096) {
                        chunk[0] = chunk[0].wrapping_add(1);
                    }
                    let elapsed = start.elapsed();
                    println!("[Node 1] Remote Write (10GB NUMA MISS) took: {:.2} ms", elapsed.as_secs_f64() * 1000.0);

                    // Send the ball back
                    tx_10.send(ball).unwrap();
                }

                std::thread::sleep(Duration::from_secs(3600)); // Hold memory
            });
        });
    });
}
