use clap::Parser;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {}

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
    let _args = Args::parse();
    
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

    println!("Initializing Tokio Async Channels...");
    // Tokio asynchronous channels
    let (tx_01, mut rx_01) = mpsc::channel::<Vec<u8>>(1);
    let (tx_10, mut rx_10) = mpsc::channel::<Vec<u8>>(1);

    std::thread::scope(|s| {
        // --- WORKER NODO 0 ---
        s.spawn(move || {
            #[cfg(target_os = "linux")]
            unsafe {
                let target_node = 0;
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                if let Some(ref cpus) = cpus_node0 {
                    libc::CPU_SET(cpus[0], &mut set);
                    libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
                }
                let mut nodemask: libc::c_ulong = 1 << target_node;
                libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);
            }

            // Create a Current-Thread Tokio Runtime bound to this thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();

            rt.block_on(async move {
                println!("[Node 0] Reserving 20GB of static padding...");
                let padding_size = 20 * 1024 * 1024 * 1024; // 20 GB
                let mut static_padding = vec![0u8; padding_size];
                for i in (0..padding_size).step_by(4096) {
                    unsafe { std::ptr::write_volatile(&mut static_padding[i], 1); }
                }
                println!("[Node 0] 20GB static padding isolated.");

                let ball_size = 2 * 1024 * 1024 * 1024; // 2 GB
                println!("[Node 0] Generating 2GB Ball...");
                let mut ball = vec![0u8; ball_size];
                for i in (0..ball_size).step_by(4096) {
                    ball[i] = 0;
                }

                for step in 1..=5 {
                    println!("\n--- ASYNC PING PONG ITERATION {} ---", step);
                    
                    let start = Instant::now();
                    for i in (0..ball_size).step_by(4096) {
                        ball[i] = ball[i].wrapping_add(1);
                    }
                    let elapsed = start.elapsed();
                    println!("[Node 0] Local Write (2GB) took: {:.2} ms", elapsed.as_secs_f64() * 1000.0);

                    // Send the ball asynchronously
                    tx_01.send(ball).await.unwrap();

                    // Receive the ball back asynchronously
                    ball = rx_10.recv().await.unwrap();
                }

                // Hold memory
                std::thread::sleep(Duration::from_secs(3600)); 
            });
        });

        // --- WORKER NODO 1 ---
        s.spawn(move || {
            #[cfg(target_os = "linux")]
            unsafe {
                let target_node = 1;
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                if let Some(ref cpus) = cpus_node1 {
                    libc::CPU_SET(cpus[0], &mut set);
                    libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
                }
                let mut nodemask: libc::c_ulong = 1 << target_node;
                libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);
            }

            // Create a Current-Thread Tokio Runtime bound to this thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();

            rt.block_on(async move {
                println!("[Node 1] Reserving 20GB of static padding...");
                let padding_size = 20 * 1024 * 1024 * 1024; // 20 GB
                let mut static_padding = vec![0u8; padding_size];
                for i in (0..padding_size).step_by(4096) {
                    unsafe { std::ptr::write_volatile(&mut static_padding[i], 1); }
                }
                println!("[Node 1] 20GB static padding isolated.");

                for _step in 1..=5 {
                    // Receive the ball asynchronously
                    let mut ball = rx_01.recv().await.unwrap();

                    let start = Instant::now();
                    for i in (0..ball.len()).step_by(4096) {
                        ball[i] = ball[i].wrapping_add(1);
                    }
                    let elapsed = start.elapsed();
                    println!("[Node 1] Remote Write (2GB NUMA MISS) took: {:.2} ms", elapsed.as_secs_f64() * 1000.0);

                    // Send the ball back asynchronously
                    tx_10.send(ball).await.unwrap();
                }

                // Hold memory
                std::thread::sleep(Duration::from_secs(3600)); 
            });
        });
    });
}
