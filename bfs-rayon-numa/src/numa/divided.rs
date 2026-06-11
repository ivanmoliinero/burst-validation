use crate::Graph;
use super::NumaPolicy;

pub struct DividedNumaPolicy;

impl NumaPolicy for DividedNumaPolicy {
    #[allow(unused_variables)]
    fn apply_memory_policy(&self, graph: &Graph, distances: &mut [std::sync::atomic::AtomicUsize]) {
        #[cfg(target_os = "linux")]
        unsafe {
            println!("Applying mbind to physically partition the Graph RAM (NUMA Divide: TRUE)...");
            
            // First Half -> Node 0
            let mut nodemask0: libc::c_ulong = 1 << 0;
            
            // Second Half -> Node 1
            let mut nodemask1: libc::c_ulong = 1 << 1;

            let offsets_len = graph.offsets.len();
            let offsets_mid = offsets_len / 2;
            
            // First Half Offsets
            let offsets_raw_ptr_0 = graph.offsets.as_ptr() as usize;
            let offsets_aligned_ptr_0 = offsets_raw_ptr_0 & !(4096 - 1);
            let offsets_alignment_offset_0 = offsets_raw_ptr_0 - offsets_aligned_ptr_0;
            let offsets_bytes_to_move_0 = (offsets_mid * std::mem::size_of::<usize>()) + offsets_alignment_offset_0;

            libc::syscall(
                libc::SYS_mbind,
                offsets_aligned_ptr_0 as *mut libc::c_void,
                offsets_bytes_to_move_0,
                2, // MPOL_BIND
                &mut nodemask0,
                64,
                2, // MPOL_MF_MOVE
            );

            // Second Half Offsets
            let offsets_raw_ptr_1 = graph.offsets.as_ptr().add(offsets_mid) as usize;
            let offsets_aligned_ptr_1 = offsets_raw_ptr_1 & !(4096 - 1);
            let offsets_alignment_offset_1 = offsets_raw_ptr_1 - offsets_aligned_ptr_1;
            let offsets_bytes_to_move_1 = ((offsets_len - offsets_mid) * std::mem::size_of::<usize>()) + offsets_alignment_offset_1;

            libc::syscall(
                libc::SYS_mbind,
                offsets_aligned_ptr_1 as *mut libc::c_void,
                offsets_bytes_to_move_1,
                2,
                &mut nodemask1,
                64,
                2,
            );

            // First Half Edges
            let edges_mid = graph.offsets[offsets_mid];
            let edges_len = graph.edges.len();

            let edges_raw_ptr_0 = graph.edges.as_ptr() as usize;
            let edges_aligned_ptr_0 = edges_raw_ptr_0 & !(4096 - 1);
            let edges_alignment_offset_0 = edges_raw_ptr_0 - edges_aligned_ptr_0;
            let edges_bytes_to_move_0 = (edges_mid * std::mem::size_of::<usize>()) + edges_alignment_offset_0;

            libc::syscall(
                libc::SYS_mbind,
                edges_aligned_ptr_0 as *mut libc::c_void,
                edges_bytes_to_move_0,
                2,
                &mut nodemask0,
                64,
                2,
            );

            // Second Half Edges
            let edges_raw_ptr_1 = graph.edges.as_ptr().add(edges_mid) as usize;
            let edges_aligned_ptr_1 = edges_raw_ptr_1 & !(4096 - 1);
            let edges_alignment_offset_1 = edges_raw_ptr_1 - edges_aligned_ptr_1;
            let edges_bytes_to_move_1 = ((edges_len - edges_mid) * std::mem::size_of::<usize>()) + edges_alignment_offset_1;

            libc::syscall(
                libc::SYS_mbind,
                edges_aligned_ptr_1 as *mut libc::c_void,
                edges_bytes_to_move_1,
                2,
                &mut nodemask1,
                64,
                2,
            );
            
            // Distances Array
            let dist_len = distances.len();
            let dist_mid = dist_len / 2;
            
            // First Half Distances
            let dist_raw_ptr_0 = distances.as_ptr() as usize;
            let dist_aligned_ptr_0 = dist_raw_ptr_0 & !(4096 - 1);
            let dist_alignment_offset_0 = dist_raw_ptr_0 - dist_aligned_ptr_0;
            let dist_bytes_to_move_0 = (dist_mid * std::mem::size_of::<std::sync::atomic::AtomicUsize>()) + dist_alignment_offset_0;

            libc::syscall(
                libc::SYS_mbind,
                dist_aligned_ptr_0 as *mut libc::c_void,
                dist_bytes_to_move_0,
                2,
                &mut nodemask0,
                64,
                2,
            );

            // Second Half Distances
            let dist_raw_ptr_1 = distances.as_ptr().add(dist_mid) as usize;
            let dist_aligned_ptr_1 = dist_raw_ptr_1 & !(4096 - 1);
            let dist_alignment_offset_1 = dist_raw_ptr_1 - dist_aligned_ptr_1;
            let dist_bytes_to_move_1 = ((dist_len - dist_mid) * std::mem::size_of::<std::sync::atomic::AtomicUsize>()) + dist_alignment_offset_1;

            libc::syscall(
                libc::SYS_mbind,
                dist_aligned_ptr_1 as *mut libc::c_void,
                dist_bytes_to_move_1,
                2,
                &mut nodemask1,
                64,
                2,
            );

            println!("Graph and Distances physically partitioned between Node 0 and Node 1.");
        }
    }

    #[allow(unused_variables)]
    fn apply_thread_policy(&self, worker_id: u32) {
        #[cfg(target_os = "linux")]
        unsafe {
            let target_node = worker_id % 2; // worker 0 -> Node 0, worker 1 -> Node 1

            // 1. Pin Memory (MPOL_BIND)
            let mut nodemask: libc::c_ulong = 1 << target_node;
            libc::syscall(libc::SYS_set_mempolicy, 2, &mut nodemask, 64);

            // 2. Pin CPU (sched_setaffinity)
            let cpulist_path = format!("/sys/devices/system/node/node{}/cpulist", target_node);
            if let Ok(cpulist) = std::fs::read_to_string(&cpulist_path) {
                let mut cpus = Vec::new();
                for part in cpulist.trim().split(',') {
                    let bounds: Vec<&str> = part.split('-').collect();
                    if bounds.len() == 1 {
                        if let Ok(c) = bounds[0].parse::<usize>() {
                            cpus.push(c);
                        }
                    } else if bounds.len() == 2 {
                        if let (Ok(start), Ok(end)) =
                            (bounds[0].parse::<usize>(), bounds[1].parse::<usize>())
                        {
                            for c in start..=end {
                                cpus.push(c);
                            }
                        }
                    }
                }
                if !cpus.is_empty() {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    for cpu in cpus {
                        libc::CPU_SET(cpu, &mut set);
                    }
                    libc::sched_setaffinity(
                        0,
                        std::mem::size_of::<libc::cpu_set_t>(),
                        &set,
                    );
                } // else, do not pin
            }
        }
    }
}
