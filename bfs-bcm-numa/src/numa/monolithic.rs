use crate::Graph;
use super::NumaPolicy;

pub struct MonolithicNumaPolicy;

impl NumaPolicy for MonolithicNumaPolicy {
    fn apply_memory_policy(&self, graph: &Graph) {
        #[cfg(target_os = "linux")]
        unsafe {
            println!("Applying mbind to force entire Graph into Node 0 (NUMA Divide: FALSE)...");
            let mut nodemask: libc::c_ulong = 1 << 0;

            // Offsets
            let offsets_raw_ptr = graph.offsets.as_ptr() as usize;
            let offsets_aligned_ptr = offsets_raw_ptr & !(4096 - 1);
            let offsets_alignment_offset = offsets_raw_ptr - offsets_aligned_ptr;
            let offsets_bytes_to_move = (graph.offsets.len() * std::mem::size_of::<usize>()) + offsets_alignment_offset;

            libc::syscall(
                libc::SYS_mbind,
                offsets_aligned_ptr as *mut libc::c_void,
                offsets_bytes_to_move,
                2,
                &mut nodemask,
                64,
                2,
            );

            // Edges
            if !graph.edges.is_empty() {
                let edges_raw_ptr = graph.edges.as_ptr() as usize;
                let edges_aligned_ptr = edges_raw_ptr & !(4096 - 1);
                let edges_alignment_offset = edges_raw_ptr - edges_aligned_ptr;
                let edges_bytes_to_move = (graph.edges.len() * std::mem::size_of::<usize>()) + edges_alignment_offset;

                libc::syscall(
                    libc::SYS_mbind,
                    edges_aligned_ptr as *mut libc::c_void,
                    edges_bytes_to_move,
                    2,
                    &mut nodemask,
                    64,
                    2,
                );
            }
            println!("Entire Graph physically pinned to Node 0.");
        }
    }

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
