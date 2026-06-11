pub fn bind_thread_to_node(node_id: u32) {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut nodemask: libc::c_ulong = 1 << node_id;
        let ret = libc::syscall(
            libc::SYS_set_mempolicy,
            2, // MPOL_BIND
            &mut nodemask as *mut _ as *mut libc::c_ulong,
            64,
        );
        if ret != 0 {
            log::warn!("Failed to set mempolicy for Node {}. AutoNUMA might still be active.", node_id);
        } else {
            log::info!("Successfully bound memory to NUMA Node {}", node_id);
        }

        // 2. Pin CPU (sched_setaffinity)
        let cpulist_path = format!("/sys/devices/system/node/node{}/cpulist", node_id);
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
                let aff_ret = libc::sched_setaffinity(
                    0,
                    std::mem::size_of::<libc::cpu_set_t>(),
                    &set,
                );
                if aff_ret != 0 {
                    log::warn!("Failed to set CPU affinity for Node {}", node_id);
                } else {
                    log::info!("Successfully bound thread to CPUs of NUMA Node {}", node_id);
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        // No-op for non-linux systems (macOS, Windows)
        let _ = node_id;
    }
}
