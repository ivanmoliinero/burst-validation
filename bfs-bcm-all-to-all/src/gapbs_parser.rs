use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use log::info;

pub struct Graph {
    pub adj: Vec<Vec<usize>>,
}

impl Graph {
    /// Loads a graph from a text file, specifically edge list formats (.el) or similar.
    /// Handles lines starting with '#' as comments.
    pub fn from_el_file<P: AsRef<Path>>(path: P) -> Self {
        let file = File::open(path).expect("Failed to open graph file");
        let reader = BufReader::new(file);

        let mut edges = Vec::new();
        let mut max_node = 0;

        for (line_num, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            if line.starts_with('#') || line.starts_with('%') || line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let (Ok(u), Ok(v)) = (parts[0].parse::<usize>(), parts[1].parse::<usize>()) {
                    edges.push((u, v));
                    if u > max_node { max_node = u; }
                    if v > max_node { max_node = v; }
                } else {
                    log::warn!("Failed to parse line {}: {}", line_num + 1, line);
                }
            }
        }

        let num_nodes = max_node + 1;
        let mut adj = vec![vec![]; num_nodes];
        for (u, v) in edges {
            adj[u].push(v);
        }

        Graph { adj }
    }

    /// Loads a graph from the GAPBS binary format (.sg).
    /// GAPBS .sg format:
    /// - uint64_t: num_nodes
    /// - uint64_t: num_edges
    /// - array of uint64_t of size (num_nodes + 1): offsets
    /// - array of int32_t of size num_edges: destinations
    pub fn from_sg_file<P: AsRef<Path>>(path: P) -> Self {
        let mut file = File::open(path).expect("Failed to open .sg file");
        
        let mut bool_buf = [0u8; 1];
        file.read_exact(&mut bool_buf).expect("Failed to read directed flag");
        let _directed = bool_buf[0] != 0;

        let mut u64_buf = [0u8; 8];
        
        file.read_exact(&mut u64_buf).expect("Failed to read num_edges");
        let _num_edges = u64_le(&u64_buf) as usize;

        file.read_exact(&mut u64_buf).expect("Failed to read num_nodes");
        let num_nodes = u64_le(&u64_buf) as usize;

        let mut offsets = vec![0usize; num_nodes + 1];
        for i in 0..=num_nodes {
            file.read_exact(&mut u64_buf).expect("Failed to read offset");
            offsets[i] = u64_le(&u64_buf) as usize;
        }

        let mut adj = vec![vec![]; num_nodes];
        let mut i32_buf = [0u8; 4];
        
        for u in 0..num_nodes {
            let degree = offsets[u + 1] - offsets[u];
            adj[u] = Vec::with_capacity(degree);
            for _ in 0..degree {
                file.read_exact(&mut i32_buf).expect("Failed to read edge destination");
                let v = i32_le(&i32_buf) as usize;
                adj[u].push(v);
            }
        }

        Graph { adj }
    }

    /// Auto-detects the format based on the extension.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        let p = path.as_ref();
        info!("Loading graph from {:?}", p);
        if p.extension().and_then(|e| e.to_str()) == Some("sg") {
            Self::from_sg_file(p)
        } else {
            Self::from_el_file(p)
        }
    }

    pub fn new_grid(rows: usize, cols: usize) -> Self {
        let mut adj = vec![vec![]; rows * cols];
        for r in 0..rows {
            for c in 0..cols {
                let u = r * cols + c;
                if r > 0 {
                    adj[u].push((r - 1) * cols + c);
                }
                if r < rows - 1 {
                    adj[u].push((r + 1) * cols + c);
                }
                if c > 0 {
                    adj[u].push(r * cols + c - 1);
                }
                if c < cols - 1 {
                    adj[u].push(r * cols + c + 1);
                }
            }
        }
        Graph { adj }
    }
}

#[inline(always)]
fn u64_le(bytes: &[u8; 8]) -> u64 {
    u64::from_le_bytes(*bytes)
}

#[inline(always)]
fn i32_le(bytes: &[u8; 4]) -> i32 {
    i32::from_le_bytes(*bytes)
}
