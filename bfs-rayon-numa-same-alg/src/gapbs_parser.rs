use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use log::info;

#[derive(Clone)]
pub struct Graph {
    pub offsets: Vec<usize>,
    pub edges: Vec<usize>,
}

impl Graph {
    pub fn num_nodes(&self) -> usize {
        if self.offsets.is_empty() { 0 } else { self.offsets.len() - 1 }
    }

    pub fn degree(&self, u: usize) -> usize {
        self.offsets[u + 1] - self.offsets[u]
    }

    pub fn get_neighbors(&self, u: usize) -> &[usize] {
        let start = self.offsets[u];
        let end = self.offsets[u + 1];
        &self.edges[start..end]
    }

    /// Loads a graph from a text file, specifically edge list formats (.el) or similar.
    /// Handles lines starting with '#' as comments.
    pub fn from_el_file<P: AsRef<Path>>(path: P) -> Self {
        let file = File::open(path).expect("Failed to open graph file");
        let reader = BufReader::new(file);

        let mut temp_edges = Vec::new();
        let mut max_node = 0;

        for (line_num, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            if line.starts_with('#') || line.starts_with('%') || line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let (Ok(u), Ok(v)) = (parts[0].parse::<usize>(), parts[1].parse::<usize>()) {
                    temp_edges.push((u, v));
                    if u > max_node { max_node = u; }
                    if v > max_node { max_node = v; }
                } else {
                    log::warn!("Failed to parse line {}: {}", line_num + 1, line);
                }
            }
        }

        let num_nodes = max_node + 1;
        let mut degree_count = vec![0usize; num_nodes];
        for &(u, _) in &temp_edges {
            degree_count[u] += 1;
        }

        let mut offsets = vec![0usize; num_nodes + 1];
        for i in 0..num_nodes {
            offsets[i + 1] = offsets[i] + degree_count[i];
        }

        let mut edges = vec![0usize; temp_edges.len()];
        let mut current_offset = offsets.clone();
        for (u, v) in temp_edges {
            let idx = current_offset[u];
            edges[idx] = v;
            current_offset[u] += 1;
        }

        Graph { offsets, edges }
    }

    pub fn from_el_file_partitioned<P: AsRef<Path>>(path: P, start_node: usize, end_node: usize) -> Self {
        let file = File::open(path).expect("Failed to open graph file");
        let reader = BufReader::new(file);

        let mut temp_edges = Vec::new();
        let mut max_node = 0;

        for (line_num, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            if line.starts_with('#') || line.starts_with('%') || line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let (Ok(u), Ok(v)) = (parts[0].parse::<usize>(), parts[1].parse::<usize>()) {
                    if u > max_node { max_node = u; }
                    if v > max_node { max_node = v; }
                    
                    if u >= start_node && u < end_node {
                        temp_edges.push((u, v));
                    }
                } else {
                    log::warn!("Failed to parse line {}: {}", line_num + 1, line);
                }
            }
        }

        let num_nodes = max_node + 1;
        let mut degree_count = vec![0usize; num_nodes];
        for &(u, _) in &temp_edges {
            degree_count[u] += 1;
        }

        let mut offsets = vec![0usize; num_nodes + 1];
        for i in 0..num_nodes {
            offsets[i + 1] = offsets[i] + degree_count[i];
        }

        let mut edges = vec![0usize; temp_edges.len()];
        let mut current_offset = offsets.clone();
        for (u, v) in temp_edges {
            let idx = current_offset[u];
            edges[idx] = v;
            current_offset[u] += 1;
        }

        Graph { offsets, edges }
    }

    /// Loads a graph from the GAPBS binary format (.sg).
    /// GAPBS .sg format:
    /// - bool: directed
    /// - uint64_t: num_edges
    /// - uint64_t: num_nodes
    /// - array of uint64_t of size (num_nodes + 1): offsets
    /// - array of int32_t of size num_edges: destinations
    pub fn from_sg_file<P: AsRef<Path>>(path: P) -> Self {
        let file = File::open(path).expect("Failed to open .sg file");
        let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file);
        
        let mut bool_buf = [0u8; 1];
        reader.read_exact(&mut bool_buf).expect("Failed to read directed flag");
        let _directed = bool_buf[0] != 0;

        let mut u64_buf = [0u8; 8];
        
        reader.read_exact(&mut u64_buf).expect("Failed to read num_edges");
        let num_edges = u64_le(&u64_buf) as usize;

        reader.read_exact(&mut u64_buf).expect("Failed to read num_nodes");
        let num_nodes = u64_le(&u64_buf) as usize;

        let mut offsets = vec![0usize; num_nodes + 1];
        for i in 0..=num_nodes {
            reader.read_exact(&mut u64_buf).expect("Failed to read offset");
            offsets[i] = u64_le(&u64_buf) as usize;
        }

        let mut edges = Vec::with_capacity(num_edges);
        let mut i32_buf = [0u8; 4];
        
        for _ in 0..num_edges {
            reader.read_exact(&mut i32_buf).expect("Failed to read edge destination");
            let v = i32_le(&i32_buf) as usize;
            edges.push(v);
        }

        Graph { offsets, edges }
    }

    pub fn from_sg_file_partitioned<P: AsRef<Path>>(path: P, start_node: usize, end_node: usize) -> Self {
        let file = File::open(path).expect("Failed to open .sg file");
        let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file);
        
        let mut bool_buf = [0u8; 1];
        reader.read_exact(&mut bool_buf).expect("Failed to read directed flag");
        let _directed = bool_buf[0] != 0;

        let mut u64_buf = [0u8; 8];
        
        reader.read_exact(&mut u64_buf).expect("Failed to read num_edges");
        let _num_edges_total = u64_le(&u64_buf) as usize;

        reader.read_exact(&mut u64_buf).expect("Failed to read num_nodes");
        let num_nodes = u64_le(&u64_buf) as usize;

        let mut original_offsets = vec![0usize; num_nodes + 1];
        for i in 0..=num_nodes {
            reader.read_exact(&mut u64_buf).expect("Failed to read offset");
            original_offsets[i] = u64_le(&u64_buf) as usize;
        }
        
        let start_n = start_node.min(num_nodes);
        let end_n = end_node.min(num_nodes);
        
        let edges_to_skip = original_offsets[start_n];
        let edges_to_read = original_offsets[end_n] - original_offsets[start_n];

        // Skip edges before our partition
        if edges_to_skip > 0 {
            let skip_bytes = (edges_to_skip * 4) as u64;
            std::io::copy(&mut reader.by_ref().take(skip_bytes), &mut std::io::sink()).expect("Failed to skip edges");
        }

        let mut edges = Vec::with_capacity(edges_to_read);
        let mut i32_buf = [0u8; 4];
        
        for _ in 0..edges_to_read {
            reader.read_exact(&mut i32_buf).expect("Failed to read edge destination");
            let v = i32_le(&i32_buf) as usize;
            edges.push(v);
        }

        let mut new_offsets = vec![0usize; num_nodes + 1];
        for u in 0..=num_nodes {
            if u < start_n {
                new_offsets[u] = 0;
            } else if u <= end_n {
                new_offsets[u] = original_offsets[u] - original_offsets[start_n];
            } else {
                new_offsets[u] = edges_to_read;
            }
        }

        Graph { offsets: new_offsets, edges }
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

    pub fn from_file_partitioned<P: AsRef<Path>>(path: P, start_node: usize, end_node: usize) -> Self {
        let p = path.as_ref();
        info!("Loading graph partition [{}, {}) from {:?}", start_node, end_node, p);
        if p.extension().and_then(|e| e.to_str()) == Some("sg") {
            Self::from_sg_file_partitioned(p, start_node, end_node)
        } else {
            Self::from_el_file_partitioned(p, start_node, end_node)
        }
    }

    pub fn new_grid(rows: usize, cols: usize) -> Self {
        let num_nodes = rows * cols;
        
        let mut degree_count = vec![0usize; num_nodes];
        for r in 0..rows {
            for c in 0..cols {
                let u = r * cols + c;
                if r > 0 { degree_count[u] += 1; }
                if r < rows - 1 { degree_count[u] += 1; }
                if c > 0 { degree_count[u] += 1; }
                if c < cols - 1 { degree_count[u] += 1; }
            }
        }
        
        let mut offsets = vec![0usize; num_nodes + 1];
        for i in 0..num_nodes {
            offsets[i + 1] = offsets[i] + degree_count[i];
        }

        let num_edges = offsets[num_nodes];
        let mut edges = vec![0usize; num_edges];
        let mut current_offset = offsets.clone();

        for r in 0..rows {
            for c in 0..cols {
                let u = r * cols + c;
                if r > 0 { 
                    edges[current_offset[u]] = (r - 1) * cols + c;
                    current_offset[u] += 1;
                }
                if r < rows - 1 { 
                    edges[current_offset[u]] = (r + 1) * cols + c;
                    current_offset[u] += 1;
                }
                if c > 0 { 
                    edges[current_offset[u]] = r * cols + c - 1;
                    current_offset[u] += 1;
                }
                if c < cols - 1 { 
                    edges[current_offset[u]] = r * cols + c + 1;
                    current_offset[u] += 1;
                }
            }
        }

        Graph { offsets, edges }
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
