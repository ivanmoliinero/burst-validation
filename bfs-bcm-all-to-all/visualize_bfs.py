import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys

def generate_charts(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    records_global = []
    compute_times = []
    comm_times = []

    for i, worker_data in enumerate(data):
        worker_id = worker_data.get('worker_id', i)
        worker_name = f"Worker {worker_id}"
        
        ts_dict = {ts['key']: int(ts['value']) for ts in worker_data.get('timestamps', [])}
        
        # 1. Global Processing Times
        graph_gen_time = 0
        total_proc_time = 0
        
        if 'graph_generated' in ts_dict and 'worker_start' in ts_dict:
            graph_gen_time = ts_dict['graph_generated'] - ts_dict['worker_start']
            
        if 'worker_end' in ts_dict and 'graph_generated' in ts_dict:
            total_proc_time = ts_dict['worker_end'] - ts_dict['graph_generated']
            
        records_global.append({
            'worker': worker_name,
            'Carga del Grafo': graph_gen_time,
            'Procesado del Grafo': total_proc_time
        })
        
        # 2. Extract Phase 1 and Communication times per iteration
        iters = sorted(list(set([int(k.split('_')[1]) for k in ts_dict.keys() if k.startswith('iter_')])))
        
        for it in iters:
            k_compute = f"iter_{it}_compute"
            k_alltoall = f"iter_{it}_alltoall"
            k_process_prev = f"iter_{it-1}_process" if it > 0 else "graph_generated"
            
            if k_compute in ts_dict and k_process_prev in ts_dict:
                compute_times.append(ts_dict[k_compute] - ts_dict[k_process_prev])
                
            if k_alltoall in ts_dict and k_compute in ts_dict:
                comm_times.append(ts_dict[k_alltoall] - ts_dict[k_compute])

    # Plot 1: Worker Execution Times
    df_global = pd.DataFrame(records_global).set_index('worker')
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    df_global.plot(kind='bar', stacked=True, ax=ax1, colormap='Set2')
    ax1.set_ylabel('Tiempo (ms)')
    ax1.set_title('Desglose de Tiempo por Worker')
    ax1.tick_params(axis='x', rotation=0)
    
    # Plot 2: Average times
    avg_compute = np.mean(compute_times) if compute_times else 0
    avg_comm = np.mean(comm_times) if comm_times else 0
    
    ax2.bar(['Cómputo Local (Fase 1)', 'Comunicación (All-to-All)'], [avg_compute, avg_comm], color=['#4C72B0', '#DD8452'])
    ax2.set_ylabel('Tiempo Medio (ms)')
    ax2.set_title('Promedios por Iteración')
    
    for i, v in enumerate([avg_compute, avg_comm]):
        ax2.text(i, v + (v*0.01), f"{v:.2f} ms", ha='center', fontweight='bold')

    plt.tight_layout()
    out_file = 'bfs_analysis.png'
    plt.savefig(out_file)
    print(f"Gráficos generados correctamente en '{out_file}'")

if __name__ == "__main__":
    file_name = sys.argv[1] if len(sys.argv) > 1 else 'output_test_group-0.json'
    generate_charts(file_name)
