import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
import re

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
        if 'graph_generated' in ts_dict and 'worker_start' in ts_dict:
            graph_gen_time = ts_dict['graph_generated'] - ts_dict['worker_start']
            
        record = {
            'worker': worker_name,
            'Carga del Grafo': graph_gen_time,
        }

        trials = sorted(list(set([int(re.search(r'trial_(\d+)', k).group(1)) for k in ts_dict.keys() if 'trial_' in k])))
        
        for idx, t in enumerate(trials):
            if f"trial_{t}_start" in ts_dict and f"trial_{t}_end" in ts_dict:
                time_taken = ts_dict[f"trial_{t}_end"] - ts_dict[f"trial_{t}_start"]
                record[f'Trial {idx}'] = time_taken
            
            # 2. Extract Phase 1 and Communication times per iteration for each trial
            iters = sorted(list(set([int(re.search(r'_iter_(\d+)_', k).group(1)) for k in ts_dict.keys() if f"trial_{t}_iter_" in k])))
            
            for it in iters:
                k_compute = f"trial_{t}_iter_{it}_compute"
                k_alltoall = f"trial_{t}_iter_{it}_alltoall"
                k_reduce = f"trial_{t}_iter_{it}_reduce"
                k_broadcast = f"trial_{t}_iter_{it}_broadcast"
                
                k_process_prev = f"trial_{t}_iter_{it-1}_process" if it > 0 else f"trial_{t}_start"
                if k_process_prev not in ts_dict and it > 0:
                    k_process_prev = f"trial_{t}_iter_{it-1}_broadcast"
                
                if k_compute in ts_dict and k_process_prev in ts_dict:
                    compute_times.append(ts_dict[k_compute] - ts_dict[k_process_prev])
                    
                if k_alltoall in ts_dict and k_compute in ts_dict:
                    comm_times.append(ts_dict[k_alltoall] - ts_dict[k_compute])
                elif k_broadcast in ts_dict and k_compute in ts_dict:
                    comm_times.append(ts_dict[k_broadcast] - ts_dict[k_compute])

        records_global.append(record)

    # Plot 1: Worker Execution Times
    df_global = pd.DataFrame(records_global).set_index('worker')
    
    # Ensure correct column order
    trial_cols = sorted([c for c in df_global.columns if c.startswith('Trial ')])
    columns_ordered = ['Carga del Grafo'] + trial_cols
    df_global = df_global[columns_ordered]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Alternating colors: Gray for Load, then alternating Blues and Oranges for Trials
    colors = ['#888888']
    for i in range(len(trial_cols)):
        colors.append('#4C72B0' if i % 2 == 0 else '#DD8452')
        
    df_global.plot(kind='barh', stacked=True, ax=ax1, color=colors, legend=False)
    ax1.set_xlabel('Tiempo (ms)')
    ax1.set_title('Desglose de Tiempo por Worker (Trials Apilados)')
    
    import matplotlib.patches as mpatches
    gray_patch = mpatches.Patch(color='#888888', label='Carga del Grafo')
    blue_patch = mpatches.Patch(color='#4C72B0', label='Trials (Pares)')
    orange_patch = mpatches.Patch(color='#DD8452', label='Trials (Impares)')
    ax1.legend(handles=[gray_patch, blue_patch, orange_patch], loc='best')
    
    # Plot 2: Average times per iteration across all trials
    avg_compute = np.mean(compute_times) if compute_times else 0
    avg_comm = np.mean(comm_times) if comm_times else 0
    
    ax2.bar(['Cómputo Local (Fase 1)', 'Comunicación (All-to-All)'], [avg_compute, avg_comm], color=['#4C72B0', '#DD8452'])
    ax2.set_ylabel('Tiempo Medio (ms)')
    ax2.set_title('Promedios por Iteración (Todos los Trials)')
    
    for i, v in enumerate([avg_compute, avg_comm]):
        ax2.text(i, v + (v*0.01), f"{v:.2f} ms", ha='center', fontweight='bold')

    plt.tight_layout()
    out_file = 'bfs_analysis.png'
    plt.savefig(out_file)
    print(f"Gráficos generados correctamente en '{out_file}'")

if __name__ == "__main__":
    file_name = sys.argv[1] if len(sys.argv) > 1 else 'output_test_group-0.json'
    generate_charts(file_name)
