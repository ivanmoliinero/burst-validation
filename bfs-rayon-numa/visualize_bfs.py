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
            graph_gen_time = (ts_dict['graph_generated'] - ts_dict['worker_start']) / 1000.0
            
        record = {
            'worker': worker_name,
            'Carga del Grafo': graph_gen_time,
        }

        trials = sorted(list(set([int(re.search(r'trial_(\d+)', k).group(1)) for k in ts_dict.keys() if 'trial_' in k])))
        
        for idx, t in enumerate(trials):
            if f"trial_{t}_start" in ts_dict and f"trial_{t}_end" in ts_dict:
                time_taken = (ts_dict[f"trial_{t}_end"] - ts_dict[f"trial_{t}_start"]) / 1000.0
                record[f'Trial {idx}'] = time_taken
            
            # 2. Extract Processing times per iteration for each trial
            iters = sorted(list(set([int(re.search(r'_iter_(\d+)_', k).group(1)) for k in ts_dict.keys() if f"trial_{t}_iter_" in k])))
            
            comm_times = [] # we will define comm_times globally later
            
            for it in iters:
                k_compute = f"trial_{t}_iter_{it}_compute"
                k_crossbeam = f"trial_{t}_iter_{it}_crossbeam"
                k_process = f"trial_{t}_iter_{it}_process"
                
                if k_compute in ts_dict and k_crossbeam in ts_dict:
                    compute_times.append((ts_dict[k_crossbeam] - ts_dict[k_compute]) / 1000.0)
                
                if k_crossbeam in ts_dict and k_process in ts_dict:
                    comm_times.append((ts_dict[k_process] - ts_dict[k_crossbeam]) / 1000.0)

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
    
    ax2.bar(['Cómputo Local (Rayon)', 'Comunicación (Crossbeam)'], [avg_compute, avg_comm], color=['#4C72B0', '#DD8452'], width=0.5)
    ax2.set_ylabel('Tiempo Medio (ms)')
    ax2.set_title('Promedios por Iteración (Todos los Trials)')
    ax2.set_xlim(-0.5, 1.5)
    
    for i, v in enumerate([avg_compute, avg_comm]):
        ax2.text(i, v + (v*0.01), f"{v:.2f} ms", ha='center', fontweight='bold')


    min_start = float('inf')
    max_end = 0
    for worker_data in data:
        ts_dict = {ts['key']: int(ts['value']) for ts in worker_data.get('timestamps', [])}
        if 'worker_start' in ts_dict:
            min_start = min(min_start, ts_dict['worker_start'])
        if 'worker_end' in ts_dict:
            max_end = max(max_end, ts_dict['worker_end'])
            
    if min_start < float('inf') and max_end > 0:
        total_time_sec = (max_end - min_start) / 1000000.0
        fig.suptitle(f"Tiempo Total de Ejecución: {total_time_sec:.2f} segundos", fontsize=16, fontweight='bold')
        print(f"Tiempo Total de Ejecución: {total_time_sec:.2f} segundos")

    plt.tight_layout()
    if min_start < float('inf') and max_end > 0:
        fig.subplots_adjust(top=0.9)

    out_file =  'bfs_rayon_analysis.png'
    plt.savefig(out_file)
    print(f"Gráficos generados correctamente en '{out_file}'")


if __name__ == "__main__":
    file_name = sys.argv[1] if len(sys.argv) > 1 else 'output_bfs_group-0.json'
    generate_charts(file_name)
