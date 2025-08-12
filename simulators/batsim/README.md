# Batsim Directory Overview

This repository contains results and scripts for evaluating the performance of various algorithms in Batsim.

- Subdirectories of simulation results organized by algorithm.
- Python scripts to generate synthetic workloads, run simulations, and visualize results.

## Directory Structure

```
.
├── results/
│   ├── conservative_bf_results/
│   ├── easy_bf_results/
│   └── ...
├── scripts/
│   ├── generate_workload.py
│   ├── compare_algorithms.py
│   ├── plot_results.py
│   └── ...
└── README.md
```

- `results/`: Contains subdirectories named after each algorithm, storing simulation output data.
- `scripts/`: Contains Python scripts for generating input workloads, executing simulations, and plotting performance results.

## Scripts Overview

### `generate_workload.py`
Generates synthetic workloads used as input for simulations.

### `compare_algorithms.py`
Compares performance metrics across different algorithms.

### `plot_results.py`
Generates plots and comparative graphs from simulation output files.

## Running the Batsim Simulation

To run a simulation with Batsim and a custom scheduler, use the following steps in **two separate terminals**.

### Step 1: Launch Batsim

**Terminal 1:**
```bash
batsim -p /tmp/batsim-src-stable/platforms/cluster512.xml \
       -w sample_workload.json \
       -e "\$(pwd)/scheduler_results/scheduler_results"
```

### Step 2: Launch the Scheduler

**Terminal 2:**
```bash
batsched -v scheduler
```

---

### Example: Using the `easy_bf` Scheduler

If you're using the built-in `easy_bf` scheduler, update the paths accordingly:

**Terminal 1:**
```bash
batsim -p /tmp/batsim-src-stable/platforms/cluster512.xml \
       -w sample_workload.json \
       -e "\$(pwd)/easy_bf/easy_bf"
```

**Terminal 2:**
```bash
batsched -v easy_bf
```

---

## Dynamic Results (Work in Progress)

The current results and analyses are based on *static workloads*—that is, all jobs are known at the start of the simulation. We are looking into extending the framework to support *dynamic workloads*, where jobs arrive over time during simulation. This will allow for a more realistic evaluation of scheduling algorithms in online environments.

Future updates will include:
- Support for Batsim dynamic job submission
- Comparison of algorithm performance under dynamic conditions
