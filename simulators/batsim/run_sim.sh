#!/bin/bash

set -euo pipefail

declare -a schedulers=("easy_bf" "fcfs" "conservative_bf" "filler" "fcfs_fast" "easy_bf_fast")

platform_path="/tmp/batsim-src-stable/platforms/cluster512.xml"
workload_path="/home/er3/ORNL-Work/Simulators/batsim/output_workload.json"

for sched in "${schedulers[@]}"
do
    echo "================================================="
    echo "Running simulation with scheduler: $sched"
    echo "================================================="

    # Define and clear output directory
    outdir="$PWD/${sched}_results"
    rm -rf "$outdir"
    mkdir -p "$outdir"

    # Make sure no lingering batsim or batsched processes exist
    killall -9 batsim batsched || true
    sleep 1

    # Start batsim in background with debug logging
    BATSIM_LOG=debug batsim --daemon\
        -p "$platform_path" \
        -w "$workload_path" \
        -e "$outdir/out" \
        > "$outdir/batsim.log" 2>&1 &
    #batsim -p /tmp/batsim-src-stable/platforms/cluster512.xml        -w sample_workload.json -e "$outdir/out" &
    #batsim -p sample_xml.xml -w sample_workload.json -e "$outdir/out" &

    BATSIM_PID=$!

    # Wait for batsim to start listening
    SOCKET="/tmp/batsim.sock"
    for i in {1..30}; do
        if [[ -S "$SOCKET" ]]; then
            echo "Batsim socket ready"
            break
        fi
        echo "Waiting for Batsim socket..."
        sleep 1
    done

    # Start batsched with variant and debug logging
    batsched -v "$sched" > "$outdir/batsched.log" 2>&1

    # Wait for batsim to complete
    wait "$BATSIM_PID"

    echo "Finished $sched"
done
