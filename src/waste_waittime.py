import os
import pandas as pd
from datetime import datetime

## This script will calculate the average pending time for jobs and the average waste in node minutes
## for preempted jobs with normal and high QOS, across all experiments. Make sure to modify the path in experiment_dirs
## to the correct location of the csv files.

experiment_dirs = [f"/Users/3ue/dev/scheduling/defiant2-experiments/exp{i}" for i in range(1, 10)]
csv_filename = "curatedjobsdata.csv"

summary = []
for exp_dir in experiment_dirs:
    path = os.path.join(exp_dir, csv_filename)
    if not os.path.exists(path):
        print(f"Missing: {path}")
        continue

    df = pd.read_csv(path)

    # Parse csv and convert to datetime
    df["Submit"] = pd.to_datetime(df["Submit"], errors="coerce")
    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["End"] = pd.to_datetime(df["End"], errors="coerce")

    # Filter preempted jobs and compute elapsed time (in minutes)
    preempted = df[df["State"].str.lower() == "preempted"].copy()
    preempted["ElapsedMin"] = (preempted["End"] - preempted["Start"]).dt.total_seconds() / 60
    preempted = preempted.dropna(subset=["ElapsedMin", "NNodes"])
    preempted["NodeMinutes"] = preempted["ElapsedMin"] * preempted["NNodes"]
    preempted["QOSType"] = preempted["QOS"].str.lower().apply(lambda x: "high" if x == "high" else "normal")
    wasted = preempted.groupby("QOSType")["NodeMinutes"].sum().to_dict()


    # Compute pending time in minutes
    df["PendingTimeMin"] = (df["Start"] - df["Submit"]).dt.total_seconds() / 60
    df["QOSType"] = df["QOS"].str.lower().apply(lambda x: "high" if x == "high" else "normal")

    # Filter valid pending times
    pending = df.dropna(subset=["PendingTimeMin"])
    avg_pending = pending.groupby("QOSType")["PendingTimeMin"].mean().to_dict()


    # Group by QOS and compute mean
    summary.append({
        "Experiment": os.path.basename(exp_dir),
        "AvgPending_HighQOS_min": round(avg_pending.get("high", 0), 2),
        "AvgPending_NormalQOS_Min": round(avg_pending.get("normal", 0), 2),
        "WastedNodeMinutes_HighQOS": round(wasted.get("high", 0), 2),
        "WastedNodeMinutes_NormalQOS": round(wasted.get("normal", 0), 2)
    })

# === Output ===
summary_df = pd.DataFrame(summary)
print(summary_df.to_string(index=False))