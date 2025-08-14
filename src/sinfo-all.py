import os
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import alpha

## This script will plot all the node utilized (allocated + completing) as percent utilized on the machine.
## All nine experiments will be displayed in a 3 x 3 plot with different y-axis. This plot also includes shading.
## This code also calculates and prints the average percent utilized node for each experiment.

# List of experiment folders
experiment_dirs = [f"/Users/3ue/dev/scheduling/defiant2-experiments/exp{i}" for i in range(1, 10)]
csv_filename = "sinfo.csv"

# Plot setup
fig, axes = plt.subplots(3, 3, figsize=(15, 10), sharex=True, sharey=True)
axes = axes.flatten()  # for easy indexing

for i, exp_dir in enumerate(experiment_dirs):
    file_path = os.path.join(exp_dir, csv_filename)

    # Read the CSV file
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()

        # Combine allocated + completing
        df["active"] = df["allocated"] + df["completing"]
        df["Percent Utilized"] = df["active"] / 20 * 100
        avg_pcnt_utilized = df["Percent Utilized"].mean()
        print(f"Experiment {i + 1}: Average Percent Utilized Nodes = {avg_pcnt_utilized:.2f}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        continue

    ax = axes[i]
    #ax.plot(df["min"], df["reserved"], label="Reserved", color="blue")
    #ax.plot(df["min"], df["allocated"], label="Allocated", color="green")
    #ax.plot(df["min"], df["completing"], label="Completing", color="orange")
    ax.plot(df["min"], df["Percent Utilized"], label="Utilized", color="green")
    ax.fill_between(df["min"], df["Percent Utilized"], color="green", alpha=0.1)

    ax.set_title(f"Experiment {i + 1}")
    ax.set_xlabel("Time (min)")
    ax.set_ylabel("Percent Utilized Node Count")
    ax.set_xlim([0, 200])
    ax.set_ylim([0, 105])
    ax.grid(True)

    # Add legend only to first plot to reduce clutter
    #if i == 0:
    #    ax.legend(loc="upper right")

# Adjust layout
plt.tight_layout()
plt.suptitle("Node Usage Over Time Across Experiments", fontsize=16, y=1.0)
plt.subplots_adjust(top=0.9)  # make room for suptitle

plt.show()
