import os
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import alpha

## This script will plot the node utilized (allocated + completing) as percent utilized on the machine
## for one experiment at a time with deminsions better suited for publication. The deminsions can be modified.
## This plot also includes shading. This code also calculates and prints the average percent utilized node for the experiment.

# List of experiment folders
exp_to_plot = 1
experiment_dir = f"/Users/3ue/dev/scheduling/defiant2-experiments/exp{exp_to_plot}"
csv_filename = "sinfo.csv"
file_path = os.path.join(experiment_dir, csv_filename)

# Read the CSV file
try:
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()

except FileNotFoundError:
    print(f"File not found: {file_path}")
    exit()

# Combine allocated + completing
df["active"] = df["allocated"] + df["completing"]
df["Percent Utilized"] = df["active"] / 20 * 100
avg_pcnt_utilized = df["Percent Utilized"].mean()
print(f"Experiment {exp_to_plot}: Average Percent Utilized Nodes = {avg_pcnt_utilized:.2f}")

# Plot
ONE_MM = 1 / 25.4
fig, ax = plt.subplots(figsize=(170 * ONE_MM, 70 * ONE_MM)) # 170 mm x 70 mm
#ax.plot(df["min"], df["reserved"], label="Reserved", color="blue")
#ax.plot(df["min"], df["allocated"], label="Allocated", color="green")
#ax.plot(df["min"], df["completing"], label="Completing", color="orange")
ax.plot(df["min"], df["Percent Utilized"], label="Utilized", color="green")
ax.fill_between(df["min"], df["Percent Utilized"], color="green", alpha=0.1)

ax.set_xlim(left=0)
ax.set_ylim(bottom=0)
ax.set_title(f"Experiment {exp_to_plot}")
ax.set_xlabel("Time (min)")
ax.set_ylabel("Percent Utilized Node Count")
ax.grid(True)
#ax.legend(loc="upper right")

plt.tight_layout()
plt.show()
