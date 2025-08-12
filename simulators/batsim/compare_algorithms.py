import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from scipy.stats import mannwhitneyu
from itertools import combinations

import pandas as pd
import plotly.express as px

# Evalys imports
from evalys.jobset import JobSet
from evalys import visu
import matplotlib.pyplot as plt

# Setup paths and schedulers
base_dir = Path.cwd()
schedulers = {
    "conservative_bf": base_dir / "conservative_bf_results",
    "easy_bf": base_dir / "easy_bf_results",
    "easy_bf_fast": base_dir / "easy_bf_fast_results",
    "filler": base_dir / "filler_results",
    "fcfs": base_dir / "fcfs_results",
    "fcfs_fast": base_dir / "fcfs_fast_results"
}

# Load jobs and machines data for all schedulers
job_dfs = []
machine_dfs = []

for name, path in schedulers.items():
    jobs_df = pd.read_csv(path / "out_jobs.csv")
    jobs_df["scheduler"] = name
    job_dfs.append(jobs_df)
    
    machines_df = pd.read_csv(path / "out_machine_states.csv")
    machines_df["scheduler"] = name
    machine_dfs.append(machines_df)

jobs_df = pd.concat(job_dfs, ignore_index=True)
machines_df = pd.concat(machine_dfs, ignore_index=True)

out_dir = base_dir / "comparison_plots"
out_dir.mkdir(exist_ok=True)

# --- Summary stats and statistical tests ---
summary_lines = []

def summary_stat(df, metric, group_col="scheduler"):
    summary = df.groupby(group_col)[metric].agg(['mean', 'median', 'std', 'count'])
    return summary

# Average stretch per scheduler
stretch_summary = summary_stat(jobs_df, "stretch")
summary_lines.append("Average Stretch per Scheduler:\n" + stretch_summary.to_string() + "\n")

# Pairwise Mann-Whitney U test on stretch
summary_lines.append("Pairwise Mann-Whitney U tests for Stretch:\n")
for sched1, sched2 in combinations(schedulers.keys(), 2):
    data1 = jobs_df[jobs_df["scheduler"] == sched1]["stretch"]
    data2 = jobs_df[jobs_df["scheduler"] == sched2]["stretch"]
    u_stat, p_val = mannwhitneyu(data1, data2, alternative='two-sided')
    summary_lines.append(f"{sched1} vs {sched2}: U={u_stat:.3f}, p={p_val:.4f}")

summary_lines.append("\nAverage Waiting Time per Scheduler:\n" + summary_stat(jobs_df, "waiting_time").to_string() + "\n")

# Pairwise Mann-Whitney U test on waiting time
summary_lines.append("Pairwise Mann-Whitney U tests for Waiting Time:\n")
for sched1, sched2 in combinations(schedulers.keys(), 2):
    data1 = jobs_df[jobs_df["scheduler"] == sched1]["waiting_time"]
    data2 = jobs_df[jobs_df["scheduler"] == sched2]["waiting_time"]
    u_stat, p_val = mannwhitneyu(data1, data2, alternative='two-sided')
    summary_lines.append(f"{sched1} vs {sched2}: U={u_stat:.3f}, p={p_val:.4f}")

# Average machine utilization over time per scheduler
machines_df["total_machines"] = machines_df["nb_computing"] + machines_df["nb_idle"] + machines_df["nb_sleeping"]
machines_df["utilization"] = machines_df["nb_computing"] / machines_df["total_machines"]
util_summary = machines_df.groupby("scheduler")["utilization"].agg(['mean', 'median', 'std', 'count'])
summary_lines.append("Average Machine Utilization per Scheduler:\n" + util_summary.to_string() + "\n")

# Pairwise Mann-Whitney U test on machine utilization
summary_lines.append("Pairwise Mann-Whitney U tests for Machine Utilization:\n")
for sched1, sched2 in combinations(schedulers.keys(), 2):
    data1 = machines_df[machines_df["scheduler"] == sched1]["utilization"]
    data2 = machines_df[machines_df["scheduler"] == sched2]["utilization"]
    u_stat, p_val = mannwhitneyu(data1, data2, alternative='two-sided')
    summary_lines.append(f"{sched1} vs {sched2}: U={u_stat:.3f}, p={p_val:.4f}")

with open(out_dir / "comparison_summary.txt", "w") as f:
    f.write("\n".join(summary_lines))


# --- Plotting ---

# 1. Stretch vs Requested Resources
fig1 = px.scatter(
    jobs_df,
    x="requested_number_of_resources",
    y="stretch",
    color="scheduler",
    title="Stretch vs Requested Resources by Scheduler",
    hover_data=["job_id", "submission_time", "execution_time"]
)
fig1.write_image(out_dir / "stretch_vs_requested_resources.png")

# 2. Gantt-style Timeline Plot (all schedulers combined)
fig2 = px.timeline(
    jobs_df,
    x_start="starting_time",
    x_end="finish_time",
    y="job_id",
    color="scheduler",
    hover_data=["requested_number_of_resources", "stretch"],
    title="Job Execution Timeline by Scheduler"
)
fig2.update_yaxes(autorange="reversed")
fig2.write_image(out_dir / "job_execution_timeline.png")

# 3. Machine State Over Time (all schedulers + states)
fig3 = go.Figure()
states = ["nb_computing", "nb_idle", "nb_sleeping", "nb_switching_on", "nb_switching_off"]
for state in states:
    for scheduler in schedulers.keys():
        sched_machines = machines_df[machines_df["scheduler"] == scheduler]
        fig3.add_trace(go.Scatter(
            x=sched_machines["time"],
            y=sched_machines[state],
            mode='lines',
            name=f"{scheduler} - {state}"
        ))

fig3.update_layout(
    title="Machine States Over Time by Scheduler",
    xaxis_title="Simulation Time",
    yaxis_title="Number of Machines",
    legend_title="Scheduler and State",
    height=700,
    width=1000
)
fig3.write_image(out_dir / "machine_states_over_time.png")

# 4. Histogram of Job Waiting Times
fig4 = px.histogram(
    jobs_df,
    x="waiting_time",
    nbins=50,
    title="Histogram of Job Waiting Times by Scheduler",
    labels={"waiting_time": "Waiting Time (seconds)"},
    color="scheduler",
    opacity=0.6,
    barmode="overlay"
)
fig4.update_layout(
    xaxis_title="Waiting Time (seconds)",
    yaxis_title="Number of Jobs",
    bargap=0.1
)
fig4.write_image(out_dir / "histogram_waiting_times.png")

# 5. Waiting Time vs Submission Time
fig5 = px.scatter(
    jobs_df,
    x="submission_time",
    y="waiting_time",
    color="scheduler",
    title="Waiting Time vs Submission Time by Scheduler",
    labels={
        "submission_time": "Submission Time (seconds)",
        "waiting_time": "Waiting Time (seconds)"
    },
    hover_data=["job_id", "requested_number_of_resources", "profile"]
)
fig5.update_traces(marker=dict(size=6, opacity=0.7))
fig5.update_layout(
    xaxis_title="Submission Time",
    yaxis_title="Waiting Time",
    legend_title="Scheduler"
)
fig5.write_image(out_dir / "waiting_vs_submission_time.png")

# 6. Distribution of Failures Over Time
failures_df = jobs_df[jobs_df["success"] == 0].copy()
bins = np.arange(jobs_df["submission_time"].min(), jobs_df["submission_time"].max() + 1, 1)
failures_df["submission_bin"] = pd.cut(failures_df["submission_time"], bins)
failures_count = failures_df.groupby(["scheduler", "submission_bin"]).size().reset_index(name="failures")
failures_count["bin_start"] = failures_count["submission_bin"].apply(lambda x: x.left)

fig6 = px.bar(
    failures_count,
    x="bin_start",
    y="failures",
    color="scheduler",
    title="Distribution of Failures Over Time (Submission Time Bins) by Scheduler",
    labels={"bin_start": "Submission Time (s)", "failures": "Number of Failed Jobs"}
)
fig6.write_image(out_dir / "failures_over_time.png")

# 7. Queue Length Over Time (all schedulers)
queue_dfs = []
for name in schedulers.keys():
    jobs_temp = jobs_df[jobs_df["scheduler"] == name]
    machines_temp = machines_df[machines_df["scheduler"] == name]
    times = machines_temp["time"].values
    
    queue_lengths = []
    for t in times:
        submitted = jobs_temp[jobs_temp["submission_time"] <= t]
        started = jobs_temp[jobs_temp["starting_time"] <= t]
        queue_length = len(submitted) - len(started)
        queue_lengths.append(queue_length)
        
    qdf = pd.DataFrame({"time": times, "queue_length": queue_lengths})
    qdf["scheduler"] = name
    queue_dfs.append(qdf)

queue_all = pd.concat(queue_dfs, ignore_index=True)

fig7 = px.line(
    queue_all,
    x="time",
    y="queue_length",
    color="scheduler",
    title="Queue Length Over Time by Scheduler",
    labels={"time": "Simulation Time (s)", "queue_length": "Number of Jobs in Queue"}
)
fig7.write_image(out_dir / "queue_length_over_time.png")

# 8. Gantt Chart of Jobs (Success Colored, all schedulers combined)
jobs_sorted = jobs_df.sort_values("submission_time")

fig8 = px.timeline(
    jobs_sorted,
    x_start="starting_time",
    x_end="finish_time",
    y="job_id",
    color="success",
    title="Gantt Chart of Jobs (Success Colored, All Schedulers)",
    hover_data=["scheduler", "profile", "requested_number_of_resources", "waiting_time", "execution_time"]
)
fig8.update_yaxes(autorange="reversed")
fig8.write_image(out_dir / "gantt_chart_jobs.png")

# 9. Evalys Gantt Chart per scheduler (saved individually)
for name, path in schedulers.items():
    job_csv = path / "out_jobs.csv"
    df = pd.read_csv(job_csv)

    df = df.sort_values(by='starting_time').reset_index(drop=True)

    df['job_index'] = df.index

    df['start'] = pd.to_datetime(df['starting_time'], unit='s')
    df['end'] = pd.to_datetime(df['finish_time'], unit='s')

    # Plot Gantt chart
    plot = px.timeline(
        df,
        x_start='start',
        x_end='end',
        y='job_index',
        hover_data=['job_id', 'start', 'end'],  # Keep original job_id in tooltip
        title=f"Plotly Gantt Chart for Scheduler '{name}' Jobs",
    )

    plot.update_yaxes(
        title="Job ID",  # More readable label
        tickmode='linear',
        dtick=100  # Adjust spacing between y-ticks (e.g., every 100 jobs)
    )

    plot.update_layout(
        height=1000,
        xaxis_title="Time",
        margin=dict(l=20, r=20, t=60, b=20),
    )

    output_png = out_dir / f"plotly_gantt_chart_{name}_jobs.png"
    plot.write_image(str(output_png), scale=2)
