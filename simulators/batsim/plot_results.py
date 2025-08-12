import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Evalys imports
from evalys.jobset import JobSet
from evalys import visu
import matplotlib.pyplot as plt

jobs_df = pd.read_csv("./easy_bf_results/out_jobs.csv")
machines_df = pd.read_csv("./easy_bf_results/out_machine_states.csv")

out_dir = Path(".")

# 1. Stretch vs Requested Resources
fig1 = px.scatter(
    jobs_df,
    x="requested_number_of_resources",
    y="stretch",
    color="profile",
    title="Stretch vs Requested Resources",
    hover_data=["job_id", "submission_time", "execution_time"]
)
fig1.write_image(out_dir / "stretch_vs_requested_resources.png")

# 2. Gantt-style Timeline Plot
jobs_df["start"] = jobs_df["starting_time"]
jobs_df["end"] = jobs_df["finish_time"]
jobs_df["duration"] = jobs_df["execution_time"]

fig2 = px.timeline(
    jobs_df,
    x_start="start",
    x_end="end",
    y="job_id",
    color="profile",
    hover_data=["requested_number_of_resources", "stretch"],
    title="Job Execution Timeline"
)
fig2.update_yaxes(autorange="reversed")
fig2.write_image(out_dir / "job_execution_timeline.png")

# 3. Machine State Over Time
fig3 = go.Figure()
fig3.add_trace(go.Scatter(x=machines_df["time"], y=machines_df["nb_computing"], name="Computing"))
fig3.add_trace(go.Scatter(x=machines_df["time"], y=machines_df["nb_idle"], name="Idle"))
fig3.add_trace(go.Scatter(x=machines_df["time"], y=machines_df["nb_sleeping"], name="Sleeping"))
fig3.add_trace(go.Scatter(x=machines_df["time"], y=machines_df["nb_switching_on"], name="Switching On"))
fig3.add_trace(go.Scatter(x=machines_df["time"], y=machines_df["nb_switching_off"], name="Switching Off"))

fig3.update_layout(
    title="Machine States Over Time",
    xaxis_title="Simulation Time",
    yaxis_title="Number of Machines",
    legend_title="State",
)
fig3.write_image(out_dir / "machine_states_over_time.png")

# 4. Histogram of Job Waiting Times
fig4 = px.histogram(
    jobs_df,
    x="waiting_time",
    nbins=50,
    title="Histogram of Job Waiting Times",
    labels={"waiting_time": "Waiting Time (seconds)"},
    color_discrete_sequence=["indianred"]
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
    color="requested_number_of_resources",
    title="Waiting Time vs Submission Time",
    labels={
        "submission_time": "Submission Time (seconds)",
        "waiting_time": "Waiting Time (seconds)",
        "requested_number_of_resources": "Requested Resources"
    },
    hover_data=["job_id", "profile"]
)
fig5.update_traces(marker=dict(size=6, opacity=0.7))
fig5.update_layout(
    xaxis_title="Submission Time",
    yaxis_title="Waiting Time",
    legend_title="Requested Resources"
)
fig5.write_image(out_dir / "waiting_vs_submission_time.png")

# 6. Distribution of Failures Over Time
failures_df = jobs_df[jobs_df["success"] == 0]
bins = np.arange(jobs_df["submission_time"].min(), jobs_df["submission_time"].max() + 1, 1)
failures_df["submission_bin"] = pd.cut(failures_df["submission_time"], bins)
failures_count = failures_df.groupby("submission_bin").size().reset_index(name="failures")
failures_count["bin_start"] = failures_count["submission_bin"].apply(lambda x: x.left)

fig6 = px.bar(
    failures_count,
    x="bin_start",
    y="failures",
    title="Distribution of Failures Over Time (Submission Time Bins)",
    labels={"bin_start": "Submission Time (s)", "failures": "Number of Failed Jobs"}
)
fig6.write_image(out_dir / "failures_over_time.png")

# 7. Queue Length Over Time
queue_lengths = []
times = machines_df["time"].values

for t in times:
    submitted = jobs_df[jobs_df["submission_time"] <= t]
    started = jobs_df[jobs_df["starting_time"] <= t]
    queue_length = len(submitted) - len(started)
    queue_lengths.append(queue_length)

queue_df = pd.DataFrame({"time": times, "queue_length": queue_lengths})

fig7 = px.line(
    queue_df,
    x="time",
    y="queue_length",
    title="Queue Length Over Time",
    labels={"time": "Simulation Time (s)", "queue_length": "Number of Jobs in Queue"}
)
fig7.write_image(out_dir / "queue_length_over_time.png")

# 8. Gantt Chart of Jobs (Success Colored)
jobs_sorted = jobs_df.sort_values("submission_time")

fig8 = px.timeline(
    jobs_sorted,
    x_start="starting_time",
    x_end="finish_time",
    y="job_id",
    color="success",
    title="Gantt Chart of Jobs (Success Colored)",
    hover_data=["profile", "requested_number_of_resources", "waiting_time", "execution_time"]
)
fig8.update_yaxes(autorange="reversed")
fig8.write_image(out_dir / "gantt_chart_jobs.png")

# 9. Evalys Gantt Chart (with Matplotlib)
js = JobSet.from_csv("./easy_bf_results/out_jobs.csv")
fig9 = visu.gantt.plot_gantt(js)

plt.savefig(out_dir / "evalys_gantt_chart.png", bbox_inches="tight")
