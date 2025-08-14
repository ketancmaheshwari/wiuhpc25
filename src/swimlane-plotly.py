import pandas as pd
from datetime import datetime
from collections import defaultdict
import plotly.graph_objects as go

# === STEP 1: Load CSV and Group Jobs ===
def load_grouped_jobs_from_csv(filepath):
    df = pd.read_csv(filepath)
    grouped_jobs = defaultdict(lambda: {"runs": []})

    for _, row in df.iterrows():
        key = f"{row['UID']}::{row['JobIDRaw']}"
        job = grouped_jobs[key]
        job["UID"] = key
        job["user"] = row["UID"]
        job["submit"] = row["Submit"]
        job["Nodes"] = row["NNodes"]
        job["Time-Limit"] = row["TimelimitRaw"]
        job["Elapsed-Time"] = row["ElapsedRaw"]
        job["QOS"] = row.get("QOS", "normal").lower()
        job["status"] = row["State"].lower()

        start = row["Start"] if pd.notna(row["Start"]) else None
        end = row["End"]

        if start is not None:
            job["runs"].append([start, end])
        else:
            job["runs"].append(["CANCELLED_NO_START", row["Submit"], end])

    return list(grouped_jobs.values())

# === STEP 2: Normalize Times to Minutes ===
def normalize_job_times(jobs):
    all_times = []

    for job in jobs:
        try:
            all_times.append(datetime.fromisoformat(job["submit"]))
        except:
            pass

        for run in job["runs"]:
            if isinstance(run[0], str) and run[0] == "CANCELLED_NO_START":
                _, sub, end = run
                all_times.append(datetime.fromisoformat(sub))
                all_times.append(datetime.fromisoformat(end))
            else:
                all_times.append(datetime.fromisoformat(run[0]))
                all_times.append(datetime.fromisoformat(run[1]))

    t0 = min(all_times)

    def to_minutes(t):
        return (datetime.fromisoformat(t) - t0).total_seconds() / 60

    for job in jobs:
        job["submit"] = to_minutes(job["submit"])
        new_runs = []
        for run in job["runs"]:
            if isinstance(run[0], str) and run[0] == "CANCELLED_NO_START":
                _, sub, end = run
                new_runs.append(["CANCELLED_NO_START", to_minutes(sub), to_minutes(end)])
            else:
                new_runs.append([to_minutes(run[0]), to_minutes(run[1])])
        job["runs"] = new_runs

    return jobs

# === STEP 3: Plot with Plotly ===
def plot_swimlane_chart_plotly(jobs):
    users = sorted(set(job["user"] for job in jobs))
    user_to_y = {user: i for i, user in enumerate(users)}
    job_offsets = {user: 0 for user in users}  # Track how many jobs we've plotted per user

    fig = go.Figure()

    status_colors = {
        'completed': 'green',
        'failed': 'red',
        'cancelled': 'orange',
        'preempted': 'blue',
        'timeout': 'purple'
    }

    for job in jobs:
        user = job["user"]
        base_y = user_to_y[user]
        offset = job_offsets[user] * 0.05  # Adjust spacing as needed
        y = base_y + offset
        job_offsets[user] += 1  # Increment job count for user

        submit = job["submit"]
        runs = job["runs"]
        status = job["status"]
        qos = job.get("QOS", "normal")
        nodes = job["Nodes"]

        color = status_colors.get(status, "gray")

        # Optional: draw pending dotted line

        if isinstance(runs[0][0], float):
            start = runs[0][0]
            fig.add_trace(go.Scatter(
                x=[submit, start],
                y=[y, y],
                mode="lines",
                line=dict(color="gray", dash="dot", width=1),
                showlegend=False
            ))

        for i, run in enumerate(runs):
            if isinstance(run[0], str) and run[0] == "CANCELLED_NO_START":
                _, sub, end = run
                fig.add_trace(go.Scatter(
                    x=[sub, end],
                    y=[y, y],
                    mode="lines",
                    line=dict(color="orange", dash="dash", width=2),
                    name="Cancelled" if i == 0 else None,
                    hovertext=f"{job['user']} CANCELLED",
                    showlegend=False
                ))
                continue

            start, end = run
            fig.add_trace(go.Scatter(
                x=[start, end],
                y=[y, y],
                mode="lines+markers",
                line=dict(color=color, width=2),
                marker=dict(
                    symbol="diamond" if qos == "high" else "circle",
                    color="gold" if qos == "high" else color,
                    size=6 if qos == "high" else 0
                ),
                hovertext=f"{job['user']} | {status} | Nodes: {job['Nodes']} | Time: {job['Time-Limit']}",
                showlegend=False
            ))

            if i > 0:  # Preemption resume
                fig.add_trace(go.Scatter(
                    x=[start],
                    y=[y],
                    mode="markers",
                    marker=dict(symbol="star", color="blue", size=6),
                    name="Preemption Resume",
                    showlegend=False
                ))
                seen_states.add("resume")

    legend_items = [
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color="green", width=3),
            name="Completed"
        ),
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color="purple", width=3),
            name="Timeout"
        ),
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color="orange", width=3, dash="dash"),
            name="Cancelled"
        ),
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color="blue", width=3),
            name="Preempted"
        ),
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color="gray", width=2, dash="dot"),
            name="Pending"
        ),
        go.Scatter(
            x=[None],
            y=[None],
            mode="markers",
            marker=dict(symbol="star", color="blue", size=10),
            name="Preemption Resume"
        ),
        go.Scatter(
            x=[None],
            y=[None],
            mode="markers",
            marker=dict(symbol="diamond", color="gold", size=10),
            name="High QOS"
        ),
    ]
    for trace in legend_items:
        fig.add_trace(trace)

    #annotations = []
    job_offset = 0.2  # vertical space between jobs

    # Get the earliest start time for horizontal label alignment
    try:
        min_x = min(
            run[0] if isinstance(run[0], float) else run[1]
            for job in jobs
            for run in job["runs"]
        )
    except ValueError:
        min_x = 0  # fallback if no jobs exist

    for user in users:
        base_y = user_to_y[user]
        job_count = job_offsets[user]

        if job_count == 0:
            continue

        # This is the total height of the user's swimlane
        top_y = base_y
        bottom_y = base_y + (job_count - 1) * job_offset
        center_y = (top_y + bottom_y) / 2

        # annotations.append(dict(
        #     x=min_x - 5,  # Align to the left of the plot
        #     y=center_y,  # Truly centered vertically
        #     xref='x',
        #     yref='y',
        #     text=user,
        #     showarrow=False,
        #     font=dict(size=12, color='black'),
        #     xanchor='right',
        #     align='right'
        # ))

    fig.update_layout(
        title="SLURM Experiment 1",
        xaxis_title="Time (minutes since earliest submit)",
        yaxis=dict(
            tickvals=[user_to_y[u] for u in users],
            ticktext=users,
            showgrid=True,
            gridcolor="black",
            title="Users"

        ),
        #annotations=annotations,
        template="plotly_white",
        height=500 + 60 * len(users)
    )

    return fig

# === MAIN ===
#csv_path = "/Users/3ue/dev/scheduling/defiant2-experiments/exp1/curatedjobsdata.csv"  # <-- Your CSV path
csv_path = "/home/km0/defiant2-experiments/exp4/curatedjobsdata.csv"  # <-- Your CSV path
jobs = load_grouped_jobs_from_csv(csv_path)
jobs = normalize_job_times(jobs)
fig = plot_swimlane_chart_plotly(jobs)
#fig.show()
fig.write_html("Experiment4_plotly1.html") ## This save the plot as an html, just remember to update the exp labels.
