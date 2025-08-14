import pandas as pd
from collections import defaultdict
from datetime import datetime
import plotly.graph_objects as go

# === STEP 1: Load and group data ===
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
        job["QOS"] = row.get("QOS", "")
        job["status"] = row["State"].lower()

        start = row["Start"] if pd.notna(row["Start"]) else None
        end = row["End"]

        if start is not None:
            job["runs"].append([start, end])
        else:
            job["runs"].append(["CANCELLED_NO_START", row["Submit"], end])

    return list(grouped_jobs.values())

# === STEP 2: Normalize time ===
def normalize_job_times(jobs):
    all_times = []

    for job in jobs:
        try:
            all_times.append(datetime.fromisoformat(job["submit"]))
        except: pass

        for run in job["runs"]:
            try:
                if isinstance(run[0], str) and run[0] == "CANCELLED_NO_START":
                    all_times.extend([datetime.fromisoformat(run[1]), datetime.fromisoformat(run[2])])
                else:
                    all_times.extend([datetime.fromisoformat(run[0]), datetime.fromisoformat(run[1])])
            except: pass

    t0 = min(all_times)

    def to_minutes(t):
        return (datetime.fromisoformat(t) - t0).total_seconds() / 60

    for job in jobs:
        job["submit"] = to_minutes(job["submit"])
        new_runs = []
        for run in job["runs"]:
            if isinstance(run[0], str) and run[0] == "CANCELLED_NO_START":
                new_runs.append(["CANCELLED_NO_START", to_minutes(run[1]), to_minutes(run[2])])
            else:
                new_runs.append([to_minutes(run[0]), to_minutes(run[1])])
        job["runs"] = new_runs

    return jobs

# === STEP 3: Plot with Plotly ===
def plot_swimlane_chart_plotly(jobs):
    fig = go.Figure()
    status_colors = {
        'completed': 'green',
        'failed': 'red',
        'cancelled': 'orange',
        'preempted': 'blue',
        'timeout' : 'purple',
        'pending': 'gray'
    }
    users = sorted(set(job['user'] for job in jobs))
    user_to_y = {user: i for i, user in enumerate(users)}
    job_offsets = {user: 0 for user in users}
    job_offset = 0.04

    status_lines = defaultdict(list)
    hovertexts = defaultdict(list)
    resume_markers = []
    qos_markers = []

    for job in jobs:
        user = job["user"]
        y_base = user_to_y[user]
        offset = job_offsets[user]
        y = y_base + offset * job_offset
        job_offsets[user] += 1

        status = job["status"].lower()
        color = status_colors.get(status, "gray")
        submit = job["submit"]
        runs = job["runs"]
        qos = job.get("QOS", "").lower()

        if isinstance(runs[0][0], str) and runs[0][0] == "CANCELLED_NO_START":
            pass
        else:
            start = runs[0][0]
            status_lines["pending"].append(([submit, start], [y, y]))
            hovertexts["pending"].append(f"{user} | pending | Nodes: {job['Nodes']} | Time: {job['Time-Limit']}")

        for i, run in enumerate(runs):
            if isinstance(run[0], str) and run[0] == "CANCELLED_NO_START":
                _, sub, end = run
                status_lines["cancelled"].append(([sub, end], [y, y]))
                hovertexts["cancelled"].append(f"{user} | cancelled | Nodes: {job['Nodes']} | Time: {job['Time-Limit']}")
            else:
                start, end = run
                status_lines[status].append(([start, end], [y, y]))
                hovertexts[status].append(f"{user} | {status} | Nodes: {job['Nodes']} | Time: {job['Time-Limit']}")
                if i > 0:
                    resume_markers.append((start, y))
                if qos == "high":
                    qos_markers.append(((start + end)/2, y))

    # Add grouped lines per status
    status_trace_styles = {
        "completed": dict(color="green", width=2.5),
        "failed": dict(color="red", width=2.5),
        "cancelled": dict(color="orange", width=2.5, dash="dash"),
        "preempted": dict(color="blue", width=2.5),
        "timeout": dict(color="purple", width=2.5),
        "pending": dict(color="gray", width=1.2, dash="dot")
    }
    for status, segments in status_lines.items():
        for x_vals, y_vals, text in zip(
                [seg[0] for seg in segments],
                [seg[1] for seg in segments],
                hovertexts[status]
        ):
            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode="lines",
                line=status_trace_styles.get(status, dict(color="gray")),
                name=status.capitalize(),
                legendgroup=status,
                showlegend=False,
                hoverinfo="text",
                hovertext=text
            ))

    # for status, segments in status_lines.items():
    #     for x_vals, y_vals in segments:
    #         fig.add_trace(go.Scatter(
    #             x=x_vals,
    #             y=y_vals,
    #             mode="lines",
    #             line=status_trace_styles.get(status, dict(color="gray")),
    #             name=status.capitalize(),
    #             legendgroup=status,
    #             showlegend=False,
    #             hoverinfo="text",
    #             hovertext = text
    #         ))

        # One dummy entry to show in legend
        fig.add_trace(go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=status_trace_styles.get(status, dict(color="gray")),
            name=status.capitalize(),
            legendgroup=status,
            showlegend=True
        ))

    if resume_markers:
        fig.add_trace(go.Scatter(
            x=[x for x, y in resume_markers],
            y=[y for x, y in resume_markers],
            mode="markers",
            marker=dict(symbol="star", color="blue", size=6),
            name="Preemption Resume",
            legendgroup="resume"
        ))

    if qos_markers:
        fig.add_trace(go.Scatter(
            x=[x for x, y in qos_markers],
            y=[y for x, y in qos_markers],
            mode="markers",
            marker=dict(symbol="diamond", color="gold", size=6),
            name="High QOS",
            legendgroup="qos"
        ))

    # Add user labels
    annotations = []
    for user in users:
        base_y = user_to_y[user]
        jobs_for_user = job_offsets[user]
        min_y = base_y
        max_y = base_y + (jobs_for_user - 1) * job_offset
        center_y = (min_y + max_y) / 2
        annotations.append(dict(
            xref='paper', yref='y', x=-0.02, y=center_y,
            text=user, showarrow=False,
            font=dict(size=12, color='black'),
            align='right', xanchor='right'
        ))

    fig.update_layout(
        title="SLURM Swimlane Chart (Interactive, Job Status Toggle)",
        xaxis=dict(title="Time (minutes since earliest submit)", showgrid=True, gridcolor="lightgray"),
        yaxis=dict(tickvals=[], showgrid=True, title="Users", gridcolor="black"),
        annotations=annotations,
        legend=dict(title="Job States", orientation="v", x=1.02, y=1),
        template="plotly_white",
        height=500 + 60 * len(users)
    )

    fig.show()

# === MAIN EXECUTION ===
csv_path = "/home/km0/defiant2-experiments/exp4/curatedjobsdata.csv"  # Replace with your path
jobs = load_grouped_jobs_from_csv(csv_path)
jobs = normalize_job_times(jobs)
plot_swimlane_chart_plotly(jobs)
