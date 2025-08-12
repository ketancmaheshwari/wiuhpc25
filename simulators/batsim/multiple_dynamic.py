import zmq
import json
import random
import argparse
import os
import shutil

def fcfs_scheduler(jobs, executed_jobs, now):
    return [
        job for job in sorted(jobs, key=lambda job: job["subtime"])
        if job["id"] not in executed_jobs and now >= job["subtime"]
    ]

def sjf_scheduler(jobs, executed_jobs, now):
    sorted_jobs = sorted(jobs, key=lambda job: job["walltime"])
    return [
        job for job in sorted_jobs
        if job["id"] not in executed_jobs and now >= job["subtime"]
    ]

def random_scheduler(jobs, executed_jobs, now):
    ready_jobs = [
        job for job in jobs
        if job["id"] not in executed_jobs and now >= job["subtime"]
    ]
    random.shuffle(ready_jobs)
    return ready_jobs

def easy_bf_scheduler(jobs, executed_jobs, now):
    waiting_jobs = [
        job for job in jobs if job["id"] not in executed_jobs and now >= job["subtime"]
    ]
    if not waiting_jobs:
        return []
    first_job = waiting_jobs[0]
    backfilled = [first_job]
    for job in waiting_jobs[1:]:
        if job["walltime"] <= first_job["walltime"]:
            backfilled.append(job)
    return backfilled

def filler_scheduler(jobs, executed_jobs, now):
    ready_jobs = [
        job for job in jobs
        if job["id"] not in executed_jobs and now >= job["subtime"]
    ]
    return sorted(ready_jobs, key=lambda job: (job["walltime"], job["res"]))

def select_jobs_to_execute(jobs, executed_jobs, now, algorithm):
    if algorithm == "fcfs":
        return fcfs_scheduler(jobs, executed_jobs, now)
    elif algorithm == "sjf":
        return sjf_scheduler(jobs, executed_jobs, now)
    elif algorithm == "random":
        return random_scheduler(jobs, executed_jobs, now)
    elif algorithm == "easy_bf":
        return easy_bf_scheduler(jobs, executed_jobs, now)
    elif algorithm == "filler":
        return filler_scheduler(jobs, executed_jobs, now)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")

def run_scheduler(algorithm, jobs):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:28000")

    workload_name = "dyn"
    profile_name = "delay_15s"
    profile_data = {"type": "delay", "delay": 15.0}

    registered_profile = False
    registered_jobs = set()
    executed_jobs = set()
    registration_finished_sent = False

    while True:
        msg = socket.recv()
        message = json.loads(msg)

        now = message["now"]
        events = message["events"]
        response = {"now": now, "events": []}

        for event in events:
            etype = event["type"]
            print(f"[{algorithm} @ {now:.2f}] Event received: {etype}")

            if etype == "SIMULATION_BEGINS":
                if not registered_profile:
                    print(f"[{now:.2f}] Registering profile '{profile_name}' once.")
                    response["events"].append({
                        "timestamp": now,
                        "type": "REGISTER_PROFILE",
                        "data": {
                            "workload_name": workload_name,
                            "profile_name": profile_name,
                            "profile": profile_data
                        }
                    })
                    registered_profile = True

            elif etype == "SIMULATION_ENDS":
                print(f"[{algorithm} @ {now:.2f}] Simulation ended.")
                response["events"] = []
                socket.send_json(response)
                break 

        if registered_profile:
            for job in jobs:
                job_id = job["id"]
                if job_id not in registered_jobs:
                    response["events"].append({
                        "timestamp": now,
                        "type": "REGISTER_JOB",
                        "data": {
                            "job_id": job_id,
                            "job": {
                                "id": job_id,
                                "profile": job["profile"],
                                "res": job["res"],
                                "walltime": job["walltime"],
                                "subtime": job["subtime"]
                            }
                        }
                    })
                    registered_jobs.add(job_id)

            to_execute = select_jobs_to_execute(jobs, executed_jobs, now, algorithm)
            for job in to_execute:
                job_id = job["id"]
                if job_id in registered_jobs and job_id not in executed_jobs:
                    response["events"].append({
                        "timestamp": now,
                        "type": "EXECUTE_JOB",
                        "data": {
                            "job_id": job_id,
                            "alloc": "0"
                        }
                    })
                    executed_jobs.add(job_id)

            if len(registered_jobs) == len(jobs) and not registration_finished_sent:
                response["events"].append({
                    "timestamp": now,
                    "type": "NOTIFY",
                    "data": {
                        "type": "registration_finished"
                    }
                })
                registration_finished_sent = True

        socket.send_json(response)

def generate_jobs(num_jobs, min_walltime, max_walltime):
    jobs = []
    for i in range(num_jobs):
        walltime = random.uniform(min_walltime, max_walltime)
        jobs.append({
            "id": f"dyn!job{i+1}",
            "profile": "delay_15s",
            "res": 1,
            "walltime": walltime,
            "subtime": i
        })
    return jobs

def move_output_files(algorithm):
    output_dir = f"{algorithm}_dynamic_results"
    os.makedirs(output_dir, exist_ok=True)
    for file in os.listdir("."):
        if file.startswith("out"):
            shutil.move(file, os.path.join(output_dir, file))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-jobs", type=int, default=10)
    parser.add_argument("--min-walltime", type=float, default=10.0)
    parser.add_argument("--max-walltime", type=float, default=15.0)
    args = parser.parse_args()

    algorithms = ["fcfs", "sjf", "random", "easy_bf", "filler", "conservative_bf"]
    original_jobs = generate_jobs(args.num_jobs, args.min_walltime, args.max_walltime)

    for algorithm in algorithms:
        print(f"\n=== Running algorithm: {algorithm} ===")

        pid = os.fork()
        if pid == 0:
            run_scheduler(algorithm, original_jobs.copy())
            os._exit(0)
        else:

            os.system("batsim -p sample_xml.xml --enable-dynamic-jobs --acknowledge-dynamic-jobs -s tcp://localhost:28000 --enable-compute-sharing")
            os.waitpid(pid, 0)

            move_output_files(algorithm)

    print("\n All algorithms finished. Results stored in *_dynamic_results folders.")

if __name__ == "__main__":
    main()
