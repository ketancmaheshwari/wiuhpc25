import zmq
import json
import random
import argparse

def main():
    parser = argparse.ArgumentParser(description="Dynamic job server for Batsim")
    parser.add_argument("--num-jobs", type=int, default=10, help="Number of jobs to generate")
    parser.add_argument("--min-walltime", type=float, default=10.0, help="Minimum job walltime")
    parser.add_argument("--max-walltime", type=float, default=15.0, help="Maximum job walltime")
    args = parser.parse_args()

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:28000")

    workload_name = "dyn"
    profile_name = "delay_15s"
    profile_data = {"type": "delay", "delay": 15.0}

    resource_count = 1
    #TODO: add a way to either randomize or speciy num resources for jobs

    # Generate jobs with random walltimes in given range and staggered subtime (0,1,2,...)
    jobs = []
    for i in range(args.num_jobs):
        walltime = random.uniform(args.min_walltime, args.max_walltime)
        jobs.append({
            "id": f"{workload_name}!job{i+1}",
            "profile": profile_name,
            "res": 1,
            "walltime": walltime,
            "subtime": i  # stagger jobs by 1 time unit each
        })


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
            print(f"[{now:.2f}] Event received: {etype}")

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
                print(f"[{now:.2f}] Simulation ended.")
                response["events"] = []

        # Only register and execute jobs after profile registered
        if registered_profile:
    # Register all jobs immediately if not registered yet
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

            # Execute jobs only when their subtime arrives
            for job in jobs:
                job_id = job["id"]
                if job_id in registered_jobs and job_id not in executed_jobs and now >= job["subtime"]:
                    response["events"].append({
                        "timestamp": now,
                        "type": "EXECUTE_JOB",
                        "data": {
                            "job_id": job_id,
                            "alloc": "0"
                        }
                    })
                    executed_jobs.add(job_id)

            # Send registration_finished notification once all jobs registered
            if (
                len(registered_jobs) == len(jobs) and
                not registration_finished_sent
            ):
                response["events"].append({
                    "timestamp": now,
                    "type": "NOTIFY",
                    "data": {
                        "type": "registration_finished"
                    }
                })
                registration_finished_sent = True

        socket.send_json(response)

if __name__ == "__main__":
    main()
