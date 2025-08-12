import json
import random
import argparse

# Constants for unique return codes
RETURN_CODES = {
    "general": 1,
    "critical": 99,
    "timeout": 124,
    "cancelled": 130
}


def generate_synthetic_workload(num_jobs, nb_res, walltime_range, res_range, subtime_range):
    profiles = {
        # Modified 'simple' to be parallel_homogeneous
        "simple": {
            "type": "parallel_homogeneous",
            "cpu": 5e6,  # Single value for homogeneous
            "com": 5e6   # Single value for homogeneous
        },
        "homogeneous": {
            "type": "parallel_homogeneous",
            "cpu": 10e6,
            "com": 1e6
        },
        "delay": {
            "type": "delay",
            "delay": 20.20
        }
    }

    jobs = []
    profile_names = list(profiles.keys())
    for i in range(num_jobs):
        jobs.append({
            "id": f"job_{i}",
            "subtime": random.randint(*subtime_range),
            "walltime": random.randint(*walltime_range),
            "res": random.randint(*res_range),
            "profile": random.choice(profile_names)
        })

    return {
        "nb_res": nb_res,
        "jobs": jobs,
        "profiles": profiles
    }


def inject_failures(workload, output_path, job_failure_range, critical_failure_range,
                    timeout_failure_range, cancel_failure_range):
    jobs = workload['jobs']
    profiles = workload['profiles']
    num_jobs = len(jobs)

    def get_count(percent_range): return min(int(num_jobs * random.uniform(*percent_range) / 100), num_jobs)

    fail_indices = set(random.sample(range(num_jobs), get_count(job_failure_range)))
    timeout_indices = set(random.sample([i for i in range(num_jobs) if i not in fail_indices], get_count(timeout_failure_range)))
    cancel_indices = set(random.sample([i for i in range(num_jobs) if i not in fail_indices and i not in timeout_indices], get_count(cancel_failure_range)))

    num_critical = get_count(critical_failure_range)
    critical_indices = set(random.sample(list(fail_indices), min(num_critical, len(fail_indices))))

    failure_map = {}
    counts = {
        "general": 0,
        "critical": 0,
        "timeout": 0,
        "cancelled": 0
    }

    for i, job in enumerate(jobs):
        original_profile = job.get("profile")
        if not original_profile or original_profile not in profiles:
            continue

        job.setdefault("metadata", {})
        job["return_code"] = 0 

        failure_type = None
        if i in timeout_indices:
            failure_type = "timeout"
        elif i in cancel_indices:
            failure_type = "cancelled"
        elif i in fail_indices:
            if i in critical_indices:
                failure_type = "critical"
            else:
                failure_type = "general"

        if failure_type:
            retcode = RETURN_CODES[failure_type]
            key = (original_profile, retcode)

            if key not in failure_map:
                new_name = f"{original_profile}_fail_{retcode}_{len(failure_map)}"
                new_profile = json.loads(json.dumps(profiles[original_profile]))
                new_profile["ret"] = retcode
                profiles[new_name] = new_profile
                failure_map[key] = new_name

            job["profile"] = failure_map[key]
            job["return_code"] = retcode
            if failure_type != "general":
                job["metadata"]["failure_type"] = failure_type
            counts[failure_type] += 1

    workload["jobs"] = jobs
    workload["profiles"] = profiles

    print("\n--- Failure Injection Summary ---")
    print(f"Total jobs: {num_jobs}")
    print(f"General failures: {counts['general']}")
    print(f"Critical failures: {counts['critical']}")
    print(f"Timeouts: {counts['timeout']}")
    print(f"Cancelled: {counts['cancelled']}")
    print(f"Profiles added: {len(failure_map)}")
    print(f"Output saved to: {output_path}")
    print("-------------------------------\n")

    with open(output_path, 'w') as f:
        json.dump(workload, f, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate Batsim workload with failures")

    parser.add_argument("output_workload", help="Path to save workload")
    parser.add_argument("--num-jobs", type=int, default=20)
    parser.add_argument("--nb-res", type=int, default=4)
    parser.add_argument("--walltime-range", nargs=2, type=int, default=[10, 100])
    parser.add_argument("--res-range", nargs=2, type=int, default=[1, 4])
    parser.add_argument("--subtime-range", nargs=2, type=int, default=[1, 50])

    parser.add_argument("--job-failure-min", type=float, default=3.0)
    parser.add_argument("--job-failure-max", type=float, default=15.0)
    parser.add_argument("--critical-failure-min", type=float, default=10.0)
    parser.add_argument("--critical-failure-max", type=float, default=30.0)
    parser.add_argument("--timeout-failure-min", type=float, default=2.0)
    parser.add_argument("--timeout-failure-max", type=float, default=10.0)
    parser.add_argument("--cancel-failure-min", type=float, default=1.0)
    parser.add_argument("--cancel-failure-max", type=float, default=5.0)

    args = parser.parse_args()

    workload = generate_synthetic_workload(
        num_jobs=args.num_jobs,
        nb_res=args.nb_res,
        walltime_range=tuple(args.walltime_range),
        res_range=tuple(args.res_range),
        subtime_range=tuple(args.subtime_range)
    )

    inject_failures(
        workload=workload,
        output_path=args.output_workload,
        job_failure_range=(args.job_failure_min, args.job_failure_max),
        critical_failure_range=(args.critical_failure_min, args.critical_failure_max),
        timeout_failure_range=(args.timeout_failure_min, args.timeout_failure_max),
        cancel_failure_range=(args.cancel_failure_min, args.cancel_failure_max)
    )