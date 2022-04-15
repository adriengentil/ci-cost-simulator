import json
import sys
from datetime import datetime, timedelta
import parse_packet_events_into_events


class Lease:
    def __init__(self, machine_type, real_time):
        self.machine_type = machine_type
        self.real_time = real_time
        self.billed_per_hour = (int(real_time / 3600) + 1) * 3600
        self.billed_per_min = (int(real_time / 60) + 1) * 60

    @staticmethod
    def from_events(created, deleted):
        created_at = datetime.strptime(created["created_at"], "%Y-%m-%dT%H:%M:%S%z")
        deleted_at = datetime.strptime(deleted["created_at"], "%Y-%m-%dT%H:%M:%S%z")

        real_time = deleted_at.timestamp() - created_at.timestamp()
        machine_type = created["interpolated"].split()[1][1:-1]
        return Lease(machine_type, real_time)


def get_lease_list(event_list):
    events_per_job = {}

    for e in event_list:
        job_id = e["interpolated"].split()[0]
        if job_id in events_per_job:
            events_per_job[job_id].append(e)
        else:
            events_per_job[job_id] = [e]

    lease_list = []
    for p in events_per_job.values():
        if p[0]["type"] == "instance.created":
            lease_list.append(Lease.from_events(p[0], p[1]))
        else:
            lease_list.append(Lease.from_events(p[1], p[0]))

    return lease_list


def print_stats_duration_list(label: str,samples: list[float]):
    samples.sort()
    sum_ = int(sum(samples))
    avg = timedelta(seconds=sum(samples) / len(samples))
    median = timedelta(seconds=samples[int(len(samples) / 2)])
    p95 = timedelta(seconds=samples[int(len(samples) * 0.95)])
    p99 = timedelta(seconds=samples[int(len(samples) * 0.99)])
    print(f"{label} samples {len(samples)} sum {sum_}s avg {avg} median {median} p95 {p95} p99 {p99}")


def group_by_machine_types(lease_list: list[Lease]):
    lease_by_type = {}
    for l in lease_list:
        if l.machine_type not in lease_by_type:
            lease_by_type[l.machine_type] = [l]
        else:
            lease_by_type[l.machine_type].append(l)

    return lease_by_type


def print_stats(lease_list):
    real_time_samples = [l.real_time for l in lease_list]
    billed_per_hour_samples = [l.billed_per_hour for l in lease_list]
    billed_per_min_samples = [l.billed_per_min for l in lease_list]

    print(f"ALL MACHINE TYPES")
    print_stats_duration_list("Real    ", real_time_samples)
    print_stats_duration_list("Per hour", billed_per_hour_samples)
    print_stats_duration_list("Per min ", billed_per_min_samples)

    lease_by_type = group_by_machine_types(lease_list)
    for k, v in lease_by_type.items():
        print(f"MACHINE TYPE {k.upper()}")
        print_stats_duration_list("Real    ",[l.real_time for l in v])
        print_stats_duration_list("Per hour",[l.billed_per_hour for l in v])
        print_stats_duration_list("Per min ",[l.billed_per_min for l in v])


def main() -> None:
    input = sys.argv[1]

    with open(input) as json_file:
        raw_event_list = json.load(json_file)

    dedup_event_list = parse_packet_events_into_events.dedup_event_ids(raw_event_list)
    event_list = parse_packet_events_into_events.keep_action_pairs(dedup_event_list)

    lease_list = get_lease_list(event_list)

    print_stats(lease_list)


if __name__ == "__main__":
    main()
