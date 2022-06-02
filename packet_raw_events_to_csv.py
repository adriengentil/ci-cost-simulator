import csv
import json
import sys
from datetime import datetime, timedelta
import parse_packet_events_into_events
from pydantic import BaseModel


class Lease(BaseModel):
    build_id: str
    billed_duration: int
    created_at: datetime
    deleted_at: datetime
    duration: int
    machine_type: str

    @classmethod
    def from_events(self, created, deleted):
        # "\"ipi-ci-op-i1f2366y-9f43c-1532349340608106496\" (s3.xlarge.x86) was deployed to project \"OpenShift CI - Bare Metal Assisted\" by James"
        build_id = created["interpolated"].split()[0].split("-")[-1][:-2]
        machine_type = created["interpolated"].split()[1][1:-1]

        created_at = datetime.strptime(created["created_at"], "%Y-%m-%dT%H:%M:%S%z")
        deleted_at = datetime.strptime(deleted["created_at"], "%Y-%m-%dT%H:%M:%S%z")
        duration = deleted_at - created_at
        billed_duration = (int(duration.seconds / 3600) + 1) * 3600

        return self(
            build_id=build_id,
            billed_duration=billed_duration,
            created_at=created_at,
            deleted_at=deleted_at,
            duration=duration.seconds,
            machine_type=machine_type,
        )


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


def main() -> None:
    input = sys.argv[1]

    with open(input) as json_file:
        raw_event_list = json.load(json_file)

    dedup_event_list = parse_packet_events_into_events.dedup_event_ids(raw_event_list)
    event_list = parse_packet_events_into_events.keep_action_pairs(dedup_event_list)

    fieldnames = list(Lease.schema()["properties"].keys())

    lease_list = get_lease_list(event_list)
    with open("test.csv", "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for lease in lease_list:
            writer.writerow(lease.dict())


if __name__ == "__main__":
    main()
