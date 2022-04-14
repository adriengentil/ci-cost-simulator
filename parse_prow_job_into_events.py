import csv
import json
import sys
from datetime import datetime

from event import ACQUIRE_INSTANCE_ACTION, RELEASE_INSTANCE_ACTION, Event


def parse_data_into_events(filename: str) -> list[Event]:
    event_list = []
    with open(filename, newline="") as csvfile:
        raw_jobs = csv.reader(csvfile)
        for raw_job in raw_jobs:
            raw_id = raw_job[0]
            raw_duration = raw_job[1]
            raw_start = raw_job[2]
            raw_status = raw_job[4]
            raw_name = raw_job[5]

            if int(float(raw_duration)) == 0:
                continue
            if raw_status != "SUCCESS" and raw_status != "FAILURE":
                continue

            try:
                start = datetime.strptime(
                    raw_start, "%Y-%m-%dT%H:%M:%S%z"
                )  # 2022-01-18T08:00:48Z
            except:
                print(f"failled to parse date {raw_start} for job id {raw_id}")
                raise
            start_seconds = start.timestamp()
            job_id = f"{raw_name}-{raw_id}"
            event_list.append(
                Event(
                    timestamp=start_seconds, action=ACQUIRE_INSTANCE_ACTION, job=job_id
                )
            )
            event_list.append(
                Event(
                    timestamp=start_seconds + float(raw_duration),
                    action=RELEASE_INSTANCE_ACTION,
                    job=job_id,
                )
            )

    return event_list


def main() -> None:
    input = sys.argv[1]
    output = sys.argv[2]

    simulation_event_list = parse_data_into_events(input)

    with open(output, "w") as outfile:
        json.dump([e.__dict__ for e in simulation_event_list], outfile)


if __name__ == "__main__":
    main()
