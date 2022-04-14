import json
import os
import sys
from typing import Final
import packet
from datetime import datetime, timedelta, timezone


def parse_date(date: str) -> datetime:
    return datetime.strptime(
            date, "%Y-%m-%dT%H:%M:%S%z"
        )  # 2022-01-18T08:00:48Z


METAL_AUTH_TOKEN = os.environ["METAL_AUTH_TOKEN"]
METAL_PROJECT_ID = os.environ["METAL_PROJECT_ID"]

output_filename = sys.argv[1]

manager = packet.Manager(auth_token=METAL_AUTH_TOKEN)

now: Final = datetime.now(timezone.utc)
until_time: Final = datetime.now(timezone.utc) - timedelta(days=30)
serializable_events = []

for project_id in METAL_PROJECT_ID.split(sep=','):
    last_event_time = now
    packet_params = {"page": 1, "per_page": 1000}
    print(f"Retrieve events between {now} and {until_time} for {project_id}")
    while last_event_time > until_time:
        events = manager.list_project_events(project_id, params=packet_params)
        serializable_events.extend([e.__dict__ for e in events if parse_date(e.created_at) > until_time])

        last_event_time = parse_date(events[-1].created_at)
        packet_params["page"] = packet_params["page"] + 1

        print(f"Got events until {last_event_time}...")



with open(output_filename, "w") as outfile:
    json.dump(serializable_events, outfile)
