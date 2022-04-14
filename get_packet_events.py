import json
import os
import sys
import packet
from datetime import datetime, timedelta, timezone

METAL_AUTH_TOKEN = os.environ["METAL_AUTH_TOKEN"]
METAL_PROJECT_ID = os.environ["METAL_PROJECT_ID"]

output_filename = sys.argv[1]

manager = packet.Manager(auth_token=METAL_AUTH_TOKEN)

last_event_time = datetime.now(timezone.utc)
until_time = datetime.now(timezone.utc) - timedelta(days=30)
serializable_events = []

packet_params = {"page": 1, "per_page": 1000}

while last_event_time > until_time:
    events = manager.list_project_events(METAL_PROJECT_ID, params=packet_params)
    serializable_events.extend([e.__dict__ for e in events])

    last_event_time = datetime.strptime(
        events[-1].created_at, "%Y-%m-%dT%H:%M:%S%z"
    )  # 2022-01-18T08:00:48Z
    packet_params["page"] = packet_params["page"] + 1

    print(f"Got events until {last_event_time}...")

with open(output_filename, "w") as outfile:
    json.dump(serializable_events, outfile)
