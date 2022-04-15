import json
import sys
from datetime import datetime

from event import ACQUIRE_INSTANCE_ACTION, RELEASE_INSTANCE_ACTION, Event


def keep_action_pairs(event_list):
    action_pairs = {}

    for e in event_list:
        # interpolated contains the job id
        # it allows us to correlate events
        job_id = e["interpolated"].split()[0] 
        if job_id in action_pairs:
            action_pairs[job_id].append(e)
        else:
            action_pairs[job_id] = [e]

    for k in list(action_pairs.keys()):
        pair = action_pairs[k]
        if not (
            len(pair) == 2
            and (
                (
                    pair[0]["type"] == "instance.created"
                    and pair[1]["type"] == "instance.deleted"
                )
                or (
                    pair[0]["type"] == "instance.deleted"
                    and pair[1]["type"] == "instance.created"
                )
            )
        ):
            del action_pairs[k]

    clean_events = []
    for e in action_pairs.values():
        clean_events.extend(e)

    return clean_events


def dedup_event_ids(event_list):
    seen_event_ids = set()
    dedup_events = []

    for e in event_list:
        if e["id"] not in seen_event_ids:
            seen_event_ids.add(e["id"])
            dedup_events.append(e)

    return dedup_events


def get_events_from_packet_events(packet_event_list):
    event_list = []

    for e in packet_event_list:
        event_date = datetime.strptime(
            e["created_at"], "%Y-%m-%dT%H:%M:%S%z"
        )  # 2022-01-18T08:00:48Z

        event_action = RELEASE_INSTANCE_ACTION
        if e["type"] == "instance.created":
            event_action = ACQUIRE_INSTANCE_ACTION

        job_id = e["interpolated"].split()[0]

        event_list.append(
            Event(
                timestamp=event_date.timestamp(), action=event_action, job=job_id
            ).__dict__
        )
    return event_list


def main() -> None:
    input = sys.argv[1]
    output = sys.argv[2]

    with open(input) as json_file:
        event_list = json.load(json_file)

    # The same even id may appear several times in the list
    # Clean them up
    print(f"Dedup {len(event_list)} events...")
    dedup_event_list = dedup_event_ids(event_list)
    print(f"Dedup {len(event_list) - len(dedup_event_list)} events")

    # Make sure we keep all the events that acquire and release machine for a given job
    clean_event_list = keep_action_pairs(dedup_event_list)
    print(f"Cleaned up {len(dedup_event_list) - len(clean_event_list)} events")

    # transform packet events into simulation events
    simulation_event_list = get_events_from_packet_events(clean_event_list)

    with open(output, "w") as outfile:
        json.dump(simulation_event_list, outfile)


if __name__ == "__main__":
    main()
