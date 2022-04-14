# Cost calculator

## Install python requirements

```
$ pip3 install -r requirements.txt
```

## Simulate a workload against Packet events

```
$ export METAL_AUTH_TOKEN=<your token>
$ export METAL_PROJECT_ID=<your project ID>
# First retrieve 1 month of events from packet
$ python get_packet_events.py raw_packet_events.json
# Sanitize packet events and make them ready to use with the simulator
$ python parse_packet_events_into_events.py raw_packet_events.json events.json
# Run the simulation
$ python ../main.py events.json
```