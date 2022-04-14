from typing import Final


ACQUIRE_INSTANCE_ACTION: Final = "ACQUIRE"
RELEASE_INSTANCE_ACTION: Final = "RELEASE"


class Event:
    def __init__(self, timestamp: float, action: str, job: str):
        self.timestamp = timestamp
        self.action = action
        self.job = job
