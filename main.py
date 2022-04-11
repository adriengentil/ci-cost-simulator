import csv
from datetime import datetime, timedelta
from enum import Enum
from time import time


class Job:
    def __init__(self, name, start, end):
        self.name = name
        self.start = start
        self.end = end


class Event:
    def __init__(self, timestamp, action, job):
        self.timestamp = timestamp
        self.action = action
        self.job = job


class Action(Enum):
    ACQUIRE_INSTANCE = "ACQUIRE"
    RELEASE_INSTANCE = "RELEASE"


class SizedPool:
    def __init__(self, max_size, cost):
        self.max_available = max_size
        self.cost = cost
        self.running_jobs = []
        self.sample_count = 0
        self.usage_sum = 0

    def acquire(self, job):
        if len(self.running_jobs) == self.max_available:
            return False
        else:
            self.running_jobs.append(job)
            return True

    def release(self, job):
        if job in self.running_jobs:
            self.running_jobs.remove(job)
            return True
        return False

    def observe(self):
        self.usage_sum = self.usage_sum + len(self.running_jobs)
        self.sample_count = self.sample_count + 1

    def usage(self):
        return self.usage_sum / self.sample_count

    def cost_period(self, sample_period):
        return (
            self.cost
            * self.max_available
            * (int((sample_period * self.sample_count) / 3600) + 1)
        )


class OnDemandPool:
    def __init__(self, cost):
        self.acquired = 0
        self.cost = cost
        self.running_jobs = []

        self.usage_sum = 0
        self.sample_count = 0
        self.cost_total = 0

    def acquire(self, job):
        self.acquired = self.acquired + 1
        self.cost_total = (
            self.cost_total + (int((job.end - job.start) / 3600) + 1) * self.cost
        )
        self.running_jobs.append(job)
        return True

    def release(self, job):
        if job in self.running_jobs:
            self.acquired = self.acquired - 1
            self.running_jobs.remove(job)
            return True
        return False

    def observe(self):
        self.usage_sum = self.usage_sum + self.acquired
        self.sample_count = self.sample_count + 1

    def usage(self):
        return self.usage_sum / self.sample_count

    def cost_period(self):
        return self.cost_total


def event_list_from_job_list(job_list):
    event_list = []
    for job in job_list:
        event_list.append(Event(job.start, Action.ACQUIRE_INSTANCE, job))
        event_list.append(Event(job.end, Action.RELEASE_INSTANCE, job))
    event_list.sort(key=lambda x: x.timestamp)

    return event_list


def observe_pools(pool_list):
    for pool in pool_list:
        pool.observe()


def simulate(event_list):
    sample_period = 5

    on_demand_pool = OnDemandPool(2)
    sized_pool = SizedPool(0, 2)
    pool_list = [sized_pool, on_demand_pool]

    sample_start = event_list[0].timestamp
    sample_count = 0
    for event in event_list:
        sample_count_from_start = int((event.timestamp - sample_start) / sample_period)
        for i in range(sample_count_from_start - sample_count):
            observe_pools(pool_list)
        sample_count = sample_count_from_start

        print(
            "{} - job {} to be {}".format(event.timestamp, event.job.name, event.action)
        )
        for pool in pool_list:
            if event.action == Action.ACQUIRE_INSTANCE:
                success = pool.acquire(event.job)
            else:
                success = pool.release(event.job)

            if success:
                break

    print(
        "Test duration: {}".format(
            timedelta(seconds=event_list[-1].timestamp - event_list[0].timestamp)
        )
    )
    print(
        "sized_pool usage (avg number for machines in use for the test duration): {}".format(
            sized_pool.usage()
        )
    )
    print("sized_pool cost: {}".format(sized_pool.cost_period(sample_period)))
    print(
        "on_demand_pool usage (avg number for machines in use for the test duration): {}".format(
            on_demand_pool.usage()
        )
    )
    print("on_demand_pool cost: {}".format(on_demand_pool.cost_period()))


def parse_data_into_jobs(filename):
    job_list = []
    with open(filename, newline="") as csvfile:
        raw_jobs = csv.reader(csvfile)
        for raw_job in raw_jobs:
            raw_id = raw_job[0]
            raw_duration = raw_job[1]
            raw_start = raw_job[2]
            raw_name = raw_job[5]

            try:
                start = datetime.strptime(
                    raw_start, "%Y-%m-%dT%H:%M:%S%z"
                )  # 2022-01-18T08:00:48Z
            except:
                print(f"failled to parse date {raw_start} for job id {raw_id}")
                raise
            start_seconds = start.timestamp()
            job_list.append(
                Job(
                    f"{raw_name}-{raw_id}",
                    start_seconds,
                    start_seconds + float(raw_duration),
                )
            )

    return job_list


def main():
    filename = "output.csv"
    job_list = parse_data_into_jobs(filename)
    event_list = event_list_from_job_list(job_list)
    simulate(event_list)


if __name__ == "__main__":
    main()
