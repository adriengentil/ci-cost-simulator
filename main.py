import csv
from datetime import datetime, timedelta
from enum import Enum
from time import time
from turtle import width

import matplotlib.pyplot as plt
import numpy as np


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
    def __init__(self, name, max_size, time_period_sec, billing_period_sec):
        self.name = name
        self.max_available = max_size
        self.billing_period_sec = billing_period_sec
        self.running_jobs = []
        self.sample_count = 0
        self.usage_sum = 0
        self.acquired_samples = []
        self.billed_time_sec_total = (
            self.max_available
            * (int(time_period_sec / billing_period_sec) + 1)
            * billing_period_sec
        )

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
        self.acquired_samples.append(len(self.running_jobs))

    def usage(self):
        return self.usage_sum / self.sample_count


class OnDemandPool:
    def __init__(self, name, billing_period_sec):
        self.name = name
        self.billing_period_sec = billing_period_sec

        self.acquired = 0
        self.running_jobs = []

        self.usage_sum = 0
        self.sample_count = 0
        self.billed_time_sec_total = 0

        self.acquired_samples = []

    def acquire(self, job):
        self.acquired = self.acquired + 1
        self.billed_time_sec_total = (
            self.billed_time_sec_total
            + (int((job.end - job.start) / self.billing_period_sec) + 1)
            * self.billing_period_sec
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
        self.acquired_samples.append(self.acquired)

    def usage(self):
        return self.usage_sum / self.sample_count


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


def simulate(event_list, pool_list, sample_period):
    sample_start = event_list[0].timestamp
    sample_count = 0
    for event in event_list:
        sample_count_from_start = int((event.timestamp - sample_start) / sample_period)
        for i in range(sample_count_from_start - sample_count):
            observe_pools(pool_list)
        sample_count = sample_count_from_start

        # print(
        #     "{} - job {} to be {}".format(event.timestamp, event.job.name, event.action)
        # )
        for pool in pool_list:
            if event.action == Action.ACQUIRE_INSTANCE:
                success = pool.acquire(event.job)
            else:
                success = pool.release(event.job)

            if success:
                break


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


def plot_poolsize_billed_time(
    billing_period, pool_size_matrix, billed_time_on_demand_pool, billed_time_sized_pool
):
    plt.rcParams["axes.axisbelow"] = True

    billed_time_total = np.add(billed_time_on_demand_pool, billed_time_sized_pool)
    X = np.array(pool_size_matrix)
    plt.subplot(3, 1, 1)
    plt.title(
        f"Billed hours per pool size, billling period={billing_period}s",
        fontsize="x-small",
    )
    plt.grid(axis="y", color="gray", linestyle="dashed", linewidth=0.25)
    plt.yticks(np.arange(0, billed_time_total.max() / 3600, 2000), fontsize=4)
    plt.xticks(pool_size_matrix, fontsize=4)
    plt.bar(
        X + 0.00, np.array(billed_time_on_demand_pool) / 3600, color="b", width=0.25
    )
    plt.bar(X + 0.25, np.array(billed_time_sized_pool) / 3600, color="g", width=0.25)
    plt.bar(X + 0.50, billed_time_total / 3600, color="r", width=0.25)
    plt.legend(labels=["OnDemand", "SizedPool", "Total"], fontsize="x-small")
    plt.xlabel("Pool size")
    plt.ylabel("Billed time (hrs)")

    plt.subplot(3, 1, 2)
    billed_time_on_demand_pool_225 = np.array(billed_time_on_demand_pool) / 3600 * 2.25
    billed_time_sized_pool_75 = np.array(billed_time_sized_pool) / 3600 * 2.082
    billed_time_total_75 = np.add(
        billed_time_on_demand_pool_225, billed_time_sized_pool_75
    )
    plt.title(
        f"Cost per pool size, OnDemand=2.25$, SizedPool=2.082$ (7.5% discount) billling period={billing_period}s",
        fontsize="x-small",
    )
    plt.grid(axis="y", color="gray", linestyle="dashed", linewidth=0.25)
    plt.yticks(np.arange(0, billed_time_total_75.max(), 5000), fontsize=4)
    plt.xticks(pool_size_matrix, fontsize=4)
    plt.bar(X + 0.00, billed_time_on_demand_pool_225, color="b", width=0.25)
    plt.bar(X + 0.25, billed_time_sized_pool_75, color="g", width=0.25)
    plt.bar(X + 0.50, billed_time_total_75, color="r", width=0.25)
    plt.legend(labels=["OnDemand", "SizedPool", "Total"], fontsize="x-small")
    plt.xlabel("Pool size")
    plt.ylabel("Cost")

    plt.subplot(3, 1, 3)
    billed_time_sized_pool_15 = np.array(billed_time_sized_pool) / 3600 * 1.91
    billed_time_total_15 = np.add(
        billed_time_on_demand_pool_225, billed_time_sized_pool_15
    )
    plt.title(
        f"Cost per pool size, OnDemand=2.25$, SizedPool=1.91$ (15% discount) billling period={billing_period}s",
        fontsize="x-small",
    )
    plt.grid(axis="y", color="gray", linestyle="dashed", linewidth=0.25)
    plt.yticks(np.arange(0, billed_time_total_15.max(), 5000), fontsize=4)
    plt.xticks(pool_size_matrix, fontsize=4)
    plt.bar(X + 0.00, billed_time_on_demand_pool_225, color="b", width=0.25)
    plt.bar(X + 0.25, billed_time_sized_pool_15, color="g", width=0.25)
    plt.bar(X + 0.50, billed_time_total_15, color="r", width=0.25)
    plt.legend(labels=["OnDemand", "SizedPool", "Total"], fontsize="x-small")
    plt.xlabel("Pool size")
    plt.ylabel("Cost")
    plt.tight_layout()
    plt.savefig(f"billedtime_{billing_period}.png", dpi=300)
    plt.close()


def plot_poolsize_avg_usage(
    billing_period,
    pool_size_matrix,
    average_usage_on_demand_pool,
    average_usage_sized_pool,
):
    X = np.array(pool_size_matrix)
    plt.subplot(2, 1, 1)
    plt.title(
        f"Number of acquired machines in average, billling period={billing_period}s"
    )
    plt.bar(X + 0.00, average_usage_on_demand_pool, color="b", width=0.25)
    plt.bar(X + 0.25, average_usage_sized_pool, color="g", width=0.25)
    plt.legend(labels=["OnDemand", "SizedPool"])
    plt.xlabel("Pool size")
    plt.ylabel("Acquired machines")
    plt.subplot(2, 1, 2)
    plt.title(f"Sized pool usage, billling period={billing_period}s")
    plt.bar(
        X + 0.00,
        np.multiply(np.divide(average_usage_sized_pool, X), 100),
        color="g",
        width=0.25,
    )
    plt.xlabel("Pool size")
    plt.ylabel("Usage in %")
    plt.tight_layout()
    plt.savefig(f"avg_usage_{billing_period}.png", dpi=300)
    plt.close()


def plot_pool_usage(on_demand_pool, sized_pool, billing_period):
    plt.title(
        f"Pool usage over simulation samples, pool size={sized_pool.max_available}"
    )
    X = np.arange(len(on_demand_pool.acquired_samples))
    plt.plot(X, on_demand_pool.acquired_samples, color="b", linewidth=0.5)
    plt.plot(X, sized_pool.acquired_samples, color="g", linewidth=0.5)
    plt.legend(labels=["OnDemand", "SizedPool"])
    plt.xlabel("Samples")
    plt.ylabel(f"Acquired machines")
    plt.savefig(f"usage_{sized_pool.max_available}_{billing_period}.png", dpi=300)


def dump_poolsize_billed_time(
    billing_period, pool_size_matrix, billed_time_on_demand_pool, billed_time_sized_pool
):
    rows = zip(pool_size_matrix, billed_time_on_demand_pool, billed_time_sized_pool)
    with open(f"billedtime_{billing_period}.csv", "w", newline="") as csvfile:
        billed_time_data = csv.writer(csvfile)
        billed_time_data.writerows(rows)


def main():
    filename = "output.csv"
    job_list = parse_data_into_jobs(filename)
    event_list = event_list_from_job_list(job_list)
    simulation_duration_sec = event_list[-1].timestamp - event_list[0].timestamp
    print(
        "Simulation duration: {} -> {}s".format(
            timedelta(seconds=simulation_duration_sec),
            simulation_duration_sec,
        )
    )

    billing_period_matrix = [60, 3600]
    pool_size_matrix = list(range(0, 21))
    sample_period = 5

    billed_time_on_demand_pool = []
    billed_time_sized_pool = []

    average_usage_on_demand_pool = []
    average_usage_sized_pool = []

    for billing_period in billing_period_matrix:
        for pool_size in pool_size_matrix:
            on_demand_pool = OnDemandPool(
                f"ondemand_{pool_size}_{billing_period}", billing_period
            )
            sized_pool = SizedPool(
                "sizedpool_{pool_size}_{billing_period}",
                pool_size,
                simulation_duration_sec,
                billing_period,
            )
            pool_list = [sized_pool, on_demand_pool]

            print(f"Simulate pool_size={pool_size} billing_period={billing_period}")
            simulate(event_list, pool_list, sample_period)

            # plot_pool_usage(on_demand_pool, sized_pool, billing_period)

            billed_time_on_demand_pool.append(on_demand_pool.billed_time_sec_total)
            billed_time_sized_pool.append(sized_pool.billed_time_sec_total)

            average_usage_on_demand_pool.append(on_demand_pool.usage())
            average_usage_sized_pool.append(sized_pool.usage())

        plot_poolsize_billed_time(
            billing_period,
            pool_size_matrix,
            billed_time_on_demand_pool,
            billed_time_sized_pool,
        )
        dump_poolsize_billed_time(
            billing_period,
            pool_size_matrix,
            billed_time_on_demand_pool,
            billed_time_sized_pool,
        )
        plot_poolsize_avg_usage(
            billing_period,
            pool_size_matrix,
            average_usage_on_demand_pool,
            average_usage_sized_pool,
        )
        billed_time_on_demand_pool = []
        billed_time_sized_pool = []
        average_usage_on_demand_pool = []
        average_usage_sized_pool = []


if __name__ == "__main__":
    main()
