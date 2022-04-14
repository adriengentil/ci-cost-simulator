import csv
import json
import sys
from datetime import timedelta

import matplotlib.pyplot as plt
import numpy as np

from event import ACQUIRE_INSTANCE_ACTION, Event


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

    def acquire(self, event: Event) -> bool:
        if len(self.running_jobs) == self.max_available:
            return False
        else:
            self.running_jobs.append(event.job)
            return True

    def release(self, event: Event) -> bool:
        if event.job in self.running_jobs:
            self.running_jobs.remove(event.job)
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

        self.running_jobs = []

        self.usage_sum = 0
        self.sample_count = 0
        self.billed_time_sec_total = 0

        self.acquired_samples = []

    def acquire(self, event):
        self.running_jobs.append(event)
        return True

    def release(self, event: Event) -> bool:
        acquired_event = None
        for e in self.running_jobs:
            if e.job == event.job:
                acquired_event = e
                break

        if not acquired_event:
            return False

        self.billed_time_sec_total = (
            self.billed_time_sec_total
            + (
                int(
                    (event.timestamp - acquired_event.timestamp)
                    / self.billing_period_sec
                )
                + 1
            )
            * self.billing_period_sec
        )

        self.running_jobs.remove(acquired_event)
        return True

    def observe(self):
        self.usage_sum = self.usage_sum + len(self.running_jobs)
        self.sample_count = self.sample_count + 1
        self.acquired_samples.append(len(self.running_jobs))

    def usage(self):
        return self.usage_sum / self.sample_count


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

        for pool in pool_list:
            if event.action == ACQUIRE_INSTANCE_ACTION:
                success = pool.acquire(event)
            else:
                success = pool.release(event)

            if success:
                break


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
    plt.legend(labels=["OnDemand", "SizedPool", "Total"], fontsize=4, ncol=3)
    plt.xlabel("Pool size")
    plt.ylabel("Billed time (hrs)")

    plt.subplot(3, 1, 2)
    billed_time_on_demand_pool_225 = np.array(billed_time_on_demand_pool) / 3600 * 2.25
    billed_time_sized_pool_25 = np.array(billed_time_sized_pool) / 3600 * 1.6875
    billed_time_total_25 = np.add(
        billed_time_on_demand_pool_225, billed_time_sized_pool_25
    )
    plt.title(
        f"Cost per pool size, OnDemand=2.25, SizedPool=1.6875 (25% discount) billling period={billing_period}s",
        fontsize="x-small",
    )
    plt.grid(axis="y", color="gray", linestyle="dashed", linewidth=0.25)
    plt.yticks(np.arange(0, billed_time_total_25.max(), 5000), fontsize=4)
    plt.xticks(pool_size_matrix, fontsize=4)
    plt.bar(X + 0.00, billed_time_on_demand_pool_225, color="b", width=0.25)
    plt.bar(X + 0.25, billed_time_sized_pool_25, color="g", width=0.25)
    plt.bar(X + 0.50, billed_time_total_25, color="r", width=0.25)
    plt.legend(labels=["OnDemand", "SizedPool", "Total"], fontsize=4, ncol=3)
    plt.xlabel("Pool size")
    plt.ylabel("Cost")

    plt.subplot(3, 1, 3)
    billed_time_sized_pool_50 = np.array(billed_time_sized_pool) / 3600 * 1.125
    billed_time_total_50 = np.add(
        billed_time_on_demand_pool_225, billed_time_sized_pool_50
    )
    plt.title(
        f"Cost per pool size, OnDemand=2.25, SizedPool=1.125 (50% discount) billling period={billing_period}s",
        fontsize="x-small",
    )
    plt.grid(axis="y", color="gray", linestyle="dashed", linewidth=0.25)
    plt.yticks(np.arange(0, billed_time_total_50.max(), 5000), fontsize=4)
    plt.xticks(pool_size_matrix, fontsize=4)
    plt.bar(X + 0.00, billed_time_on_demand_pool_225, color="b", width=0.25)
    plt.bar(X + 0.25, billed_time_sized_pool_50, color="g", width=0.25)
    plt.bar(X + 0.50, billed_time_total_50, color="r", width=0.25)
    plt.legend(labels=["OnDemand", "SizedPool", "Total"], fontsize=4, ncol=3)
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
    plt.close()


def dump_poolsize_billed_time(
    billing_period, pool_size_matrix, billed_time_on_demand_pool, billed_time_sized_pool
):
    rows = zip(pool_size_matrix, billed_time_on_demand_pool, billed_time_sized_pool)
    with open(f"billedtime_{billing_period}.csv", "w", newline="") as csvfile:
        billed_time_data = csv.writer(csvfile)
        billed_time_data.writerows(rows)


def main():
    # filename = "output.csv"
    # job_list = parse_data_into_jobs(filename)
    # event_list = event_list_from_job_list(job_list)
    event_file = sys.argv[1]
    with open(event_file) as json_file:
        event_list = json.load(json_file, object_hook=lambda d: Event(**d))
    event_list.sort(key=lambda x: x.timestamp)
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

            plot_pool_usage(on_demand_pool, sized_pool, billing_period)

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
