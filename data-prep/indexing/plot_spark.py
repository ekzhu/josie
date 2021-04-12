import sys
import json
import collections
import matplotlib.pyplot as plt
import numpy as np

attributes={
        "internal.metrics.executorRunTime" : {
            "transform_func" : lambda xs : xs / (60 * 1000),
            "ylabel" : "Duration (minute)",
            "xlabel" : "Tasks",
        },
        "internal.metrics.peakExecutionMemory" : {
            "transform_func" : lambda xs : xs / (1024 * 1024),
            "ylabel" : "Memory Footprint (MB)",
            "xlabel" : "Tasks",
        },
}

def read_tasks(logfilename, attributes):
    results = collections.defaultdict(
            lambda : collections.defaultdict(
                lambda : collections.defaultdict(
                    lambda : 0)))
    with open(logfilename) as f:
        for line in f:
            record = json.loads(line)
            if "Event" not in record or \
                    record["Event"] != "SparkListenerTaskEnd":
                continue
            if "Task End Reason" not in record or \
                    record["Task End Reason"]["Reason"] != "Success":
                continue
            stage_id = record["Stage ID"]
            task_id = record["Task Info"]["Task ID"]
            accumulables = record["Task Info"]["Accumulables"]
            for item in accumulables:
                attribute = item["Name"]
                if item["Name"] in attributes:
                    results[stage_id][task_id][attribute] = item["Update"]
    return results

logfilename = sys.argv[1]
print("Reading tasks from log file...")
results = read_tasks(logfilename, attributes)
print("Finished reading tasks")

stage_ids = sorted(results.keys())
for attribute, config in attributes.items():
    values = []
    for stage_id in stage_ids:
        task_ids = results[stage_id].keys()
        values.extend([results[stage_id][task_id][attribute]
                for task_id in task_ids])
    values = np.sort(np.array([v for v in values if v != 0]))
    values = config["transform_func"](values)
    print(attribute, np.mean(values), np.median(values), np.max(values))
    plt.plot(values, "-")
    plt.ylabel(config["ylabel"])
    plt.xlabel(config["xlabel"])
    plt.grid()
    plt.fill_between(range(len(values)), values, interpolate=True, alpha=0.5)
    plt.tight_layout()
    plt.savefig("{}_{}".format(logfilename, attribute).replace(".", "_") + \
            ".pdf")
    plt.close()

