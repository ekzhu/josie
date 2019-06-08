import matplotlib
import matplotlib.pyplot as plt
plt.rc('text', usetex=True)
plt.rc('font', family='serif')
from matplotlib.ticker import PercentFormatter
import pandas as pd
import sys
import os
import numpy as np
import argparse
from scipy import stats

result_dirs = {
        "default": "results",
}
scale = 100
search_funcs = [
    ("MergeList-D", "merge_distinct_list"),
    ("ProbeSet-D", "probe_set_optimized"),
    # ("MergeList", "merge_list"),
    # ("ProbeSet", "probe_set_suffix"),
    ("JOSIE-D", "merge_probe_cost_model_greedy"),
    ("LSHEnsemble-60", "lsh_ensemble_precision_60"),
    ("LSHEnsemble-90", "lsh_ensemble_precision_90"),
    ("MinHashLSH", "lsh_ensemble_partition_1"),
    ("LSHEnsemble (4)", "lsh_ensemble_partition_4"),
    ("LSHEnsemble (8)", "lsh_ensemble_partition_8"),
    ("LSHEnsemble (16)", "lsh_ensemble_partition_16"),
    ("LSHEnsemble (32)", "lsh_ensemble_partition_32"),
]
dir_names = dict(search_funcs)
colors = dict((func, color) for (func,_), color in
        zip(search_funcs, plt.rcParams['axes.prop_cycle'].by_key()['color']))

def get_intervals(max_query_size, num_interval):
    m = int(max_query_size / num_interval)
    intervals = [(10, m)] + [(m*i, m*(i+1)) for i in range(1, num_interval)]
    return intervals

configs = {
    "canada_us_uk" : {
        "name" : "canada_us_uk",
        "query_scales" : [
            "1k",
            "10k",
            "100k",
        ],
        "intervals": {
            "1k" : get_intervals(1000, 10),
            "10k" : get_intervals(10000, 10),
            "100k" : get_intervals(100000, 10),
        },
        "scales" : [100,],
        "ks" : [
            #1,
            5,
            10,
            20,
            #30,
            #50,
        ],
        "plot_distinct_list" : [
            ("MergeList", "MergeList-D"),
            ("ProbeSet", "ProbeSet-D"),
            ("JOSIE", "JOSIE-D"),
        ],
        "plot_mean" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_set_read" : ["ProbeSet-D", "JOSIE-D"],
        "plot_list_read" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_std" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_counter" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_set_size_read" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_list_size_read" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_ignores" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_memory_footprint" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_composite" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
    },
    "canada_us_uk_lsh" : {
        "name" : "canada_us_uk",
        "query_scales" : [
            "1k",
            "10k",
            "100k",
        ],
        "intervals": {
            "1k" : get_intervals(1000, 10),
            "10k" : get_intervals(10000, 10),
            "100k" : get_intervals(100000, 10),
        },
        "scales" : [100,],
        "ks" : [
            #1,
            5,
            10,
            20,
            #30,
            #50,
        ],
        "plot_mean" : ["LSHEnsemble-60", "LSHEnsemble-90", "JOSIE-D"],
        "plot_set_read" : ["LSHEnsemble-60", "LSHEnsemble-90", "JOSIE-D"],
        "plot_std" : ["LSHEnsemble-60", "LSHEnsemble-90", "JOSIE-D"],
        "plot_composite" : ["LSHEnsemble-60", "LSHEnsemble-90", "JOSIE-D"],
    },
    "canada_us_uk_lsh_only" : {
        "name" : "canada_us_uk",
        "query_scales" : [
            "1k",
            "10k",
            "100k",
        ],
        "intervals": {
            "1k" : get_intervals(1000, 10),
            "10k" : get_intervals(10000, 10),
            "100k" : get_intervals(100000, 10),
        },
        "scales" : [100,],
        "ks" : [
            #1,
            5,
            10,
            20,
            30,
            50,
        ],
        "plot_mean" : ["MinHashLSH", "LSHEnsemble (4)", "LSHEnsemble (8)",
            "LSHEnsemble (16)", "LSHEnsemble (32)"],
        "plot_precision" : ["MinHashLSH", "LSHEnsemble (4)", "LSHEnsemble (8)",
            "LSHEnsemble (16)", "LSHEnsemble (32)"],
    },
    "webtable" : {
        "name" : "webtable",
        "query_scales" : [
            "100",
            "1k",
            "10k",
        ],
        "intervals": {
            "100" : get_intervals(100, 10),
            "1k" : get_intervals(1000, 10),
            "10k" : get_intervals(5000, 5),},
        "scales" : [100,],
        "ks" : [
            #1,
            5,
            10,
            20,
            #30,
            #50,
        ],
        "plot_distinct_list" : [
            ("MergeList", "MergeList-D"),
            ("ProbeSet", "ProbeSet-D"),
            ("JOSIE", "JOSIE-D"),
        ],
        "plot_mean" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_set_read" : ["ProbeSet-D", "JOSIE-D", "JOSIE-D"],
        "plot_list_read" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_std" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_counter" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_set_size_read" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_list_size_read" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_max_ignores" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_memory_footprint" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
        "plot_composite" : ["MergeList-D", "ProbeSet-D", "JOSIE-D"],
    },
}

def get_df(result_dir, benchmark, search_func, query_scale="1k", k=10):
    if search_func in ("MergeList", "MergeList-D") and k != 10:
        k = 10
    filename = os.path.join(result_dir, benchmark, str(scale), dir_names[search_func],
            "{}_{}.csv".format(query_scale, k))
    df = pd.read_csv(filename, index_col="query_id")
    return df

def mean_on_intervals(xs, ys, intervals, mean_func):
    ys_means = np.array([mean_func(ys[(xs >= i) & (xs < j)]) for i, j in intervals])
    xs_means = np.array([(i + j)/2 for i, j in intervals])
    return xs_means, ys_means

def get_axis(axes, i, j, config):
    num_k = len(config["ks"])
    num_scales = len(config["query_scales"])
    if num_k == 1 and num_scales == 1:
        return axes
    if num_k == 1 and num_scales > 1:
        return axes[j]
    if num_k > 1 and num_scales == 1:
        return axes[i]
    return axes[i, j]

def plot_distinct_list(result_dir, config, output):
    search_funcs = config["plot_distinct_list"]
    nrow, ncol = len(config["ks"]), len(config["query_scales"])
    fig, axes = plt.subplots(nrow, ncol, sharex="col", sharey="col",
            figsize=(ncol * 2.5, nrow * 2))
    for i, k in enumerate(config["ks"]):
        for j, query_scale in enumerate(config["query_scales"]):
            for func_nd, func_d in search_funcs:
                df_nd = get_df(result_dir, config["name"], func_nd, query_scale, k)
                df_d = get_df(result_dir, config["name"], func_d, query_scale, k)
                df = pd.merge(df_nd, df_d, on="query_id")
                sizes = np.array(df["query_num_token_x"])
                durations_nd = np.array(df["duration_x"])
                durations_d = np.array(df["duration_y"])
                duration_changes = (durations_nd - durations_d) / durations_nd * 100.0 + 1
                #print("k = {}, query_scale = {}, mean improve = {} pct".format(
                #    k, query_scale, np.mean(duration_changes)))
                intervals = config["intervals"][query_scale]
                xs, ys = mean_on_intervals(sizes, duration_changes, intervals, np.mean)
                ax = get_axis(axes, i, j, config)
                ax.plot(xs, ys, "-x", label=func_nd, color=colors[func_d])
                if i == 1 and j == 0:
                    ax.set_ylabel("Percentage Improvement in Mean Duration")
                if j == ncol - 1:
                    ax.yaxis.set_label_position("right")
                    ax.set_ylabel("k = {}".format(k))
                if i == len(config["ks"])-1:
                    ax.set_xlabel("Query Size")
                ax.get_yaxis().set_major_formatter(PercentFormatter())
                ax.grid(True)
    plt.subplots_adjust(left=0.2, wspace=0.35, hspace=0.1)
    fig.legend(get_axis(axes,nrow-1,ncol-1,config).get_lines(),
            [p[0] for p in search_funcs],
            bbox_to_anchor=(0.545, 0), loc="center",
            bbox_transform=plt.gcf().transFigure, ncol=len(search_funcs))
    plt.savefig(output, bbox_inches='tight', pad_inches=0)
    plt.close()

def plot_mean(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_mean",
            lambda df, _ : np.array(df["duration"]) / 1000.0,
            "Mean Duration (sec)", agg_func=np.mean)

def plot_std(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_std",
            lambda df, _ : np.array(df["duration"]) / 1000.0,
            "Standard Deviation Duration (sec)", agg_func=np.std)

def plot_set_read(result_dir, config, output):
    _plot_mean_measure(config, output, "plot_set_read",
            lambda df, _ : np.array(df["num_set_read"]),
            "Mean Number of Sets Read")

def plot_list_read(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_list_read",
            lambda df, _ : np.array(df["num_list_read"]),
            "Mean Number of Lists Read")

def plot_max_counter(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_max_counter",
            lambda df, _ : np.array(df["max_counter_size"]),
            "Mean Num. of Allocated Candidate Slots")

def plot_max_set_size_read(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_max_set_size_read",
            lambda df, _ : np.array(df["max_set_size_read"]),
            "Mean Allocated for Reading Sets")

def plot_max_list_size_read(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_max_list_size_read",
            lambda df, _ : np.array(df["max_list_size_read"]),
            "Mean Allocated for Reading Lists")

def plot_max_ignores(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_max_ignores",
            lambda df, _ : np.array(df["max_ignore_size"]),
            "Mean Allocated for Tracking Sets")

def _compute_footprint(df, search_func):
    ignores = np.array(df["max_ignore_size"])
    list_reads = np.array(df["max_list_size_read"])
    set_reads = np.array(df["max_set_size_read"])
    counters = np.array(df["max_counter_size"])
    if search_func.startswith("LSH") and np.sum(ignores) == 0:
        # NOTE: fix for forgeting to record ignores for LSH.
        ignores = np.array(df["num_set_read"])
    # Sets and Ignores are taking up 1 integer for every slot
    total = set_reads + ignores
    if search_func.startswith("MergeList"):
        total += list_reads + counters * 2
    if search_func.startswith("ProbeSet") or search_func.startswith("JOSIE"):
        total += list_reads * 3
    if search_func.startswith("JOSIE"):
        total += counters * 5
    # Each integer is 4 byte
    return (total * 4) / 1024.0

def plot_memory_footprint(result_dir, config, output):
    _plot_mean_measure(result_dir, config, output, "plot_memory_footprint",
            _compute_footprint, "Mean Memory Footprint (KB)")

def _plot_mean_measure(result_dir, config, output, plot_config_name, measure_func, ylabel,
        agg_func=np.mean):
    search_funcs = config[plot_config_name]
    nrow, ncol = len(config["ks"]), len(config["query_scales"])
    fig, axes = plt.subplots(nrow, ncol, sharex="col", sharey="col",
            figsize=(ncol * 2.5, nrow * 2))
    for i, k in enumerate(config["ks"]):
        for j, query_scale in enumerate(config["query_scales"]):
            for func in search_funcs:
                df = get_df(result_dir, config["name"], func, query_scale, k)
                sizes = np.array(df["query_num_token"])
                measures = measure_func(df, func)
                intervals = config["intervals"][query_scale]
                xs, ys = mean_on_intervals(sizes, measures, intervals, agg_func)
                ax = get_axis(axes, i, j, config)
                ax.plot(xs, ys, "+-", label=func, color=colors[func])
                if i == 1 and j == 0:
                    ax.set_ylabel(ylabel)
                if j == ncol - 1:
                    ax.yaxis.set_label_position("right")
                    ax.set_ylabel("k = {}".format(k))
                if i == len(config["ks"])-1:
                    ax.set_xlabel("Query Size")
                ax.grid(True)
        ax.xaxis.set_tick_params(rotation=20)
    plt.subplots_adjust(left=0.2, wspace=0.2, hspace=0.1)
    fig.legend(get_axis(axes,nrow-1,ncol-1,config).get_lines(), search_funcs,
            bbox_to_anchor=(0.5, 0), loc="center",
            bbox_transform=plt.gcf().transFigure, ncol=len(search_funcs))
    plt.savefig(output, bbox_inches='tight', pad_inches=0)
    plt.close()

def plot_composite(result_dir, config, output_prefix, output_format="pdf"):
    search_funcs = config["plot_composite"]
    ncol = len(config["query_scales"])
    for i, k in enumerate(config["ks"]):
        fig, axes = plt.subplots(4, ncol, sharex='col', sharey=False,
                figsize=(ncol * 2.5, 4 * 2))
        for j, query_scale in enumerate(config["query_scales"]):
            for func in search_funcs:
                df = get_df(result_dir, config["name"], func, query_scale, k)
                sizes = np.array(df["query_num_token"])
                # plot std
                durations = np.array(df["duration"]) / 1000.0
                intervals = config["intervals"][query_scale]
                xs, ys = mean_on_intervals(sizes, durations, intervals, np.std)
                ax = get_axis(axes, 0, j, config)
                ax.plot(xs, ys, "+-", label=func, color=colors[func])
                ax.grid(True)
                if j == 0:
                    ax.set_ylabel("Std. of Duration")
                if not func.startswith("MergeList"):
                    # Plot num sets
                    num_sets = np.array(df["num_set_read"])
                    xs, ys = mean_on_intervals(sizes, num_sets, intervals, np.mean)
                    ax = get_axis(axes, 1, j, config)
                    ax.plot(xs, ys, "+-", label=func, color=colors[func])
                    ax.grid(True)
                    if j == 0:
                        ax.set_ylabel("Mean \# of Sets Read")
                # plot num lists
                num_lists = np.array(df["num_list_read"])
                xs, ys = mean_on_intervals(sizes, num_lists, intervals, np.mean)
                ax = get_axis(axes, 2, j, config)
                ax.plot(xs, ys, "+-", label=func, color=colors[func])
                ax.grid(True)
                if j == 0:
                    ax.set_ylabel("Mean \# of Lists Read")
                # Plot memory footprint
                footprints = _compute_footprint(df, func)
                xs, ys = mean_on_intervals(sizes, footprints, intervals, np.mean)
                ax = get_axis(axes, 3, j, config)
                ax.plot(xs, ys, "+-", label=func, color=colors[func])
                ax.grid(True)
                if j == 0:
                    ax.set_ylabel("Mean Footprint (KB)")
                ax.set_xlabel("Query Size")
        ax.xaxis.set_tick_params(rotation=20)
        fig.legend(get_axis(axes,4-1,ncol-1,config).get_lines(), search_funcs,
                bbox_to_anchor=(0.5, 0), loc="center",
                bbox_transform=plt.gcf().transFigure, ncol=len(search_funcs))
        plt.subplots_adjust(left=0.2, wspace=0.3, hspace=0.1)
        plt.savefig("{}_k_{}.{}".format(output_prefix, k, output_format), bbox_inches='tight',
                pad_inches=0)
        plt.close()

def plot_composite_lsh(result_dir, config, output_prefix, output_format="pdf"):
    search_funcs = config["plot_composite"]
    ncol = len(config["query_scales"])
    for i, k in enumerate(config["ks"]):
        fig, axes = plt.subplots(3, ncol, sharex='col', sharey=False,
                figsize=(ncol * 2.5, 3 * 2))
        for j, query_scale in enumerate(config["query_scales"]):
            for func in search_funcs:
                df = get_df(result_dir, config["name"], func, query_scale, k)
                sizes = np.array(df["query_num_token"])
                # plot std
                durations = np.array(df["duration"]) / 1000.0
                intervals = config["intervals"][query_scale]
                xs, ys = mean_on_intervals(sizes, durations, intervals, np.std)
                ax = get_axis(axes, 0, j, config)
                ax.plot(xs, ys, "+-", label=func, color=colors[func])
                ax.grid(True)
                if j == 0:
                    ax.set_ylabel("Std. of Duration")
                # Plot num sets
                num_sets = np.array(df["num_set_read"])
                xs, ys = mean_on_intervals(sizes, num_sets, intervals, np.mean)
                ax = get_axis(axes, 1, j, config)
                ax.plot(xs, ys, "+-", label=func, color=colors[func])
                ax.grid(True)
                if j == 0:
                    ax.set_ylabel("Mean \# of Sets Read")
                # Plot memory footprint
                num_sets = np.array(df["num_set_read"])
                footprints = _compute_footprint(df, func)
                xs, ys = mean_on_intervals(sizes, footprints, intervals, np.mean)
                ax = get_axis(axes, 2, j, config)
                ax.plot(xs, ys, "+-", label=func, color=colors[func])
                ax.grid(True)
                if j == 0:
                    ax.set_ylabel("Mean Footprint (KB)")
                ax.set_xlabel("Query Size")
        ax.xaxis.set_tick_params(rotation=20)
        fig.legend(get_axis(axes,3-1,ncol-1,config).get_lines(), search_funcs,
                bbox_to_anchor=(0.5, 0), loc="center",
                bbox_transform=plt.gcf().transFigure, ncol=len(search_funcs))
        plt.subplots_adjust(left=0.2, wspace=0.3, hspace=0.1)
        plt.savefig("{}_k_{}.{}".format(output_prefix, k, output_format), bbox_inches='tight',
                pad_inches=0)
        plt.close()

def plot_precision(result_dir, config, output):
    search_funcs = config["plot_precision"]
    nrow, ncol = len(config["ks"]), len(config["query_scales"])
    fig, axes = plt.subplots(nrow, ncol, sharex="col", sharey="col",
            figsize=(ncol * 2.5, nrow * 2))
    for i, k in enumerate(config["ks"]):
        for j, query_scale in enumerate(config["query_scales"]):
            for func in search_funcs:
                df = get_df(result_dir, config["name"], func, query_scale, k)
                sizes = np.array(df["query_num_token"])
                precisions = np.array(df["lsh_precision"])
                intervals = config["intervals"][query_scale]
                xs, ys = mean_on_intervals(sizes, precisions, intervals, np.mean)
                ax = get_axis(axes, i, j, config)
                ax.plot(xs, ys, "-x", label=func, color=colors[func])
                if i == 1 and j == 0:
                    ax.set_ylabel("Mean Precision")
                if j == ncol - 1:
                    ax.yaxis.set_label_position("right")
                    ax.set_ylabel("k = {}".format(k))
                if i == len(config["ks"])-1:
                    ax.set_xlabel("Query Size")
                ax.grid(True)
    plt.subplots_adjust(left=0.2, wspace=0.2, hspace=0.1)
    fig.legend(get_axis(axes,nrow-1,ncol-1,config).get_lines(), search_funcs,
            bbox_to_anchor=(0.545, 0), loc="center",
            bbox_transform=plt.gcf().transFigure, ncol=len(search_funcs))
    plt.savefig(output, bbox_inches='tight', pad_inches=0)
    plt.close()

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", type=str, default="plots")
args = parser.parse_args(sys.argv[1:])

# Plots for open data benchmark
plot_mean(result_dirs["default"], configs["canada_us_uk"], os.path.join(args.output_dir, "mean_canada_us_uk.pdf"))
# plot_std(configs["canada_us_uk"], os.path.join(args.output_dir, "std_canada_us_uk.pdf"))
# plot_set_read(configs["canada_us_uk"], os.path.join(args.output_dir, "set_read_canada_us_uk.pdf"))
# plot_list_read(configs["canada_us_uk"], os.path.join(args.output_dir, "list_read_canada_us_uk.pdf"))
# plot_max_counter(configs["canada_us_uk"], os.path.join(args.output_dir, "max_counter_canada_us_uk.pdf"))
# plot_max_set_size_read(configs["canada_us_uk"], os.path.join(args.output_dir, "max_set_size_read_canada_us_uk.pdf"))
# plot_max_list_size_read(configs["canada_us_uk"], os.path.join(args.output_dir, "max_list_size_read_canada_us_uk.pdf"))
# plot_max_ignores(configs["canada_us_uk"], os.path.join(args.output_dir, "max_ignores_canada_us_uk.pdf"))
# plot_memory_footprint(configs["canada_us_uk"], os.path.join(args.output_dir, "memory_footprint_canada_us_uk.pdf"))
plot_composite(result_dirs["default"], configs["canada_us_uk"], os.path.join(args.output_dir, "composite_canada_us_uk"))

# Plot for comparing with LSH
plot_mean(result_dirs["default"], configs["canada_us_uk_lsh"], os.path.join(args.output_dir, "mean_lsh.pdf"))
# plot_std(configs["canada_us_uk_lsh"], os.path.join(args.output_dir, "std_lsh.pdf"))
# plot_set_read(configs["canada_us_uk_lsh"], os.path.join(args.output_dir, "set_read_lsh.pdf"))
plot_composite_lsh(result_dirs["default"], configs["canada_us_uk_lsh"], os.path.join(args.output_dir, "composite_canada_us_uk_lsh"))

# Plot for webtable benchmark
plot_mean(result_dirs["default"], configs["webtable"], os.path.join(args.output_dir, "mean_webtable.pdf"))
# plot_std(configs["webtable"], os.path.join(args.output_dir, "std_webtable.pdf"))
# plot_set_read(configs["webtable"], os.path.join(args.output_dir, "set_read_webtable.pdf"))
# plot_list_read(configs["webtable"], os.path.join(args.output_dir, "list_read_webtable.pdf"))
# plot_max_counter(configs["webtable"], os.path.join(args.output_dir, "max_counter_webtable.pdf"))
# plot_max_set_size_read(configs["webtable"], os.path.join(args.output_dir, "max_set_size_read_webtable.pdf"))
# plot_max_list_size_read(configs["webtable"], os.path.join(args.output_dir, "max_list_size_read_webtable.pdf"))
# plot_max_ignores(configs["webtable"], os.path.join(args.output_dir, "max_ignores_webtable.pdf"))
# plot_memory_footprint(configs["webtable"], os.path.join(args.output_dir, "memory_footprint_webtable.pdf"))
plot_composite(result_dirs["default"], configs["webtable"], os.path.join(args.output_dir, "composite_webtable"))

# Plot for LSH Precision
# plot_mean(configs["canada_us_uk_lsh_only"], os.path.join(args.output_dir,
#     "mean_lsh_only.pdf"))
# plot_precision(configs["canada_us_uk_lsh_only"], os.path.join(args.output_dir, "precision_lsh_only.pdf"))
