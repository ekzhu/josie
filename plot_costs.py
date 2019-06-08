import matplotlib
import matplotlib.pyplot as plt
plt.rc('text', usetex=True)
plt.rc('font', family='serif')
import pandas as pd
import sys
import os
import numpy as np
import argparse
import psycopg2
from psycopg2 import sql

parser = argparse.ArgumentParser()
parser.add_argument("-H", "--pg-host", default="localhost")
parser.add_argument("-p", "--pg-port", default=5442)
parser.add_argument("-s", "--pg-read-set-cost-table",
        default="canada_us_uk_read_set_cost_samples")
parser.add_argument("-l", "--pg-read-list-cost-table",
        default="canada_us_uk_read_list_cost_samples")
args = parser.parse_args(sys.argv[1:])
parser.add_argument("-o", "--output-dir", default="plots")
args = parser.parse_args(sys.argv[1:])

conn = psycopg2.connect("host={} port={} sslmode=disable".format(args.pg_host, args.pg_port))
queries = []
cur = conn.cursor()

def fit_fn_2(xs, m, b):
    fs = []
    for x in xs:
        f = max(0.1, m*x + b)
        fs.append(f)
    return fs

def fit_fn(xs, m, b):
    return m*xs + b

def get_set_cost_fit():
    cur.execute(sql.SQL('''SELECT size, cost FROM {};''').format(
        sql.Identifier(args.pg_read_set_cost_table)))
    sizes, costs = np.sort(np.array([row for i, row in enumerate(cur) if i != 0]), axis=0).T
    costs = costs / (10**6)
    fit = np.polyfit(sizes, costs, 1)
    return sizes, costs, fit

def get_list_cost_fit():
    cur.execute(sql.SQL('''SELECT frequency, cost FROM {};''').format(
        sql.Identifier(args.pg_read_list_cost_table)))
    freqs, costs = np.sort(np.array([row for i, row in enumerate(cur) if i != 0]), axis=0).T
    costs = costs / (10**6)
    fit = np.polyfit(freqs, costs, 1)
    return freqs, costs, fit

def sci_note(x):
    s = "{:.2e}".format(x).lower()
    part, exp = s.split("e")
    part = "{:.2}".format(float(part))
    exp = "{:.2}".format(float(exp))
    return "(" + part + r"\times e^{" + exp + "})"

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(2*2.2, 2))
# Set costs
sizes, costs, (m, b) = get_set_cost_fit()
ax1.plot(sizes, costs, "o")
ax1.plot(sizes, fit_fn(sizes, m, b), label="$max(1, {}x {:.2f})$".format(sci_note(m), b))
ax1.set_xlabel("Set Size")
ax1.set_ylabel("Read Cost (ms)")
ax1.grid(True)
#ax1.legend()
# List costs
freqs, costs, (m, b) = get_list_cost_fit()
ax2.plot(freqs, costs, "o")
ax2.plot(freqs, fit_fn(freqs, m, b), label="$max(1, {}x + {:.2f})$".format(sci_note(m), b))
ax2.set_xlabel("List Length")
#ax2.set_ylabel("Read Cost (ms)")
ax2.grid(True)
#ax2.legend()

#plt.subplots_adjust(wspace=0.35)
plt.savefig(os.path.join(args.output_dir, "read_costs.pdf"),
        bbox_inches='tight', pad_inches=0)
plt.close()
