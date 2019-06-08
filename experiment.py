import collections
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from collections import namedtuple

def convert(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            obj[key] = convert(value)
        return namedtuple('GenericDict', obj.keys())(**obj)
    elif isinstance(obj, list):
        return [convert(item) for item in obj]
    else:
        return obj

def read_result(filename):
    df = pd.read_csv(filename, index_col="query_id")
    df = df.loc[df['num_result'] > 0]
    print("Number of queries with non-empty results in {} : {}".format(filename, len(df)))
    return df

def read_multi_results(filenames, read_action=False):
    dfs = []
    names = []
    actions = []
    common_query_ids = None
    for filename in filenames:
        filename, name = filename.split(":")
        df = pd.read_csv(filename)
        df = df.loc[df['num_result'] > 0]
        dfs.append(df)
        names.append(name)
        if read_action:
            actions.append(parse_all_actions(df))
        print("Number of queries with non-empty results in {} : {}".format(name, len(df)))
        if common_query_ids is None:
            common_query_ids = df["query_id"]
        else:
            common_query_ids = np.intersect1d(common_query_ids, df["query_id"], assume_unique=True)
    print("Number of common queries: {}".format(len(common_query_ids)))
    return dfs, names, actions, common_query_ids

def parse_benefit_cost(bc_string):
    def get_next_non_digit_index(i):
        j = i + 1
        while j < len(bc_string) and bc_string[j].isdigit():
            j+=1
        return j
    list_costs = collections.deque([])
    list_benefits = collections.deque([])
    set_costs = collections.deque([])
    set_benefits = collections.deque([])
    i = 0
    while i < len(bc_string):
        j = get_next_non_digit_index(i)
        list_benefit = int(bc_string[i+1:j])
        i = j
        j = get_next_non_digit_index(i)
        list_cost = int(bc_string[i+1:j])
        i = j
        j = get_next_non_digit_index(i)
        set_benefit = int(bc_string[i+1:j])
        i = j
        j = get_next_non_digit_index(i)
        set_cost = int(bc_string[i+1:j])
        i = j
        list_costs.append(list_cost)
        list_benefits.append(list_benefit)
        set_costs.append(set_cost)
        set_benefits.append(set_benefit)
    return convert({"list_benefits" : np.array(list(list_benefits)),
                    "list_costs" : np.array(list(list_costs)),
                    "set_benefits" : np.array(list(set_benefits)),
                    "set_costs" : np.array(list(set_costs))})

def parse_results(result_string):
    if type(result_string) != str or len(result_string) == 0:
        return []
    def get_next_non_digit_index(i):
        j = i + 1
        while j < len(result_string) and result_string[j].isdigit():
            j+=1
        return j
    results = []
    i = 0
    while i < len(result_string):
        j = get_next_non_digit_index(i)
        ID = int(result_string[i+1:j])
        i = j
        j = get_next_non_digit_index(i)
        overlap = int(result_string[i+1:j])
        i = j
        results.append((ID, overlap))
    return results

def parse_actions(actions_string):
    def get_next_non_digit_index(i):
        j = i + 1
        while j < len(actions_string) and actions_string[j].isdigit():
            j+=1
        return j

    actions = collections.deque([])
    sets_read = collections.deque([])
    overlaps = collections.deque([])
    lists_read = collections.deque([])
    i = 0
    while i < len(actions_string):
        curr_action = actions_string[i]
        if curr_action == "l":
            j = get_next_non_digit_index(i)
            freq = int(actions_string[i+1:j])
            if j < len(actions_string) and actions_string[j] == "o":
                i = j
                j = get_next_non_digit_index(i)
                overlap = int(actions_string[i+1:j])
                actions.append(("l", freq, overlap))
            else:
                actions.append(("l", freq, None))
            lists_read.append(freq)
        elif curr_action == "s":
            j = get_next_non_digit_index(i)
            size = int(actions_string[i+1:j])
            i = j
            j = get_next_non_digit_index(i)
            overlap = int(actions_string[i+1:j])
            actions.append(("s", size, overlap))
            sets_read.append(size)
            overlaps.append(overlap)
        i = j
    return convert({"actions" : actions,
        "lists_read" : np.array(list(lists_read)),
        "sets_read" : np.array(list(sets_read)),
        "overlaps" : np.array(list(overlaps))})

def parse_all_actions(df):
    return [parse_actions(s) for s in df["actions"]]

def query_size_interval_axis(max_query_size, num_interval):
    m = int(max_query_size / num_interval)
    intervals = [(10, m)] + [(m*i, m*(i+1)) for i in range(1, num_interval)]
    print("size intervals: {}".format(intervals))
    xs = [(end+start)/2 for start, end in intervals]
    xticks = [intervals[0][0]] + [i[1] for i in intervals]
    return intervals, xs, xticks

def read_queries(conn, pg_query_table, pg_list_table):
    queries = []
    cur = conn.cursor()
    cur.execute(sql.SQL('''SELECT tokens FROM {};''').format(sql.Identifier(pg_query_table)))
    for row in cur:
        queries.append(row[0])
    for i, query in enumerate(queries):
        cur.execute(sql.SQL('''
            SELECT array_agg(a.token), array_agg(a.frequency), array_agg(a.duplicate_group_id)
            FROM (
                SELECT token, frequency, duplicate_group_id
                FROM {}
                WHERE token = ANY(%s) AND frequency > 1
                ORDER BY token ASC
            ) AS a;''').format(sql.Identifier(pg_list_table)), (query,))
        tokens, freqs, gids = cur.fetchone()
        queries[i] = (tokens, freqs, gids)
        print("{} queries read so far".format(i+1))
    return queries

