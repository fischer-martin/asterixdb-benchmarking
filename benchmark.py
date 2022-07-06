#!/usr/bin/env python3

import requests
import json
import re
import os
import sys
import enum
import datetime



# the DB turns a nano time long into a string and we parse the string back into nano time.
# nice
def parse_time_string(time_string):
    def get_fractional_digits_for_unit(time_string):
        # AsterixDB uses "s", "ms", "µs"/"us", and "ns" to represent the execution time's unit
        unit_fractional_digits = {
                    "ns": 0,
                    "µs": 3,
                    "us": 3,
                    "ms": 6,
                    "s" : 9 # listed at the end since e.g. "ms" also contains "s"
                }
        for key, value in unit_fractional_digits.items():
            if key in time_string:
                return value

    expected_fract_dig = get_fractional_digits_for_unit(time_string)
    time_string_without_unit = re.sub(r"s|ms|µs|us|ns", "", time_string)
    fract_dig_diff = expected_fract_dig - (len(time_string_without_unit) - (time_string_without_unit.index('.') + 1))
    
    return int(re.sub(r"\.", "", time_string_without_unit) + ("0" * fract_dig_diff))

def retrieve_execution_time_from_json(json_data):
    return json_data["metrics"]["executionTime"]

def retrieve_execution_time_from_json_in_ns(json_data):
    return parse_time_string(retrieve_execution_time_from_json(json_data))

def retrieve_query_status(json_data):
    return json_data["status"]

def query_was_successful(json_data):
    return retrieve_query_status(json_data) == "success"

def run_query(query, url = "http://localhost:19002/query/service", parameters = {}, timeout = (9.2, sys.maxsize)):
    # timeout for the HTTP requests (in seconds); first value is for connecting, second value is for response
    # (see https://requests.readthedocs.io/en/latest/user/advanced/#timeouts)
    #return requests.post(url, dict(**{"statement": query}, **parameters), timeout = timeout)
    return requests.post(url, dict(**{"statement": query}, **parameters))

def read_file_content(filename, is_json = False):
    with open(filename, "r") as file:
        if is_json:
            file_content = json.load(file)
        else:
            file_content = file.read()

    return file_content

def get_current_time_iso(separators = True):
    if separators:
        # https://stackoverflow.com/a/28147286
        return datetime.datetime.now().replace(microsecond = 0).isoformat()
    else:
        # ISO8601 time format string: https://stackoverflow.com/a/52187229
        return datetime.datetime.now().strftime("%Y%m%dT%H%M%S.%fZ")

def benchmark_dataset(dataset, config, url = "http://localhost:19002/query/service"):
    class QueryType(enum.Enum):
        PREPARATION = "preparation"
        BENCHMARK = "benchmark"
        CLEANUP = "cleanup"
    # TODO: There is this undocumented "timeout" parameter (see QueryServiceRequestParameters) which takes a value
    # in the format nx (n = number, x = time unit; e.g. 200s) that is always converted into ms (i.e. 1400us is
    # converted into 1ms) and is applied in NCQueryServiceServlet but not in QueryServiceServlet, both of which
    # are used by the NCs (NCApplication) and the CCs (CCApplication) respectively when answering queries.
    def get_timeout_string(config, query_type):
        DEFAULT_QUERY_TIMEOUT = str(60 * 1000) + "ms"

        if "timeouts" in config.keys():
            if query_type.value in config["timeouts"].keys():
                return config["timeouts"][query_type.value]

        return DEFAULT_QUERY_TIMEOUT

    def get_query(query_type):
        filename = "data/statements/" + dataset + "/" + str([q for q in QueryType].index(query_type) + 1) + "." + query_type.value + ".sqlpp"
        return read_file_content(filename)

    def print_query_run(query_type, threshold = None):
        output = "[{timestamp}] running {query_type} query of dataset {dataset}".format(timestamp = get_current_time_iso(), query_type = query_type.value, dataset = dataset)
        if (query_type == QueryType.BENCHMARK):
            output = output + " with threshold " + str(threshold)
        output = output + "... "
        print(output, end = "", flush = True)

    def print_success(json_data):
        print("done [{time}]".format(time = retrieve_execution_time_from_json(json_data)))

    def print_failure(json_data):
        print("failed (status: {query_status}) [{time}]".format(query_status = retrieve_query_status(res_json), time = retrieve_execution_time_from_json(res_json)))
        if "errors" in json_data.keys():
            errors = json_data["errors"]
            for err in errors:
                print("    error code:    {error_code}".format(error_code = err["code"]))
                print("    error message: {error_msg}".format(error_msg = err["msg"]))

    results = {}

    preparation_query = get_query(QueryType.PREPARATION).format(host = "localhost", path = os.path.abspath("data/datasets/" + dataset + "/" + dataset + ".json"))
    print_query_run(QueryType.PREPARATION)
    res = run_query(preparation_query, url)
    res_json = res.json()
    if query_was_successful(res_json):
        print_success(res_json)
    else:
        print_failure(res_json)
        # TODO: "handle" this (exception?)
    #print(json.dumps(res_json, indent = 4, ensure_ascii = False))

    benchmark_query_unformatted = get_query(QueryType.BENCHMARK)
    for threshold in config["thresholds"]:
        benchmark_query_formatted = benchmark_query_unformatted.format(threshold = threshold)
        print_query_run(QueryType.BENCHMARK, threshold)
        res = run_query(benchmark_query_formatted, url)
        res_json = res.json()
        if query_was_successful(res_json):
            print_success(res_json)
            results = dict(**results, **{str(threshold): retrieve_execution_time_from_json_in_ns(res_json)})
        else:
            print_failure(res_json)
        #print(json.dumps(res_json, indent = 4, ensure_ascii = False))

    cleanup_query = get_query(QueryType.CLEANUP)
    print_query_run(QueryType.CLEANUP)
    res = run_query(cleanup_query, url)
    res_json = res.json()
    if query_was_successful(res_json):
        print_success(res_json)
    else:
        # don't need to handle this since it's not really critical
        print_failure(res_json)

    return results

config = read_file_content("config.json", True)

for ds in config["datasets"]:
    if ds["enabled"]:
        dataset_name = ds["dataset"]

        results = benchmark_dataset(dataset_name, ds["config"])

        results_filename = "data/runtimes/" + dataset_name + "/" + get_current_time_iso(False) + ".txt"
        os.makedirs(os.path.dirname(results_filename), exist_ok = True)
        with open(results_filename, "w") as results_file:
            for key, value in results.items():
                results_file.write(str(key) + " " + str(value) + "\n")
