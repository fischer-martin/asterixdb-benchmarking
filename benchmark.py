#!/usr/bin/env python3

import requests
import json
import re
import os
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

def run_query(query, url = "http://localhost:19004/query/service", parameters = {}, http_connection_timeout_sec = 9.2):
    # timeout for the HTTP requests (in seconds); first value is for connecting, second value is for response
    # (see https://requests.readthedocs.io/en/latest/user/advanced/#timeouts)
    timeout = (http_connection_timeout_sec, None)

    return requests.post(url, dict(**{"statement": query}, **parameters), timeout = timeout)

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

class QueryType(enum.Enum):
    PREPARATION = "preparation"
    BENCHMARK = "benchmark"
    CLEANUP = "cleanup"

def retrieve_timeout_string(default_timeouts, config, query_type):
    DEFAULT_QUERY_TIMEOUT = default_timeouts[query_type.value]

    # check if a run-specific timeout has been specified
    if "query_timeouts" in config.keys():
        if query_type.value in config["query_timeouts"].keys():
            return config["query_timeouts"][query_type.value]

    return DEFAULT_QUERY_TIMEOUT

class PreparationException(Exception):
    pass

def benchmark_run(run, dataset, config, timeouts, url = "http://localhost:19004/query/service", http_connection_timeout_sec = 9.2):
    def print_file_not_found(query_type, file):
        print("[{timestamp}] could not find {query_type} query file '{file}'".format(timestamp = get_current_time_iso(), query_type = query_type.value,file = file))

    def get_query(query_type):
        filename = "data/statements/" + run + "/" + str([q for q in QueryType].index(query_type) + 1) + "." + query_type.value + ".sqlpp"
        try:
            file_content = read_file_content(filename)
            return file_content
        except FileNotFoundError:
            print_file_not_found(query_type, filename)
            raise FileNotFoundError

    def print_query_run(query_type, threshold = None):
        output = "[{timestamp}] running {query_type} query of run {run}".format(timestamp = get_current_time_iso(), query_type = query_type.value, run = run)
        if (query_type == QueryType.BENCHMARK):
            output = output + " with threshold " + str(threshold)
        output = output + "... "
        print(output, end = "", flush = True)

    def print_success(json_data):
        print("done [{time}]".format(time = retrieve_execution_time_from_json(json_data)))

    def print_failure(json_data):
        print("failed (status: {query_status}) [{time}]".format(query_status = retrieve_query_status(json_data), time = retrieve_execution_time_from_json(json_data)))
        if "errors" in json_data.keys():
            errors = json_data["errors"]
            for err in errors:
                print("    error code:    {error_code}".format(error_code = err["code"]))
                print("    error message: {error_msg}".format(error_msg = err["msg"]))

    def print_connection_timeout():
        print("could not connect to server within {conn_timeout}s".format(conn_timeout = http_connection_timeout_sec))

    def run_preparation_query(timeout):
        try:
            preparation_query = get_query(QueryType.PREPARATION).format(host = "localhost", path = os.path.abspath("data/datasets/" + dataset + "/" + dataset + ".json"))
        except FileNotFoundError:
            raise PreparationException

        print_query_run(QueryType.PREPARATION)
        try:
            res = run_query(preparation_query, url, {"timeout": timeout}, http_connection_timeout_sec)
            res_json = res.json()
            if query_was_successful(res_json):
                print_success(res_json)
            else:
                print_failure(res_json)
                raise PreparationException()
        except requests.ConnectTimeout:
            print_connection_timeout()
            raise PreparationException()

    def run_benchmark_query(unformatted_query, threshold, timeout):
        nonlocal results

        benchmark_query_formatted = unformatted_query.format(threshold = threshold)
        print_query_run(QueryType.BENCHMARK, threshold)
        try:
            res = run_query(benchmark_query_formatted, url, {"timeout": timeout}, http_connection_timeout_sec)
            res_json = res.json()
            if query_was_successful(res_json):
                print_success(res_json)
                results = dict(**results, **{str(threshold): retrieve_execution_time_from_json_in_ns(res_json)})
            else:
                print_failure(res_json)
        except requests.ConnectTimeout:
            print_connection_timeout()

    def run_cleanup_query(timeout):
        try:
            cleanup_query = get_query(QueryType.CLEANUP)
        except FileNotFoundError:
            # don't need to handle this since it's not really critical
            return

        print_query_run(QueryType.CLEANUP)
        try:
            res = run_query(cleanup_query, url, {"timeout": timeout}, http_connection_timeout_sec)
            res_json = res.json()
            if query_was_successful(res_json):
                print_success(res_json)
            else:
                # don't need to handle this since it's not really critical
                print_failure(res_json)
        except requests.ConnectTimeout:
            print_connection_timeout()

    results = {}

    run_preparation_query(timeouts[QueryType.PREPARATION.value])

    try:
        benchmark_query_unformatted = get_query(QueryType.BENCHMARK)
        for threshold in config["thresholds"]:
            run_benchmark_query(benchmark_query_unformatted, threshold, timeouts[QueryType.BENCHMARK.value])
    except FileNotFoundError:
        pass

    run_cleanup_query(timeouts[QueryType.CLEANUP.value])

    return results

config = read_file_content("config.json", True)
url = config["url"]
http_connection_timeout_sec = config["http_connection_timeout_sec"]
default_timeouts = config["query_timeouts"]

for run_name, run_v in config["runs"].items():
    if run_v["enabled"]:
        dataset_name = run_v["dataset"]
        run_config = run_v["config"]
        run_timeouts = {k.value: retrieve_timeout_string(default_timeouts, run_config, k) for k in QueryType}

        try:
            results = benchmark_run(run_name, dataset_name, run_config, run_timeouts, url, http_connection_timeout_sec)
        except PreparationException:
            print("[{timestamp}] could not run preparation query. aborting run {run}.".format(timestamp = get_current_time_iso(), run = run_name))
            continue

        if any(results):
            results_filename = "data/runtimes/" + run_name + "/" + get_current_time_iso(False) + ".txt"
            os.makedirs(os.path.dirname(results_filename), exist_ok = True)
            with open(results_filename, "w") as results_file:
                for key, value in results.items():
                    results_file.write(str(key) + " " + str(value) + "\n")
