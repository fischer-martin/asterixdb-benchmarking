#!/usr/bin/env python3

import requests
import json
import re
import os
import enum
import datetime
import argparse



def log(msg, newline = True, append = False):
    if append:
        output = str(msg)
    else:
        output = "[{timestamp}] {msg}".format(timestamp = get_current_time_iso(), msg = msg)

    print(output, end = (None if newline else ""), flush = True)

def get_directory():
    return os.path.dirname(os.path.realpath(__file__))

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
    with open(get_directory()  + "/" + filename, "r") as file:
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
    def __init__(self, message):
        self.message = message
        super().__init__(message)

def benchmark_run(run, config, timeouts, connection_config):
    query_url = connection_config["url"] + ":" + str(connection_config["query_api_port"]) + "/query/service"
    http_connection_timeout_sec = connection_config["http_connection_timeout_sec"]

    def log_file_not_found(query_type, file):
        log("could not find {query_type} query file '{file}'".format(query_type = query_type.value, file = file))

    def get_query(query_type):
        filename = "data/statements/" + run + "/" + str([q for q in QueryType].index(query_type) + 1) + "." + query_type.value + ".sqlpp"
        try:
            file_content = read_file_content(filename)
            return file_content
        except FileNotFoundError:
            log_file_not_found(query_type, filename)
            raise FileNotFoundError

    def log_query_run(query_type, threshold = None):
        output = "running {query_type} query of run {run}".format(query_type = query_type.value, run = run)
        if (query_type == QueryType.BENCHMARK):
            output = output + " with threshold " + str(threshold)
        output = output + "... "
        log(output, newline = False)

    def log_success(json_data):
        log("done [{time}]".format(time = retrieve_execution_time_from_json(json_data)), append = True)

    def log_failure(json_data):
        log("failed (status: {query_status}) [{time}]".format(query_status = retrieve_query_status(json_data), time = retrieve_execution_time_from_json(json_data)), newline = False, append = True)
        if "errors" in json_data.keys():
            errors = json_data["errors"]
            for err in errors:
                log("\n    error code:    {error_code}".format(error_code = err["code"]), newline = False, append = True)
                log("\n    error message: {error_msg}".format(error_msg = err["msg"]), newline = False, append = True)
        log("", append = True)

    def log_connection_timeout():
        log("connection request timed out after {conn_timeout}s".format(conn_timeout = http_connection_timeout_sec), append = True)

    def log_connection_error():
        log("could not reach server".format(conn_timeout = http_connection_timeout_sec), append = True)

    def run_preparation_query(timeout):
        try:
            preparation_query = get_query(QueryType.PREPARATION).format(host = "localhost", dataverse = config["dataverse"], path_prefix = get_directory() + "/data/datasets/")
        except FileNotFoundError:
            raise PreparationException("could not find preparation query file")

        log_query_run(QueryType.PREPARATION)
        try:
            res = run_query(preparation_query, query_url, {"timeout": timeout}, http_connection_timeout_sec)
            res_json = res.json()
            if query_was_successful(res_json):
                log_success(res_json)
            else:
                log_failure(res_json)
                raise PreparationException("could not run preparation query")
        except requests.ConnectTimeout:
            log_connection_timeout()
            raise PreparationException("could not run preparation query")
        except requests.ConnectionError:
            log_connection_error()
            raise PreparationException("could not run preparation query")

    def upload_join_library():
        if not ("username" in connection_config.keys() and "password" in connection_config.keys()):
            raise PreparationException("can not upload join library without DB credentials")

        library_url = connection_config["url"] + ":" + str(connection_config["library_api_port"]) + "/" + connection_config["username"] + "/udf/" + config["dataverse"] + "/" + config["join_library"]
        join_library = config["join_library"]
        filename = get_directory() + "/lib/" + join_library
        timeout = (http_connection_timeout_sec, None)
        try:
            with open(filename, "rb") as lib_file:
                # has to have the behaviour of curl -v -u username:password -X POST -F 'data=@filename' -F 'type=java' library_url
                requests.post(library_url, auth = (connection_config["username"], connection_config["password"]), files = {"data": lib_file, "type": "java"}, timeout = timeout)
        except Exception:
            raise PreparationException("could not upload join library")

    def run_benchmark_query(unformatted_query, threshold, timeout):
        nonlocal results

        benchmark_query_formatted = unformatted_query.format(dataverse = config["dataverse"], threshold = threshold)
        log_query_run(QueryType.BENCHMARK, threshold)
        try:
            res = run_query(benchmark_query_formatted, query_url, {"timeout": timeout}, http_connection_timeout_sec)
            res_json = res.json()
            if query_was_successful(res_json):
                log_success(res_json)
                results = dict(**results, **{str(threshold): retrieve_execution_time_from_json_in_ns(res_json)})
            else:
                log_failure(res_json)
        except requests.ConnectTimeout:
            log_connection_timeout()
        except requests.ConnectionError:
            log_connection_error()

    def run_cleanup_query(timeout):
        try:
            cleanup_query = get_query(QueryType.CLEANUP).format(dataverse = config["dataverse"])
        except FileNotFoundError:
            # don't need to handle this since it's not really critical
            return

        log_query_run(QueryType.CLEANUP)
        try:
            res = run_query(cleanup_query, query_url, {"timeout": timeout}, http_connection_timeout_sec)
            res_json = res.json()
            if query_was_successful(res_json):
                log_success(res_json)
            else:
                # don't need to handle this since it's not really critical
                log_failure(res_json)
        except requests.ConnectTimeout:
            log_connection_timeout()
        except requests.ConnectionError:
            log_connection_error()

    results = {}

    run_preparation_query(timeouts[QueryType.PREPARATION.value])

    if "join_library" in config.keys():
        upload_join_library()

    try:
        benchmark_query_unformatted = get_query(QueryType.BENCHMARK)
        for threshold in config["thresholds"]:
            run_benchmark_query(benchmark_query_unformatted, threshold, timeouts[QueryType.BENCHMARK.value])
    except FileNotFoundError:
        pass

    run_cleanup_query(timeouts[QueryType.CLEANUP.value])

    return results


argparser = argparse.ArgumentParser()
argparser.add_argument("-u", "--username", help = "database username")
argparser.add_argument("-p", "--password", help = "database password")
args = argparser.parse_args()
if args.username and args.password:
    credentials = {"username": args.username, "password": args.password}
else:
    credentials = {}

config = read_file_content("config.json", True)
default_timeouts = config["query_timeouts"]

for run_name, run_v in config["runs"].items():
    if run_v["enabled"]:
        run_config = run_v["config"]
        run_timeouts = {k.value: retrieve_timeout_string(default_timeouts, run_config, k) for k in QueryType}
        connection_config = dict(config["connection_config"]) # let's make a copy since we don't want to modify the original data
        connection_config.update(credentials) # credentials as args should have higher priority than the ones in the config

        try:
            results = benchmark_run(run_name, run_config, run_timeouts, connection_config)
        except PreparationException as exc:
            log("{message}. aborting run {run}.".format(message = getattr(exc, "message"), run = run_name))
            continue

        if any(results):
            results_filename = get_directory() + "/data/runtimes/" + run_name + "/" + get_current_time_iso(False) + ".txt"
            os.makedirs(os.path.dirname(results_filename), exist_ok = True)
            with open(results_filename, "w") as results_file:
                for key, value in results.items():
                    results_file.write(str(key) + " " + str(value) + "\n")
