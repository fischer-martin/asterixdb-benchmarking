#!/usr/bin/env python3

import requests
import urllib.parse
import json
import re
import os



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
    return parse_time_string(json_data["metrics"]["executionTime"])

def query_was_successful(json_data):
    return json_data["status"] == "success"

def run_query(query, url = "http://localhost:19002/query/service", parameters = {}):
    return requests.post(url, dict(**{"statement": query}, **parameters))

def read_file_content(filename, is_json = False):
    with open(filename, "r") as file:
        if is_json:
            file_content = json.load(file)
        else:
            file_content = file.read()

    return file_content

def benchmark_dataset(dataset, config, url = "http://localhost:19002/query/service"):
    results = {}

    prepare_query = read_file_content("data/statements/" + dataset + "/1.prepare.sqlpp").format(host = "localhost", path = os.path.abspath("data/datasets/" + dataset + "/" + dataset + ".json"))
    # TODO: check if query was successfully executed
    run_query(prepare_query, url)

    benchmark_query_unformatted = read_file_content("data/statements/" + dataset + "/2.query.sqlpp")
    for threshold in config["thresholds"]:
        benchmark_query_formatted = benchmark_query_unformatted.format(threshold = threshold)
        # TODO: check if query was successfully executed
        res = run_query(benchmark_query_formatted, url)
        results = dict(**results, **{str(threshold): retrieve_execution_time_from_json(res.json())})
        print(json.dumps(res.json(), indent=4, ensure_ascii=False))

    # TODO: check if query was successfully executed
    cleanup_query = read_file_content("data/statements/" + dataset + "/3.cleanup.sqlpp")
    run_query(cleanup_query, url)

    return results

config = read_file_content("config.json", True)

for ds in config["datasets"]:
    if ds["enabled"]:
        print(benchmark_dataset(ds["dataset"], ds["config"]))
