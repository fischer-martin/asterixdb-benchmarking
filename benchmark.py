#!/usr/bin/env python3

import requests
import urllib.parse
import json
import re

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


# the DB turns a nano time long into a string and we parse the string back into nano time.
# nice
def parse_time_string(time_string):
    expected_fract_dig = get_fractional_digits_for_unit(time_string)
    time_string_without_unit = re.sub(r"s|ms|µs|us|ns", "", time_string)
    fract_dig_diff = expected_fract_dig - (len(time_string_without_unit) - (time_string_without_unit.index('.') + 1))
    
    return int(re.sub(r"\.", "", time_string_without_unit) + ("0" * fract_dig_diff))


url = "http://localhost:19002/query/service"
#dataverse = "Tweets"
dataverse = "Restaurants"
#path = "localhost:///media/Shared/Martin/Documents/Uni/Master_Informatik_Salzburg/22SS/Masterarbeit/Unterlagen/Source_Code/CartilageFramework/twm2.adm"
path = "localhost:///media/Shared/Martin/Documents/Uni/Master_Informatik_Salzburg/22SS/Masterarbeit/asterixdb/asterixdb/asterix-app/data/restaurants/restaurants.adm"
shared_parameters = {
        "dataverse": dataverse
}

#prepare_query = """
#    DROP DATAVERSE {dataverse} IF EXISTS;
#    CREATE DATAVERSE {dataverse};
#    USE {dataverse};
#    
#    CREATE TYPE TweetType AS {{
#        tweetid: string
#    }};
#    	
#    CREATE DATASET {dataverse}(TweetType) PRIMARY KEY tweetid;
#    
#    LOAD DATASET {dataverse} USING localfs((`path`=`{path}`),(`format`=`adm`));""".format(dataverse = dataverse, path = path)
#benchmark_query = """
#    SELECT t1.tweetid AS d1, t2.tweetid AS d2, jedi(t1, t2) AS dist
#        FROM {} t1, {} t2
#        WHERE jedi(t1, t2) <= 8 --AND t1.tweetid != t2.tweetid
#        ORDER BY d1, d2;""".format(dataverse, dataverse)
#cleanup_query = "DROP DATAVERSE {};".format(dataverse)

prepare_query = """
    DROP DATAVERSE {dataverse} IF EXISTS;
    CREATE DATAVERSE {dataverse};
    USE {dataverse};
    	
    CREATE TYPE RestaurantType AS {{
        restr_id: bigint
    }};
    
    CREATE DATASET Rests(RestaurantType) PRIMARY KEY restr_id;
    
    LOAD DATASET Rests USING localfs((`path`=`{path}`),(`format`=`adm`));""".format(dataverse = dataverse, path = path)
benchmark_query = """
    SELECT r1.restr_id AS d1, r2.restr_id AS d2, jedi(r1, r2) AS dist
        FROM Rests r1, Rests r2
        WHERE jedi(r1, r2) <= 8 AND r1.restr_id != r2.restr_id
        ORDER BY d1, d2;"""
cleanup_query = "DROP DATAVERSE {dataverse};".format(dataverse = dataverse)

requests.post(url, dict(**shared_parameters, **{"statement": prepare_query}))
response = requests.post(url, dict(**shared_parameters, **{"statement": benchmark_query}))
requests.post(url, dict(**shared_parameters, **{"statement": cleanup_query}))

data = response.json()
print(json.dumps(data, indent = 4, ensure_ascii = False))
print(parse_time_string(data["metrics"]["executionTime"]))
