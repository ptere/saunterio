#!/usr/bin/env python3
# coding: utf-8

"""
Score all forecasts.
Run nightly.
"""

import calendar
import csv
import datetime  # Hate this, but necessary
import os
import pickle
import platform

import arrow as arrow_dt
from pymongo import MongoClient, DESCENDING

import etl
import thresholds

assert (platform.python_version_tuple()[0:2] == ('3', '3'))

if 'MONGOLAB_URI' in os.environ:  # prod
    client = MongoClient(os.environ.get('MONGOLAB_URI'))
else:  # dev
    client = MongoClient(os.environ.get('MONGODB_URI'))

db = client.get_default_database()

# Retrieve most recently stored record
# mongo_record = db.forecasts.find().sort("currently.time", DESCENDING).limit(1)
# PyMongo's sort() syntax differs substantially from MongoDB's
# See https://api.mongodb.org/python/current/api/pymongo/cursor.html#pymongo.cursor.Cursor.sort


def recalculate_all_scores():
    historical_thresholds = {}

    # raleigh_thresholds.csv comes from Mathematica; generation process is currently manual
    with open('data/raleigh_thresholds.csv') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            historical_thresholds[int(row[0])] = float(row[1])

    # For now, empty collection and recalculate all scorings (since fields are changing frequently)
    db.scorings.remove()

    mongo_records = db.forecasts.find().sort("currently.time", DESCENDING)

    for mongo_record in mongo_records:
        scoring = {}
        report_time = arrow_dt.get(mongo_record["currently"]["time"]).to(mongo_record["timezone"])
        tomorrow = report_time.replace(days=+1).date()
        day_ordinal = int(tomorrow.strftime("%j"))

        # Pretend that leap years don't happen, since we don't have enough historical weather data to account for them
        if calendar.isleap(int(tomorrow.strftime("%Y"))) and day_ordinal >= 60:
            day_ordinal -= 1

        (score, hourly_scores) = thresholds.process_day(etl.forecast_io_to_qclcd(mongo_record))

        scoring['scored_date_iso'] = report_time.replace(days=+1).format("YYYY-MM-DD")
        # Recording the scored date as an ISO string has a use in processing.
        # If we were to only record a timestamp, that means an extra line of code to convert back to local timezone
        # before we can confidently check the date. (Even if we stored at UTC noon, there are UTC+12 and UTC-12 offsets
        # in the real world (UTC+14, even), so sometimes we'd get the wrong date if we didn't account for local offset.)

        scoring['origin_forecast_id'] = mongo_record["_id"]
        scoring['scored_date_friendly'] = report_time.replace(days=+1).format("MMMM D, YYYY")
        scoring['generated_datetime_arrow'] = pickle.dumps(arrow_dt.now().floor('second').to(mongo_record["timezone"]))
        scoring['report_datetime_arrow'] = pickle.dumps(report_time)
        scoring['report_datetime_native'] = datetime.datetime.utcfromtimestamp(mongo_record["currently"]["time"])
        scoring['eligible'] = True if isinstance(score, list) else False
        scoring['historical_threshold'] = historical_thresholds[day_ordinal]
        scoring['hourly_scores_diagnostic'] = hourly_scores
        if scoring['eligible']:
            scoring['qualifying_runs'] = [{'start': s.start, 'end': s.end, 'qualifying_score': s.worst_score}
                                          for s in score]
            scoring['qualifying_score'] = score[0].worst_score  # All qualifying runs will be tied in score
        else:
            scoring['ineligible_reason'] = score
        scoring['beat_threshold'] = True if (scoring['eligible'] and
                                             scoring['qualifying_score'] < scoring['historical_threshold']) else False

        db.scorings.insert_one(scoring)
