#!/usr/bin/env python3
# coding: utf-8

import os
import platform

from pymongo import MongoClient, DESCENDING

import etl
import notify_email
import score

assert (platform.python_version_tuple()[0:2] == ('3', '3'))

if 'MONGOLAB_URI' in os.environ:  # prod   # stop this, get it under one env var. find a way, this is dumb.
    client = MongoClient(os.environ.get('MONGOLAB_URI'))
else:  # dev
    client = MongoClient(os.environ.get('MONGODB_URI'))

db = client.get_default_database()

# 1. Download the forecast

etl.download_forecast()

# 2. Score it

# Until the algorithm is known stable, recalculate all scores every night

score.recalculate_all_scores()

# 3. Alert if threshold beat

most_recent_scoring = db.scorings.find_one(sort=[("report_datetime_native", DESCENDING)])

if most_recent_scoring['beat_threshold']:
    notify_email.send_alerts(score=100.0 - most_recent_scoring['qualifying_score'],
                             threshold=100.0 - most_recent_scoring['historical_threshold'])
