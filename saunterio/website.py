#!/usr/bin/env python3
# coding: utf-8

import os
import platform
import time

import arrow as arrow_dt
from bottle import Bottle, run, template, static_file
from pymongo import MongoClient, DESCENDING

assert (platform.python_version_tuple()[0:2] == ('3', '3'))

app = Bottle()

if 'MONGOLAB_URI' in os.environ:  # prod
    client = MongoClient(os.environ.get('MONGOLAB_URI'))
else:  # dev
    client = MongoClient(os.environ.get('MONGODB_URI'))

db = client.get_default_database()


@app.route('/static/<filename>')
def send_static(filename):
    return static_file(filename, root='static')


@app.route('/about')
def about():
    return static_file('about.html', root='static')


@app.route('/')
def index():
    start = time.clock()  # Timing execution, from http://stackoverflow.com/a/7370824

    scoring = db.scorings.find_one(sort=[("report_datetime_native", DESCENDING)])

    # Todo: Handle scoring missing/failed
    # Todo: Distinguish bad data v. bad weather

    last_beaten = db.scorings.find_one(filter={"beat_threshold": True}, sort=[("report_datetime_native", DESCENDING)])

    scored_date = arrow_dt.get(scoring['scored_date_iso'])

    args = {'scored_date_friendly': scoring['scored_date_friendly'],
            'eligible': scoring['eligible'],
            'past_tense': scored_date.replace(hour=9, tzinfo="America/New_York") < arrow_dt.utcnow(),
            "days_since": scored_date.toordinal() - arrow_dt.get(last_beaten['scored_date_iso']).toordinal()}

    if scoring['eligible']:
        # Forecast.io API guarantees hourly predictions will be at zero min past the hour, so don't report minutes
        periods = ", and again from ".join(["{} to {}".format(
            arrow_dt.utcnow().replace(hour=qr['start'] // 60, minute=qr['start'] % 60).format("ha"),
            arrow_dt.utcnow().replace(hour=qr['end'] // 60, minute=qr['end'] % 60).format("ha"))
                                            for qr in scoring['qualifying_runs']])
        # Todo: Test multiple periods

        args.update({
            'score': 100.0 - scoring['qualifying_score'],
            'threshold': 100.0 - scoring['historical_threshold'],
            'periods': periods,
            'beat_bool': scoring['beat_threshold']})

    end = time.clock()

    args['data_prep_time'] = "{:.1g}&thinsp;s.".format(end - start)

    return template('weather', args)


# run(app, host='0.0.0.0', port=int(os.environ.get("PORT")), reloader=True)  # for debugging
run(app, host='0.0.0.0', port=int(os.environ.get("PORT")))
