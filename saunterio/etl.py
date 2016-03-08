#!/usr/bin/env python3
# coding: utf-8

"""
Download and transform forecasts.
"""

import os
import platform

import arrow
import requests
from pymongo import MongoClient

assert (platform.python_version_tuple()[0:2] == ('3', '3'))


def download_forecast(lat=35.8775, long=-78.7875, time=None):
    """
    Download a forecast from Forecast.io, and save to MongoDB.
    Defaults to Raleigh WBAN 13722 (RDU) for location.
    :param lat: Latitude, in decimal degrees.
    :param long: Longitude, in decimal degrees.
    :param time: Time, in a format compatible with `arrow.get()`
    """

    # On Heroku, MongoLab sets MONGOLAB_URI. In dev, the var is
    # MONGODB_URI, hence this conditional.
    if 'MONGOLAB_URI' in os.environ:  # prod
        client = MongoClient(os.environ.get('MONGOLAB_URI'))
    else:  # dev
        client = MongoClient(os.environ.get('MONGODB_URI'))

    db = client.get_default_database()

    forecast_io_api_key = os.environ.get('FORECAST_IO_API_KEY')

    if time is None:
        fio_forecast = requests.get("https://api.forecast.io/forecast/{}/{},{}".format(forecast_io_api_key, lat, long))

        # HTTP header content type comes back application/json, so no need to convert result
        result = db.forecasts.insert_one(fio_forecast.json())
    else:
        # Back-fill data catalog
        at = arrow.get(time)
        fio_forecast_today = requests.get(
            "https://api.forecast.io/forecast/{}/{},{},{}".format(forecast_io_api_key, lat, long, at.timestamp))
        fio_forecast_tomorrow = requests.get(
            "https://api.forecast.io/forecast/{}/{},{},{}".format(forecast_io_api_key, lat, long,
                                                                  at.replace(days=+1).timestamp))

        jsn = fio_forecast_tomorrow.json()
        jsn['currently'] = fio_forecast_today.json()['currently']
        jsn['daily']['data'].insert(0, fio_forecast_today.json()['daily']['data'][0])

        result = db.forecasts.insert_one(jsn)

    print("Inserted %s" % result.inserted_id)


def forecast_io_to_qclcd(mongo_record, next_day_only=True):
    """
    Convert a Forecast.io json forecast to a flat QCLCD forecast.
    :param mongo_record: A dict (generally from iterating through a PyMongo Cursor instance) with one MongoDB
    record containing a Forecast.io forecast.
    :param next_day_only: Only return results for 'tomorrow,' local to the forecast.
    """

    '''
    Mapping Forecast.io fields to QCLCD fields

    Sources of comparison:
    QCLCD: http://www.ncdc.noaa.gov/qclcd/qclcddocumentation.pdf
    Forecast.io: https://developer.forecast.io/docs/v2

    Temporal:
    ObsTimeISO8601: hourly.data.[0-48].time (in UNIX time, which is GMT)
    SunriseISO8601: daily.data.1.sunriseTime (in UNIX time, which is GMT)
    SunsetISO8601: daily.data.1.sunsetTime (in UNIX time, which is GMT)
        Sunrise and sunset:
        Index 1 means 'next day' after the current request. Presumably this is calculated on local time,
        and reported in GMT. Should confirm empirically. Also, there are parts of the world where the sun
        does not rise or set in a given day during parts of the year. Those cases need to be handled to match QCLCD.
        Currently they are excluded (simply an edge case not yet worth the time to handle correctly).
        Value in that case for Sun<rise,set>ISO8601 should be 'NoDayNight'.

    Meteorological:
    DryBulbFarenheit: temperature (units are degrees Fahrenheit)
    DewPointFarenheit: dewPoint (units are degrees Fahrenheit)
    WindSpeed (units are MPH): windSpeed (units are MPH)

    HourlyPrecip (units are inches per hour): precipIntensity (units are inches per hour)
        precipIntensity is "the average expected intensity of precipitation occurring at the given time conditional on
                            probability"
        HourlyPrecip is "Precipitation totals (inches and hundredths)
                         Hourly totals if column 20 is 'AA' (hourly METAR report)."
        This means both have significant ambiguity, but since the goal is only a binary assessment of whether
        there's any precipitation at all, that's okay.

        Tue Jan 26 10:38:34 EST 2016:
        Setting the binary precipitation cutoff at precipIntensity >= 0.07, which equates to something a little
        stronger than 'very light rain' according to Forecast.io. This is based on a very rough analysis of
        a few recorded forecasts, and correlates to a low precipProbability (quite roughly, 10% or under).
        This needs to be re-evaluated once more forecasts are collected and a better quantitative analysis
        can be done.

        MongoLab queries:
        Query:  {'hourly.data.precipIntensity':{$gt:0.07}}
        Fields: {'hourly.data.precipIntensity':1, 'hourly.data.precipProbability':1}
        Expand ∞ in bottom right

    SkyCondition:
        Forecast.io provides `cloudCover`, which is a decimal percentage of cloud cover.
        QCLCD SkyCondition values are:
            CLR: CLEAR BELOW 12,000 FT
            FEW: > 0/8 - 2/8 SKY COVER
            SCT SCATTERED:  3/8 - 4/8 SKY COVER
            BKN BROKEN: 5/8 - 7/8 SKY COVER
            OVC OVERCAST: 8/8 SKY COVER
        Simple switch/case should suffice

    It's more computationally efficient, but a little less modular, to do the next_day_only check in this function.
    Placing it outside would mean doing all the timestamp resolution twice.
    '''

    report_time = arrow.get(mongo_record["currently"]["time"]).to(mongo_record["timezone"])
    tomorrow = report_time.replace(days=+1).date()

    hourly_forecasts = []

    for hourly_forecast in mongo_record["hourly"]["data"]:
        observation_time = arrow.get(hourly_forecast["time"]).to(mongo_record["timezone"])

        if next_day_only and observation_time.date() != tomorrow:
            continue

        # Alternate construct to long if chain that follows
        skip_obs = False
        for meteor in ["cloudCover", "temperature", "dewPoint", "windSpeed", "precipIntensity"]:
            if meteor not in hourly_forecast:
                # Should probably log missing weather fields
                skip_obs = True

        if skip_obs:
            continue

        # SkyCondition "switch/case"
        # Downstream algorithm makes a binary decision at 5/8 cloud cover, but might as well preserve precision
        if hourly_forecast["cloudCover"] == 0:
            sky_condition = "CLR"
        elif hourly_forecast["cloudCover"] <= 2 / 8:  # This is a float in Python 3 (an int in Python 2)
            sky_condition = "FEW"
        elif hourly_forecast["cloudCover"] <= 5 / 8:
            sky_condition = "SCT"
        elif hourly_forecast["cloudCover"] <= 7 / 8:
            sky_condition = "BKN"
        elif hourly_forecast["cloudCover"] <= 1:
            sky_condition = "OVC"
        else:
            sky_condition = ""

        forecast = {
            'ObsTimeISO8601': observation_time.isoformat(),
            'SunriseISO8601': arrow.get(mongo_record["daily"]["data"][1]["sunriseTime"]).to(mongo_record["timezone"])
                                                                                        .isoformat(),
            'SunsetISO8601': arrow.get(mongo_record["daily"]["data"][1]["sunsetTime"]).to(mongo_record["timezone"])
                                                                                      .isoformat(),
            'DryBulbFarenheit': hourly_forecast["temperature"],
            'DewPointFarenheit': hourly_forecast["dewPoint"],
            'WindSpeed': hourly_forecast["windSpeed"],
            'HourlyPrecip': hourly_forecast["precipIntensity"],
            'SkyCondition': sky_condition
        }
        hourly_forecasts.append(forecast)

    # hourly_forecasts makes for valid input to thresholds.process_day()
    return hourly_forecasts
