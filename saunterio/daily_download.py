#!/usr/bin/env python3
# coding: utf-8

"""
Download current forecast.
Run nightly.
"""

import platform

import etl

assert (platform.python_version_tuple()[0:2] == ('3', '3'))

# Get a single weather report from Forecast.io and store it in MongoDB.
etl.download_forecast()
