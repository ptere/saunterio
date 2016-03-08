# Saunter.io daily weather check

[Saunter.io](https://saunter.io) gives you a heads up the night before a day of nice weather.

Notification signups are not currently available, but the website is live.

## What's in here

This is the code powering saunter.io, which includes:

* Serve the web site
* Download weather data for the next day and assign it a score (via scheduled job)
* Send email alerts when the next day's score beats a pre-defined threshold

## What's not in here

The work to develop the scoring algorithm was done outside this repo, mostly in a mixture of Jupyter notebooks (Python) and Mathematica. To determine if a particular day is "nice weather," its score is compared to a pre-calculated numerical threshold. The work to calculate those thresholds (listed in `data/raleigh_thresholds.csv`) was mostly done outside of this repo. The scoring algorithm is in `saunterio/thresholds.py`, but the application of it to bulk historical data was done offline with [mrjob](https://pythonhosted.org/mrjob/), and smoothing and other tweaks were done in Mathematica.
