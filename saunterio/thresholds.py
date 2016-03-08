#!/usr/bin/env python3
# coding: utf-8

"""
Threshold setting from QCLCD
"""

import collections
import copy as cpy  # 'copy' conflicts with numpy.copy()
import datetime
import math
import platform
from functools import reduce
from itertools import groupby
from itertools import tee

import arrow as arrow_dt  # 'arrow' conflicts with matplotlib.pyplot.arrow()

assert (platform.python_version_tuple()[0:2] == ('3', '3'))


def pairwise(iterable):
    """s -> (s0, s1), (s1, s2), (s2, s3), ....
    :param iterable: s
    """

    # From https://docs.python.org/3/library/itertools.html#itertools-recipes
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def process_day(day):
    """
    Find the best score maintained for at least three hours, given a day of QCLCD-format observations.
    Day breaks are at midnight local.
    :param day: A list of all observations (as a list of dictionaries) from one WBAN on one day.
    :return: Either a tuple or scalar with the best score maintained for three hours (currently in flux).
             Returns the string 'Ineligible-NoScoredThreeHours' if no block of three hours was score-able.
    """

    # Probably add an assert against `not None` wherever this returns. Confirm mrjob doesn't break.

    if len(day) >= 21:
        if day[0]['SunriseISO8601'] == 'NoDayNight':
            return 'Ineligible-NoDayNight'

        # Check that a day has a relatively full set of data. Each day is allowed
        # to miss data from up to two daylight hours.
        fullness_check = [0] * 24
        try:
            for i in range(arrow_dt.get(day[0]['SunriseISO8601']).time().hour,
                           min(arrow_dt.get(day[0]['SunsetISO8601']).time().hour + 1 + 1, 24)):
                # +1 for inclusive, +1 for 1hr after sunset, except max 24 in case sun sets during 11pm
                fullness_check[i] = 1
            for obs in day:
                fullness_check[arrow_dt.get(obs['ObsTimeISO8601']).time().hour] = 0
        except:
            print("Error checking data density")
            print(day)
            print("Sunrise: %d" % arrow_dt.get(day[0]['SunriseISO8601']).time().hour)
            print("Sunset: %d" % (arrow_dt.get(day[0]['SunsetISO8601']).time().hour + 1 + 1))
            # +1 for inclusive, +1 for 1hr after sunset
            print("Fullness check: %d" % sum(fullness_check))
            print(fullness_check)
            raise

        if sum(fullness_check) >= 2:  # If two or more daylight hours have no data
            return "Ineligible-InsufficientDaylightObs"

        scores = list(map(_score_obs, day))

        # day_score = _best_three_hour_window_score(list(zip([obs['ObsTimeISO8601'] for obs in day], scores)))
        day_score = _best_three_plus_hour_window(list(zip([obs['ObsTimeISO8601'] for obs in day], scores)))
        if day_score is None:
            day_score = 'Ineligible-NoScoredThreeHours'  # Replacing None with a string is not the cleanest

        return (day_score, scores)

    return 'Ineligible-InsufficientHourlyObs'


def _score_obs(obs):
    """
    Score a single weather observation.

    :param obs: Dictionary with weather properties (names come from QCLCD headers):
    DryBulbFarenheit,DewPointFarenheit,RelativeHumidity,HourlyPrecip,Latitude,Longitude,
    WetBulbFarenheit,WindSpeed,SkyCondition,Visibility,WeatherType,ValueForWindCharacter,
    StationPressure,PressureTendency,PressureChange,RecordType,Altimeter,StationType
    :return: Scalar floating point score.
    """

    # Constants & coefficients are from "Weather parameter curve development.nb"

    dt = arrow_dt.get(obs['ObsTimeISO8601'])
    dbf = float(obs['DryBulbFarenheit'])
    dpf = float(obs['DewPointFarenheit'])
    hp = float(obs['HourlyPrecip']) if obs['HourlyPrecip'] != '' else 0.0
    ws = float(obs['WindSpeed']) if obs['WindSpeed'] != '' else 0.0
    sc = obs['SkyCondition']
    sr = arrow_dt.get(obs['SunriseISO8601'])
    ss = arrow_dt.get(obs['SunsetISO8601'])

    if hp > 0:  # No precipitation allowed
        return "Ineligible-Precipitation"

    if 'BRK' in sc or 'OVC' in sc:  # No cloudy hours (5/8+ cloud cover)
        return "Ineligible-Cloudy"

    if ws > 15:
        return "Ineligible-WindSpeed"

    if dt < sr:
        return "Ineligible-SunNotRisen"  # Sun not up

    if dt > ss + datetime.timedelta(hours=1):
        return "Ineligible-SunHasSet"  # Sun set over an hour ago

    # Temperature
    # if statement prevents trying to calculate log(0), which is undefined
    score = 12 * math.log(abs(dbf - 73) * 0.2 + 1.2) - 2.18 if abs(dbf - 73) != 0 else 0.0

    # Dew point
    if dpf > 50:
        score += 5 * ((0.8 * (dpf - 50)) ** 0.5)
    if dpf < 37:
        score += 5 * ((0.8 * (37 - dpf)) ** 0.5)

    # Wind speed
    if ws < 5:
        pass
    elif ws < 10:
        score += 2
    elif ws < 15:
        score = score + 0.35 * ((ws - 10) ** 2) + 2
    else:
        return "Ineligible-WindSpeed"

    return round(score, 1)


# Deprecated in favor of _best_three_plus_hour_window
def _best_three_hour_window_score(the_day):
    """
    Find the best score maintained for at least three hours, with no more than a one-hour gap
    between observations in that window.

    :param the_day: a list of tuples of (timestamp in ISO8601, score or Ineligible)
    :return: The best score maintained for at least three hours, as a scalar.
    """

    # Convert (ISO8601, score) to (minutes since midnight, score)
    the_day_mmmm = [((arrow_dt.get(k).hour * 60) + arrow_dt.get(k).minute, v) for (k, v) in the_day]
    # Any value that's not a float is presumed to be a string containing "Ineligible"
    # (Should make this check more explicit and introduce error handling)
    eligible = [((arrow_dt.get(k).hour * 60) + arrow_dt.get(k).minute, v) for (k, v) in the_day if isinstance(v, float)]

    eligible_by_score = cpy.copy(eligible)  # There are faster ways to resort but they are less readable
    # Python 2: eligible_by_score.sort(cmp=lambda x, y: cmp(x[1], y[1]))
    eligible_by_score.sort(key=lambda x: x[1])  # sort by score
    eligible_by_time = cpy.copy(eligible)

    # the_day sort is same as input in notebook runs, but mrjob mapper alters order. Need to sort by time.
    eligible_by_time.sort(key=lambda x: x[0])
    the_day_mmmm.sort(key=lambda x: x[0])

    # General approach:
    # Test each score, from best to worst, to see if it could be the worst score in a qualifying run.
    # Start at 4th best score, as that's the first that could possibly be sustained for three hours.
    # Once the first qualifying run is found, return it as the best qualifying run.
    # (No later run could be better, because scores are tested from best to worst.)
    for test_pos in range(3, len(eligible_by_score)):
        center_score = eligible_by_score[test_pos]

        run_since = None
        case_closed = False

        for (i, score) in enumerate(the_day_mmmm):
            # score[0] is minutes since midnight of this score
            # score[1] is the score
            # test_pos is the position in the list of ordered scores (regardless of time)
            # that this algorithm is checking to see if it's the worst score of a qualifying run
            # of scores
            # center_score is the score at that position (test_pos's position)
            if score[0] < center_score[0] - 180:
                continue

            if isinstance(score[1], str):  # Presumed to contain 'Ineligible'
                run_since = None
                continue

            if score[1] > center_score[1]:
                run_since = None
                continue

            if score[0] > center_score[0] + 180:
                # print("No three hour window found for test_pos %d (time span boundary reached)" % test_pos)
                case_closed = True
                break

            # This score could be the start of a run
            if run_since is None:
                run_since = score[0]
            elif score[0] - the_day_mmmm[i - 1][0] > 60:  # More than one hour gap disqualifies run
                run_since = score[0]
            elif score[0] - run_since >= 180:
                print("Found a three hour window for test_pos %d. Center: %d, %f. "
                      "Beginning no later than %d, and running at least through %d." %
                      (test_pos, center_score[0], center_score[1], run_since, score[0]))
                # For debugging output, uncomment `print()`s, then switch `return` for `break` and
                # `case_closed = True`. Note, result will likely be wrong (since no return value).
                return center_score[1]
                # case_closed = True
                # break
                # else if run_since is not None, let the streak build

        if not case_closed:
            # print("No three hour window found for test_pos %d (fell off end of day)" % test_pos)
            pass


def _best_three_plus_hour_window(scored_day):
    """
    Find the best score maintained for at least three hours, with no more than a one-hour gap
    between observations in that window.
    """

    # Trying to avoid using the naked word 'score' in this code, as it's ambiguous whether that's a scalar or tuple
    def minutes_past_midnight(iso_dt):
        adt = arrow_dt.get(iso_dt)
        return (adt.hour * 60) + adt.minute

    QualifyingRun = collections.namedtuple('QualifyingRun', ['start', 'end', 'worst_score'])
    # namedtuple is a class factory, hence assigning to a class name (see http://stackoverflow.com/q/33259220)

    # Convert (ISO8601, score) to (minutes since midnight, score)
    TimeScore = collections.namedtuple('TimeScore', ['minute', 'value'])

    scores_with_ineligible_by_time = [TimeScore(minute=minutes_past_midnight(t), value=s) for (t, s) in scored_day]
    # Sort by time to ensure chronological order (unnecessary in IPython notebook, but necessary for mrjob)
    scores_with_ineligible_by_time.sort(key=lambda time_score: time_score.minute)

    # Any value that's not a float is presumed to be a string containing "Ineligible"
    # (Should make this check more explicit and introduce error handling)
    scores_without_ineligible = [TimeScore(minute=minutes_past_midnight(t), value=s) for (t, s) in scored_day
                                 if isinstance(s, float)]

    scores_without_ineligible_by_time = cpy.copy(scores_without_ineligible)
    scores_without_ineligible_by_time.sort(key=lambda time_score: time_score.minute)

    scores_without_ineligible_by_score = cpy.copy(scores_without_ineligible)
    scores_without_ineligible_by_score.sort(key=lambda time_score: time_score.value)

    del scores_without_ineligible  # Avoid accidentally referring to this unsorted list

    # Caution about debugging generators: There's an observer effect, where if you print out the contents,
    # you move the internal pointer to the end, and code that uses that generator then fails.

    # General approach
    # 1. Break up the day into chronological segments of consecutive numeric scores
    # 2. For each numeric score, from best to worst (regardless of time)
    #   a. Break each segment from (1) into sub-segments of consecutive scores that are better and worse than (2)
    #   b. For each segment from (2a) that is better than the candidate
    #       1. Calculate its duration and the maximum time between data points
    #       2. If those satisfy criteria decided elsewhere (hardcoded here), count it as a qualifying run
    #   c. If there were any qualifying runs from (2b2), return all qualifying runs
    #   d. Otherwise, return to (2) and continue with the next worst score

    # Ineligible scores break up runs of numeric scores
    # Must store as list instead of iterator for repeated readings
    numeric_runs = [list(g) for t, g in
                    groupby(scores_with_ineligible_by_time, lambda s: isinstance(s.value, float)) if t]

    for candidate_position in range(3, len(scores_without_ineligible_by_score)):
        candidate = scores_without_ineligible_by_score[candidate_position]

        # Built-in tuple equality checking allows for lookup across lists, e.g.
        # could hypothetically do `scores_with_ineligible_by_time.index(candidate)`.

        qualifying_runs = []

        for numeric_run in numeric_runs:

            for better, better_run in groupby(numeric_run, key=lambda s: s.value <= candidate.value):
                if better:
                    run = list(better_run)
                    duration = run[-1].minute - run[0].minute
                    max_gap = reduce(lambda mg, ts: max(mg, ts[1].minute - ts[0].minute), pairwise(run), 0)
                    if duration >= 180 and max_gap <= 61:
                        # 61 allows an extra minute in case of oddities in rounding, time sync, etc
                        qualifying_runs.append(QualifyingRun(start=run[0].minute,
                                                             end=run[-1].minute,
                                                             worst_score=candidate.value))

        if len(qualifying_runs) >= 1:
            return qualifying_runs
