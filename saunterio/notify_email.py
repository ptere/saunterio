#!/usr/bin/env python3
# coding: utf-8

import os
import platform

from pymongo import MongoClient
from sendgrid import Mail, SendGridClient, SendGridClientError, SendGridServerError

# Debug snippet:
import pprint
debug_printer = pprint.PrettyPrinter()

assert (platform.python_version_tuple()[0:2] == ('3', '3'))

if 'MONGOLAB_URI' in os.environ:  # prod
    client = MongoClient(os.environ.get('MONGOLAB_URI'))
else:  # dev
    client = MongoClient(os.environ.get('MONGODB_URI'))

sg = SendGridClient(os.environ.get('SENDGRID_ORANGE_KEY'), raise_errors=True)

db = client.get_default_database()

alert_text = "Tomorrow's weather earned a score of {}, which beats the threshold of {}. Visit https://saunter.io."


def send_alerts(score, threshold):
    sub_list = db.subscribers.find_one({"_id": 13722})

    for email in sub_list['emails']:
        message = Mail()
        message.add_to(email)
        message.set_subject("Nice weather alert")
        message.set_html(alert_text.format(score, threshold))
        message.set_text(alert_text.format(score, threshold))
        message.set_from('Saunter <saunter@saunter.io>')

        try:
            status, msg = sg.send(message)
            debug_printer.pprint(status)
            debug_printer.pprint(msg)
        except SendGridClientError:
            debug_printer.pprint(SendGridClientError)
        except SendGridServerError:
            debug_printer.pprint(SendGridServerError)
