#!/usr/bin/env python3

###
# Notifications center
# Delivery API
###

from flask import Flask


app = Flask(__name__)
endpoint = "/delivery"


@app.route(endpoint + "/status")
def status():
    '''Determine if the application returns a 200 on GET request.'''
    return "ALIVE"


if __name__ == "__main__":
    app.run(host="0.0.0.0")
