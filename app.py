#!/usr/bin/env python3

#   -------------------------------------------------------------
#   Notifications center - Delivery API
#   - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   Author:         Sébastien Santoro aka Dereckson
#   Project:        Nasqueron
#   Description:    AMQP to HTTP gateway
#   Created:        2017-05-22
#   Dependencies:   Pika, direct access to the broker
#   -------------------------------------------------------------

"""
This module connects to the message broker, fire a web server,
and allow to interact through the broker from HTTP requests.
"""

from flask import Flask, abort, jsonify, request
import uuid
import pika
import os
import sys


#   -------------------------------------------------------------
#   Configuration
#   - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


app = Flask(__name__)
service = {}
endpoint = "/delivery"


def mandatory_environment_variables():
    return ["BROKER_HOST", "BROKER_USERNAME", "BROKER_PASSWORD"]


def check_config():
    """Ensure configuration is complete."""
    return set(mandatory_environment_variables()).issubset(os.environ)


def get_config():
    return {
        "Broker": get_broker_config(),
        "DefaultExchange":  get_default_exchange(),
    }


def get_broker_config():
    return {
        "Host": os.environ["BROKER_HOST"],
        "User": os.environ["BROKER_USERNAME"],
        "Password": os.environ["BROKER_PASSWORD"],
        "Vhost": get_broker_vhost(),
    }


def get_broker_vhost():
    if "BROKER_VHOST" in os.environ:
        return os.environ["BROKER_VHOST"]

    return "/"


def get_default_exchange():
    if "DEFAULT_EXCHANGE" in os.environ:
        return os.environ["DEFAULT_EXCHANGE"]

    return None


#   -------------------------------------------------------------
#   Broker connection helper methods
#   - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


def get_credentials(config):
    """Get credentials to connect to the broker from the configuration."""
    return pika.PlainCredentials(
        username=config['Broker']['User'],
        password=config['Broker']['Password'],
        erase_on_connect=True
    )


def get_broker_connection_parameters(config):
    return pika.ConnectionParameters(
            host=config['Broker']['Host'],
            credentials=get_credentials(config),
            virtual_host=config['Broker']['Vhost'])


def get_broker_connection(config):
    """Connect to the broker."""
    parameters = get_broker_connection_parameters(config)
    return pika.BlockingConnection(parameters)


#   -------------------------------------------------------------
#   API helper methods
#   - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


def get_default_exchange_name():
    """Gets from the configuration the default exchange,
       or return a 400 if undefined."""
    if service['config']['DefaultExchange'] is None:
        abort(400)

    return service['config']['DefaultExchange']


def generate_queue_key():
    """Generates an API key matching a specific queue."""
    return str(uuid.uuid4())


def get_queue_name(queue_key):
    """Map a queue name with its key."""
    return "delivery-" + queue_key


def add_broker_queue(exchange_name, routing_key):
    """Add a queue to the broker, bind to the exchange."""
    queue_key = generate_queue_key()
    queue_name = get_queue_name(queue_key)

    connection = get_broker_connection(service['config'])
    channel = connection.channel()
    channel.queue_declare(durable=True,
                          queue=queue_name)
    try:
        channel.queue_bind(exchange=exchange_name,
                           queue=queue_name,
                           routing_key=routing_key)
    except pika.exceptions.ChannelClosed as err:
        # We've created a queue but we can't use it,
        # so let's try to delete it through a new connection.
        try:
            print("Deleting unused queue: {0}".format(queue_name))
            delete_broker_queue(queue_name, True)
        except pika.exceptions.ChannelClosed:
            pass

        raise err

    connection.close(reply_text="Operation done")

    return queue_key


def delete_broker_queue(queue_name, force_queue_deletion):
    connection = get_broker_connection(service['config'])
    channel = connection.channel()
    channel.queue_delete(queue=queue_name, if_empty=not force_queue_deletion)
    connection.close(reply_text="Operation done")


def get_variable_from_request(key, default_value=None):
    """Gets variable from request JSON payload or a default value."""
    if key in request.json:
        return request.json[key]

    return default_value


def get_exchange_from_request():
    """Get the exchange name from the request, the environment, or abort."""
    exchange_name = get_variable_from_request("exchange")

    if exchange_name is None:
        return get_default_exchange_name()

    return exchange_name


def error_handler(err):
    """Logs the exception, return 400"""
    print("Channel error: {0}".format(err))
    abort(400)


#   -------------------------------------------------------------
#   API methods
#   - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


@app.route(endpoint + "/status")
def status():
    """Determine if the application returns a 200 on GET request."""
    return "ALIVE"


@app.route(endpoint + "/register_consumer", methods=['POST'])
def register_consumer():
    """Subscribe to an exchange, record the queue, send queue key."""

    # Reads request
    if not request.json:
        abort(400)

    exchange_name = get_exchange_from_request()
    routing_key = get_variable_from_request("routing-key", "*")

    # Handles request
    try:
        queue_key = add_broker_queue(exchange_name, routing_key)
    except pika.exceptions.ChannelClosed as err:
        return error_handler(err)

    # Returns result
    return jsonify(key=queue_key)


@app.route(endpoint + "/unregister_consumer", methods=['POST'])
def unregister_consumer():
    """Unregister a queue key."""

    # Reads request
    if not request.json or "key" not in request.json:
        abort(400)

    queue = get_queue_name(request.json["key"])
    force_delete = get_variable_from_request("force", False)

    # Handles request
    try:
        delete_broker_queue(queue, force_delete)
        return jsonify(key=queue, success=True,
                       result="Queue deletion request sent to the broker.")
    except pika.exceptions.ChannelClosed as err:
        return error_handler(err)


#   -------------------------------------------------------------
#   Start application
#   - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


def initialize_application():
    """Initialize a container with required services."""
    return {
        'config': get_config()
    }


def run_application(web_application):
    """Run the server."""
    if not check_config():
        sys.exit(1)

    global service
    service = initialize_application()

    if __name__ == "__main__":
        web_application.run(host="0.0.0.0")


run_application(app)
