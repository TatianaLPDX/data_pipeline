#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from validations import Validations
from confluent_kafka import Consumer
import json
import ccloud_lib
import datetime
import psycopg2

TableTrip = "Trip"
TableBreadcrumb = "BreadCrumb"
config_file = "/home/tatianal/config.conf"

def getBreadcrumbVal(act_time, opd, lat, long, dir, speed, trip_id):
    tstamp = opd + datetime.timedelta(seconds=act_time)
    val = f"""
    '{tstamp}',
    {lat},
    {long},
    {dir},
    {speed},
    {trip_id}
    """

    return val


def getTripVal(trip_id, route_id, vehicle_id, service_key, dir):

    val = f"""
    {trip_id},
    {route_id},
    {vehicle_id},
    '{service_key}',
    '{dir}'
    """

    return val

# connect to the database
def dbconnect(conf):

    connection = psycopg2.connect(
        host=conf['DBhost'],
        database=conf['DBname'],
        user=conf['DBuser'],
        password=conf['DBpwd'],
        )
    connection.autocommit = True
    return connection

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    topic = "sensor_data"
    conf = ccloud_lib.read_config(config_file)
    v = Validations()
    conn = dbconnect(conf)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'sensor_readings_consumer',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    # total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_value = msg.value()
                data = json.loads(record_value)

                print(data)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        # conn.close()

