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
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from urllib.request import urlopen
from datetime import date
config_file = "/home/tatianal/config.conf"

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    topic = "sensor_data"
    conf = ccloud_lib.read_config(config_file)
    today = date.today()

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            # print("Produced record to topic {} partition [{}] @ offset {}"
            #      .format(msg.topic(), msg.partition(), msg.offset()))

    #url = "http://rbi.ddns.net/getBreadCrumbData"

    #html = urlopen(url)

    #string = html.read().decode('utf-8')

    #data = json.loads(string)

    date_time = today.strftime("%b_%d_%Y")
    file_name = '/home/tatianal/sensor_logs_for__' + date_time + '.json'
    with open('/home/tatianal/sensor_data/2021_02_20.json') as f:
        data = json.load(f)
    
        count = 0
        with open(file_name, 'w') as file:
            json.dump(len(data), file)
        for element in data:
            record_event_no_trip = element['EVENT_NO_TRIP']
            record_event_no_stop = element['EVENT_NO_STOP']
            record_opd_date = element['OPD_DATE']
            record_vehicle_id = element['VEHICLE_ID']
            record_meters = element['METERS']
            record_act_time = element['ACT_TIME']
            record_velocity = element['VELOCITY']
            record_direction = element['DIRECTION']
            record_radio_quality = element['RADIO_QUALITY']
            record_gps_longitude = element['GPS_LONGITUDE']
            record_gps_latitude = element['GPS_LATITUDE']
            record_gps_satellites = element['GPS_SATELLITES']
            record_gps_hdop = element['GPS_HDOP']
            record_schedule_deviation = element['SCHEDULE_DEVIATION']

            producer.produce(topic, value=json.dumps(element), on_delivery=acked)

        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
            producer.poll(0)
            if count % 10000 == 0:
                producer.flush()
            count+=1

        producer.flush()

    txt = "\n{} messages were produced to topic {}!".format(delivered_records, topic)
    with open(file_name, 'a') as file:
        file.write(txt)

