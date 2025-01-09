from shutil import copy2

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime


# InfluxDB connection details
INFLUXDB_URL = "http://172.25.200.8:8086/"
INFLUXDB_TOKEN = "NVrfQy2lSIzWDWFTDMbiT1wYlx0fl0g7_O7I9EMb8eV49XP89FPO3UdtMV6_F9IUysM_SASucHL7LQ948pve2w=="
INFLUXDB_ORG = "spseol"
INFLUXDB_BUCKET = "testing"

# MQTT broker details
BROKER = "t331e64b.ala.eu-central-1.emqxsl.com"
PORT = 8883
USERNAME = "admin"
PASSWORD = "admin"
TOPIC = "sensor_dump"

# Initialize the InfluxDB client
client_influx = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client_influx.write_api(write_options=SYNCHRONOUS)

# Variables to store the latest temperature and humidity

def write_to_influxdb():
    global latest_temp, latest_humi

    # If both temperature and humidity are available, write to InfluxDB
    if latest_temp is not None and latest_humi is not None:
        current_time = datetime.utcnow()

        point = Point("Test_SPSEOL") \
            .tag("client_id", "sensor_1") \
            .field("temperature", latest_temp) \
            .field("humidity", latest_humi) \
            .time(current_time)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(f"Data written to InfluxDB: Temp={latest_temp}, Humi={latest_humi} at {current_time}")


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
        # Subscribe to both temperature and humidity topics
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code {rc}")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):

    try:
        # Check which topic the message came from and update the respective variable
        if msg.topic == TOPIC:
            json_str = msg.payload.decode()
            print(f"Temperature received: {json_str}")
        # Try writing to InfluxDB if both temperature and humidity are available
            data = json.loads(json_str)
            esp_id = data["esp_id"]
            mm = data["measurement"]
            time = mm["time"]
            temperature_raw = mm["temperature"]
            humidity_raw = mm["humidity"]
            co2 = mm["co2"]
            voc_index_raw = mm["voc_index"]
            nox_index_raw = mm["nox_index"]
            pm1p0_raw = mm["pm1p0"]
            pm2p5_raw = mm["pm2p5"]
            pm4p0_raw = mm["pm4p0"]
            pm10p0_raw = mm["pm10p0"]
            print(f"ESP ID: {esp_id}")
            print(f"Time: {time}")
            print(f"Temperature: {float(temperature_raw)/200}")
            print(f"Humidity: {float(humidity_raw)/100}")
            print(f"CO2: {co2}")
            print(f"VOC Index: {voc_index_raw/10}")
            print(f"NOx Index: {nox_index_raw/10}")
            print(f"PM1.0: {float(pm1p0_raw)/10}")
            print(f"PM2.5: {float(pm2p5_raw)/10}")
            print(f"PM4.0: {float(pm4p0_raw)/10}")
            print(f"PM10.0: {float(pm10p0_raw)/10}")
            temperature = float(temperature_raw)/200
            humidity = float(humidity_raw)/100
            voc_index = voc_index_raw/10
            nox_index = nox_index_raw/10
            pm1p0 = float(pm1p0_raw)/10
            pm2p5 = float(pm2p5_raw)/10
            pm4p0 = float(pm4p0_raw)/10
            pm10p0 = float(pm10p0_raw)/10

            point = Point("Test_SPSEOL") \
                .tag("esp_id", esp_id) \
                .field("temperature", temperature) \
                .field("humidity", humidity) \
                .field("co2", co2) \
                .field("voc_index", voc_index) \
                .field("nox_index", nox_index) \
                .field("pm1p0", pm1p0) \
                .field("pm2p5", pm2p5) \
                .field("pm4p0", pm4p0) \
                .field("pm10p0", pm10p0) \
                .time(time)
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)







    except Exception as e:
        print(f"Failed to process MQTT message: {e}")

# Create an MQTT client instance
client = mqtt.Client()

# Set username and password for broker authentication
client.username_pw_set(USERNAME, PASSWORD)
# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message
client.tls_set()

# Connect to the broker
client.connect(BROKER, PORT, 60)

# Blocking call to process network traffic and dispatch callbacks
client.loop_forever()
