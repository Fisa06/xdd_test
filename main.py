
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime

# InfluxDB connection details
INFLUXDB_URL = "http://raspberrypi.local:8086"    # Replace with your InfluxDB OSS URL
INFLUXDB_TOKEN = "Eu5BBoaNb-ozGhQlXpd1zOOYUu_VFe0e6PsvC7Z4zsBDPZtKe1bl1UoOyYLCWslerO5hNSIEqfIJFUfI8i0E7A=="        # Replace with your token
INFLUXDB_ORG = "xdd"                 # Replace with your organization name
INFLUXDB_BUCKET = "nevim"           # Replace with your bucket name



# Initialize the InfluxDB client
client_influx = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client_influx.write_api(write_options=SYNCHRONOUS)

# MQTT broker details
BROKER = "t331e64b.ala.eu-central-1.emqxsl.com"
PORT = 8883
USERNAME = "grafana"
PASSWORD = "grafana"
TOPIC_TEMP = "xdd/temp"  # Topic for temperature
TOPIC_HUMI = "xdd/humi"  # Topic for humidity

# Variables to store the latest temperature and humidity
latest_temp = None
latest_humi = None

# Function to write to InfluxDB when both temperature and humidity are available
def write_to_influxdb():
    global latest_temp, latest_humi

    # If both temperature and humidity are available, write to InfluxDB
    if latest_temp is not None and latest_humi is not None:
        current_time = datetime.utcnow()

        point = Point("environment") \
            .tag("sensor", "sensor_1") \
            .field("temperature", latest_temp) \
            .field("humidity", latest_humi) \
            .time(current_time)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(f"Data written to InfluxDB: Temp={latest_temp}, Humi={latest_humi} at {current_time}")

        # Reset the latest temperature and humidity after writing to InfluxDB
        latest_temp = None
        latest_humi = None

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
        # Subscribe to both temperature and humidity topics
        client.subscribe(TOPIC_TEMP)
        client.subscribe(TOPIC_HUMI)
    else:
        print(f"Connection failed with code {rc}")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global latest_temp, latest_humi

    try:
        # Check which topic the message came from and update the respective variable
        if msg.topic == TOPIC_TEMP:
            latest_temp = float(msg.payload.decode())
            print(f"Temperature received: {latest_temp}")
        elif msg.topic == TOPIC_HUMI:
            latest_humi = float(msg.payload.decode())
            print(f"Humidity received: {latest_humi}")

        # Try writing to InfluxDB if both temperature and humidity are available
        write_to_influxdb()

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
