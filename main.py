import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime

# InfluxDB connection details
INFLUXDB_URL = "http://raspberrypi.local:8086"    # Replace with your InfluxDB OSS URL
INFLUXDB_TOKEN = "Eu5BBoaNb-ozGhQlXpd1zOOYUu_VFe0e6PsvC7Z4zsBDPZtKe1bl1UoOyYLCWslerO5hNSIEqfIJFUfI8i0E7A=="        # Replace with your token
INFLUXDB_ORG = "xdd"                 # Replace with your organization name
INFLUXDB_BUCKET = "home"           # Replace with your bucket name

# Initialize the InfluxDB client
client_influx = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client_influx.write_api(write_options=SYNCHRONOUS)

# MQTT broker details
BROKER = "t331e64b.ala.eu-central-1.emqxsl.com"
PORT = 8883
USERNAME = "pyscript"
PASSWORD = "123abc456"
TOPIC = "sensor_dump"  # Topic for environment data

# Function to write to InfluxDB
def write_to_influxdb(sensor_data):
    try:

        for measurement in sensor_data["mm"]:
            print(measurement)
            # Convert the incoming timestamp to UTC datetime
            incoming_time = datetime.strptime(measurement["time"], "%Y-%m-%d %H:%M:%S")
            # Create a point for each measurement
            point = Point("environment") \
                .tag("sensor", sensor_data["esp_id"]) \
                .field("temperature", measurement["temperature"]) \
                .field("humidity", measurement["humidity"]) \
                .time(incoming_time)

            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        # Write all points to InfluxDB

        print(f"Data written to InfluxDB for {sensor_data['esp_id']} with incoming timestamps.")

    except Exception as e:
        print(f"Failed to write data to InfluxDB: {e}")

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
        # Subscribe to the topic
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code {rc}")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    try:
        # Parse the incoming JSON payload
        sensor_data = json.loads(msg.payload.decode())
        print(f"Received data: {sensor_data}")

        # Write to InfluxDB
        write_to_influxdb(sensor_data)

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
