from shutil import copy2

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json

INFLUXDB_URL = "http://172.25.200.8:8086/"
INFLUXDB_TOKEN = "NVrfQy2lSIzWDWFTDMbiT1wYlx0fl0g7_O7I9EMb8eV49XP89FPO3UdtMV6_F9IUysM_SASucHL7LQ948pve2w=="
INFLUXDB_ORG = "spseol"
INFLUXDB_BUCKET = "testing"


BROKER = "t331e64b.ala.eu-central-1.emqxsl.com"
PORT = 8883
USERNAME = "admin"
PASSWORD = "admin"
SENSOR_DATA_DUMP_TOPIC = "sensor_dump"

client_influx = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client_influx.write_api(write_options=SYNCHRONOUS)





def process_incoming_data(json_str) -> Point:
    try:
        root = json.loads(json_str)
        mm_keys = root["mm"].keys()
        point = Point(root["measurement"])
        point.time(root["time"])
        point.tag("esp_id", root["esp_id"])
        if "placement" in root:
            point.tag("placement", root["placement"])


        for key in mm_keys:
            if (root["mm"][key])%(root["mm"]["divider"]) == 0:                       #zmenit vsechno na jeden datovy typ
                point.field(key, int( (root["mm"][key])/(root["mm"]["divider"])))
            else:
                point.field(key, float( (root["mm"][key])/(root["mm"]["divider"])))
        return point
    except Exception as e:
        print(f"Failed to process given data: {e}")

class ElClient(mqtt.Client):


    def __init__(self):
        super().__init__()
        self.username_pw_set(USERNAME, PASSWORD)
        self.tls_set()
        self.on_connect = self.on_connectt
        self.on_message = self.on_messagee
        self.tls_set()


    def connect_to_broker(self):
        self.connect(BROKER, PORT, 60)


    def on_connectt(self, userdata, flags, rc):
        if rc == 0:
            print("Connected successfully")
            self.subscribe(SENSOR_DATA_DUMP_TOPIC)
        else:
            print(f"Connection failed with code {rc}")


    def on_messagee(self, userdata, msg):

        try:
            if msg.topic == SENSOR_DATA_DUMP_TOPIC:
                json_str = msg.payload.decode()
                print(f"Packet received: {json_str}")
                point = process_incoming_data(json_str)
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        except Exception as e:
            print(f"Failed to process MQTT message: {e}")


# client = mqtt.Client()
#
# # Set username and password for broker authentication
# client.username_pw_set(USERNAME, PASSWORD)
# # Assign the callback functions
# client.on_connect = on_connect
# client.on_message = on_message
# client.tls_set()
#
# # Connect to the broker
# client.connect(BROKER, PORT, 60)
#
# # Blocking call to process network traffic and dispatch callbacks
# client.loop_forever()


