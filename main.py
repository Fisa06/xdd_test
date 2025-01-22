
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json

#INFLUXDB_URL = "http://172.25.200.8:8086/"
INFLUXDB_URL = "http://localhost:8086/"
INFLUXDB_TOKEN = "NVrfQy2lSIzWDWFTDMbiT1wYlx0fl0g7_O7I9EMb8eV49XP89FPO3UdtMV6_F9IUysM_SASucHL7LQ948pve2w=="
INFLUXDB_ORG = "spseol"
INFLUXDB_BUCKET = "testing"


BROKER = "hroch.spseol.cz"
PORT = 8883
USERNAME = "python_script"
PASSWORD = "asdf"
SENSOR_DATA_DUMP_TOPIC = "spseol_aq_sensors/+/data"

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
            print(f"Key: {key}, Value: {float( (root['mm'][key])/(root['divider'][key]) )}")
            point.field(key, float( (root["mm"][key])/(root["divider"][key]) ))
        return point
    except Exception as e:
        print(f"Failed to process given data: {e}")

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("Connected successfully")
        client.subscribe(SENSOR_DATA_DUMP_TOPIC)
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):

    try:
        json_str = msg.payload.decode()
        print(f"Packet received: {json_str}")
        point = process_incoming_data(json_str)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    except Exception as e:
        print(f"Failed to process MQTT message: {e}")



client = mqtt.Client(protocol=mqtt.MQTTv5)

# Set username and password for broker authentication
client.username_pw_set(USERNAME, PASSWORD)
# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(ca_certs="ca_cert.crt")

# Connect to the broker
client.connect(BROKER, PORT, 60)

# Blocking call to process network traffic and dispatch callbacks
client.loop_forever()


