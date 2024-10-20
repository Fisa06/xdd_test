from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# InfluxDB connection details
url = "http://raspberrypi.local:8086"  # Replace with your InfluxDB OSS URL
token = "anVQZfvzSY_vs6fsQvHrl9kr6oCCwiNHgHg_oH2iOy-BVeMlwuV2YrozbSOecNKF4kVpdjQas3wqJNuRTmDu3A=="  # Replace with your token
org = "xdd"  # Replace with your organization name
bucket = "chlapik"  # Replace with your bucket name

# Initialize the InfluxDB client
client = InfluxDBClient(url=url, token=token, org=org)

# Write API
write_api = client.write_api(write_options=SYNCHRONOUS)

# Example data points to insert
data = [
    {"measurement": "environment", "tags": {"sensor": "sensor_1", "location": "room1"},
     "fields": {"temperature": 23.5, "humidity": 45.3}, "time": datetime.utcnow()},
    {"measurement": "environment", "tags": {"sensor": "sensor_2", "location": "room1"},
     "fields": {"temperature": 24.0, "humidity": 46.0}, "time": datetime.utcnow()},
    {"measurement": "environment", "tags": {"sensor": "sensor_3", "location": "room2"},
     "fields": {"temperature": 22.0, "humidity": 44.5}, "time": datetime.utcnow()},
    {"measurement": "environment", "tags": {"sensor": "sensor_4", "location": "room2"},
     "fields": {"temperature": 25.0, "humidity": 47.5}, "time": datetime.utcnow()},
]
print(datetime.utcnow())

# Insert data into InfluxDB
for point_data in data:
    point = Point(point_data["measurement"]) \
        .tag("sensor", point_data["tags"]["sensor"]) \
        .tag("location", point_data["tags"]["location"]) \
        .field("temperature", point_data["fields"]["temperature"]) \
        .field("humidity", point_data["fields"]["humidity"]) \
        .time(point_data["time"])

    write_api.write(bucket=bucket, org=org, record=point)

print("Data written successfully.")

# Close the client connection
client.close()
