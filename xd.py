import csv
from datetime import datetime, timedelta

# File name for the CSV
csv_file = "sensor_data.csv"

# Column headers
headers = ["time", "sensor_id", "location", "temperature", "humidity"]

# Starting time for the data
start_time = datetime.utcnow()

# Sample locations and sensors
sensors = ["sensor_1", "sensor_2", "sensor_3", "sensor_4"]
locations = ["room1", "room1", "room2", "room2"]

# Data generation
data = []
for i in range(10):  # Generating 10 data points
    for sensor, location in zip(sensors, locations):
        time_point = start_time + timedelta(minutes=i*5)  # Each sensor reading 5 minutes apart
        temperature = round(20 + (i % 5) + (hash(sensor) % 3), 2)  # Random temperature variation
        humidity = round(40 + (i % 5) + (hash(sensor) % 5), 2)     # Random humidity variation
        data.append([time_point.isoformat() + "Z", sensor, location, temperature, humidity])

# Write the data to a CSV file
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(headers)  # Writing headers
    writer.writerows(data)    # Writing data rows

print(f"CSV file '{csv_file}' created successfully.")
