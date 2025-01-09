from influxdb_client import InfluxDBClient

# InfluxDB connection details (from the data input script)
INFLUXDB_URL = "http://raspberrypi.local:8086"  # Replace with your InfluxDB OSS URL
INFLUXDB_TOKEN = "Eu5BBoaNb-ozGhQlXpd1zOOYUu_VFe0e6PsvC7Z4zsBDPZtKe1bl1UoOyYLCWslerO5hNSIEqfIJFUfI8i0E7A=="  # Replace with your token
INFLUXDB_ORG = "xdd"  # Replace with your organization name
INFLUXDB_BUCKET = "nevim"  # Replace with your bucket name

# Initialize the InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

# Query all data from the specified bucket
query = f'''
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: 0)  // Fetch all data from the beginning
'''

try:
    # Execute the query
    result = query_api.query(org=INFLUXDB_ORG, query=query)

    # Process and print the results
    print(f"Querying all data from bucket '{INFLUXDB_BUCKET}' in org '{INFLUXDB_ORG}'...\n")
    for table in result:
        for record in table.records:
            print(f"Time: {record.get_time()}, Measurement: {record.get_measurement()}, Data: {record.values}")

except Exception as e:
    print(f"Error querying data: {e}")
