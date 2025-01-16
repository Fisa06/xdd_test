import json

def process_incomeing_data(json_str):
    try:
        root = json.loads(json_str)
        mm = root["mm"]
        mm_keys = mm.keys()
        print(mm_keys)
        print(mm["time"])


    except Exception as e:
        print(f"Failed to process MQTT message: {e}")

xdd = '{"mm": {"time": 1626955200, "temperature": 200, "humidity": 100, "co2": 400, "voc_index": 10, "nox_index": 10, "pm1p0": 10, "pm2p5": 10, "pm4p0": 10, "pm10p0": 10}, "esp_id": "ESP32-1"}'
process_incomeing_data(xdd)

