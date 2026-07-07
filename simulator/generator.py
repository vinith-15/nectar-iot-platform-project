import json, random, time
from datetime import datetime
from kafka import KafkaProducer
import threading

KAFKA_BROKER = 'localhost:9092'
TELEMETRY_TOPIC = 'iot-telemetry'
EVENTS_TOPIC = 'iot-events'
METADATA_TOPIC = 'asset-metadata'

SITES = ['site-001','site-002','site-003']
BUILDINGS = {'site-001': ['bldg-1','bldg-2'], 'site-002': ['bldg-3'], 'site-003': ['bldg-4','bldg-5','bldg-6']}
ASSETS = {
    'bldg-1': ['asset-101','asset-102'],
    'bldg-2': ['asset-201','asset-202','asset-203'],
    'bldg-3': ['asset-301'],
    'bldg-4': ['asset-401','asset-402'],
    'bldg-5': ['asset-501'],
    'bldg-6': ['asset-601','asset-602','asset-603']
}
SENSORS = ['sensor-01','sensor-02','sensor-03']

def generate_telemetry():
    site = random.choice(SITES)
    building = random.choice(BUILDINGS[site])
    asset = random.choice(ASSETS[building])
    return {
        'timestamp': datetime.utcnow().isoformat()+'Z',
        'site_id': site, 'building_id': building, 'asset_id': asset,
        'sensor_id': random.choice(SENSORS),
        'temperature': round(random.uniform(18,30),2),
        'humidity': round(random.uniform(30,70),2),
        'pressure': round(random.uniform(980,1020),2),
        'vibration': round(random.uniform(0,5),2),
        'power_consumption': round(random.uniform(10,500),2),
        'operating_mode': random.choice(['Normal','Idle','Maintenance','Fault'])
    }

def generate_event():
    site = random.choice(SITES)
    building = random.choice(BUILDINGS[site])
    asset = random.choice(ASSETS[building])
    return {
        'event_id': f"evt-{int(time.time())}-{random.randint(1000,9999)}",
        'timestamp': datetime.utcnow().isoformat()+'Z',
        'asset_id': asset,
        'event_type': random.choice(['Alarm','Warning','Fault']),
        'severity': random.choice(['Low','Medium','High']),
        'message': random.choice(['Overheating','Low pressure','Vibration spike','Power surge'])
    }

def generate_metadata():
    assets_list = []
    for bldg, assets in ASSETS.items():
        for asset in assets:
            assets_list.append({
                'asset_id': asset,
                'asset_name': f"Asset {asset}",
                'asset_type': random.choice(['AHU','Chiller','Pump','Sensor']),
                'manufacturer': random.choice(['BrandA','BrandB','BrandC']),
                'installation_date': datetime.now().strftime('%Y-%m-%d'),
                'site_id': [s for s,bldgs in BUILDINGS.items() if bldg in bldgs][0]
            })
    return assets_list

def produce_metadata():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for item in generate_metadata():
        producer.send(METADATA_TOPIC, item)
    producer.flush()
    print("Metadata sent.")

def produce_continuous():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while True:
        producer.send(TELEMETRY_TOPIC, generate_telemetry())
        if random.random() < 0.1:
            producer.send(EVENTS_TOPIC, generate_event())
        time.sleep(1)

if __name__ == '__main__':
    produce_metadata()
    threading.Thread(target=produce_continuous, daemon=True).start()
    print("Simulator running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping.")
