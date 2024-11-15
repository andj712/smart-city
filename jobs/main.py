import os
import random
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import uuid


POZEGA_COORDINATES= { "latitude":43.846,
                      "longitude":20.036
                      }
BEOGRAD_COORDINATES={
    "latitude":44.787,
    "longitude":20.457
}

#izracunavanje inkrementa 
LATITUDE_INCREMENT=(BEOGRAD_COORDINATES['latitude']-POZEGA_COORDINATES['latitude'])/100
LONGITUDE_INCREMENT=(BEOGRAD_COORDINATES['longitude']-POZEGA_COORDINATES['longitude'])/100

#varijable za konfiguraciju
KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
VEHICLE_TOPIC=os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC=os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC=os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC=os.getenv('EMERGENCY_TOPIC','emergency_data')

start_time=datetime.now()
start_location=POZEGA_COORDINATES.copy()

def generate_gps_data(device_id, timestamp,vehicle_type='private'):
    return{
        'id': uuid.uuid4(),
        'deviceId':device_id,
        'timestamp':timestamp,
        'speed':random.uniform(0,40),#km/h
        'direction':'North',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id,timestamp,camera_id):
    return{
        'id':uuid.uuid4,
        'deviceId':device_id,
        'cameraId':camera_id,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def simulate_vehicle_movement():
    global start_location
    
    #pomera se od Pozege ka Beogradu
    start_location['latitude']+=LATITUDE_INCREMENT
    start_location['longitude']+=LONGITUDE_INCREMENT

    #pomeranje random uniformna raspodela 
    start_location['latitude']+=random.uniform(-0.0005,0.0005)
    start_location['longitude']+=random.uniform(-0.0005,0.0005)

    return start_location
    
def get_next_time():
    global start_time
    start_time+=timedelta(seconds=random.randint(30,60))
    return start_time


def generate_vehicle_data(device_id):
    location=simulate_vehicle_movement()
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'],location['longitude']),
        'speed': random.uniform(10,40),
        'direction': 'North',
        'make': 'BMW',
        'model': 'C500',
        'year':2024,
        'fuelType': 'Hybrid'
    }

def simulate_journey(producer, device_id):
    while True:
        vehicle_data= generate_vehicle_data(device_id)
        gps_data= generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data=generate_traffic_camera_data(device_id,vehicle_data['timestamp'],'Nikon123')
        weather_data= generate_weather_data(device_id,vehicle_data['timestamp'],vehicle_data)
        break

if __name__ == "__main__":
    producer_config={
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }   
    producer= SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Autic')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unespected Error occurred: {e}')
    


