import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime

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
VECHILE_TOPIC=os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC=os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC=os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC=os.getenv('EMERGENCY_TOPIC','emergency_data')

start_time=datetime.now()
start_location=POZEGA_COORDINATES.copy()



def simulate_journey(producer, device_id):
    while True:
        vehicle_data= generate_vehicle_data(device_id)


if __name__ == "__main__":
    producer_config={
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }   
    producer= SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vechile-Autic')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unespected Error occurred: {e}')
    


