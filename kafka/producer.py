from kafka import KafkaProducer
from utils.utils import get_velib_data, get_single_station_data
import json
from time import sleep

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

last_record_id = None

while True:
    data = get_velib_data(100)
    # new_record_id = data['records'][0]['recordid']
    
    # if new_record_id != last_record_id:
    print("Sending data to Kafka...")
    producer.send('velib_data', data)
        # last_record_id = new_record_id
    
    print("Waiting for the next data update...")
    sleep(10)

#from kafka import KafkaProducer
# import time
# import json
# from datetime import datetime, date, timedelta
# from dateutil import parser
# import requests
# import os

# KAFKA_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# LAST_TIMESTAMP_FILE_PATH = "last_timestamp.json"
# VELIB_REST_API_URL = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&rows={}"

# # Initialisation du producteur Kafka
# producer = KafkaProducer(
#     bootstrap_servers="localhost:9092", value_serializer=lambda x: json.dumps(x).encode("utf-8")
# )

# while True:
#     # Charger le fichier JSON des derniers timestamps
#     if os.path.isfile(LAST_TIMESTAMP_FILE_PATH):
#         with open(LAST_TIMESTAMP_FILE_PATH, "r") as f:
#             last_refresh_timestamps = json.load(f)
#     else:
#         last_refresh_timestamps = {}

#     # Obtenir les données des stations Vélib
#     response = requests.get(VELIB_REST_API_URL.format(9999))
#     velib_station_data = response.json()

#     # Filtrer les stations avec des dates valides
#     ref_date = date.today() - timedelta(days=7)
#     filtered_data = [
#         record
#         for record in velib_station_data["records"]
#         if "duedate" in record["fields"]
#         and ref_date < parser.parse(record["fields"]["duedate"]).date()
#     ]

#     # Parcourir les données des stations filtrées
#     for station_info in filtered_data:
#         # Convertir et renommer la colonne de date
#         due_date = datetime.strptime(station_info["fields"]["duedate"], "%Y-%m-%dT%H:%M:%S%z")

#         timestamp = int(due_date.timestamp())
#         station_info["fields"]["timestamp"] = timestamp
#         del station_info["fields"]["duedate"]

#         # Récupérer le code de la station
#         station_code = station_info["fields"].get("stationcode")

#         # Ignorer les stations sans code
#         if station_code is None:
#             continue

#         # Récupérer le dernier timestamp de rafraîchissement de la station
#         station_last_refresh = station_info["fields"]["timestamp"]

#         # Vérifier si le code de la station existe déjà dans les derniers timestamps
#         if station_code in last_refresh_timestamps:
#             # Comparer les dates pour vérifier si la nouvelle date est plus récente
#             if datetime.fromtimestamp(station_last_refresh) > datetime.fromtimestamp(
#                 last_refresh_timestamps[station_code]
#             ):
#                 print(station_info["fields"]["stationcode"], " UPDATED")
#                 producer.send("stations_raw_data", station_info["fields"])
#                 last_refresh_timestamps[station_code] = station_last_refresh
#         else:
#             print(station_info["fields"]["stationcode"], " CREATED")
#             producer.send("stations_raw_data", station_info["fields"])
#             last_refresh_timestamps[station_code] = station_last_refresh

#     # Enregistrer les derniers timestamps
#     with open(LAST_TIMESTAMP_FILE_PATH, "w") as f:
#         json.dump(last_refresh_timestamps, f)

#     # Attendre 60 secondes avant de rafraîchir les données
#     time.sleep(60)