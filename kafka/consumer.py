from utils.utils import get_velib_data
from kafka import KafkaConsumer
import csv
from datetime import date
import os.path
import json

CSV_FOLDER_PATH = "../saved_data/"

consumer = KafkaConsumer('velib_data', bootstrap_servers='localhost:9092')

# Create an empty list for storing the station keys that have already been written
existing_station_keys = []

for msg in consumer:
    data = json.loads(msg.value.decode('utf-8'))
    stations = data['records']
    
    today_date = date.today().strftime("%d_%m_%Y")
    csv_name = "velib_data_" + today_date + ".csv"
    csv_file_path = CSV_FOLDER_PATH + csv_name

    # Check if the file exists
    file_exist = os.path.isfile(csv_file_path)

    # Create the folder if it doesn't exist
    if not os.path.exists(CSV_FOLDER_PATH):
        os.makedirs(CSV_FOLDER_PATH)

    # Open the CSV file in append mode
    with open(csv_file_path, mode='a', newline='\n', encoding='UTF8') as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        
        # Write the header row if the file is newly created
        if not file_exist:
            header = ['station_name', 'station_code', 'ebike', 'mechanical', 'latitude', 'longitude', 'due_date', 'num_bikes_available', 'num_docks_available', 'capacity', 'is_renting', 'is_installed', 'city', 'is_returning']
            writer.writerow(header)
            print("Header row written to the CSV file.")
        
        # Write the values of each station as a row in the CSV file
        for station in stations:
            station_info = station['fields']
            
            # Create the station key for deduplication
            station_key = station_info['name'] + station_info['stationcode'] + station_info['duedate']
            
            # Check if the station key has already been written to the file
            if station_key in existing_station_keys:
                print("Duplicate station found. Skipping row.")
                continue
            
            # Write the row to the file and add the station key to the list of existing keys
            row = [
                station_info['name'],
                station_info['stationcode'],
                station_info['ebike'],
                station_info['mechanical'],
                station_info['coordonnees_geo'][0],
                station_info['coordonnees_geo'][1],
                station_info['duedate'],
                station_info['numbikesavailable'],
                station_info['numdocksavailable'],
                station_info['capacity'],
                station_info['is_renting'],
                station_info['is_installed'],
                station_info['nom_arrondissement_communes'],
                station_info['is_returning']
            ]
            writer.writerow(row)
            existing_station_keys.append(station_key)
            print("Data written to the CSV file.")
