from utils.utils import get_velib_data, get_single_station_data
from kafka import KafkaConsumer
import csv
from datetime import date
import os.path
import json

CSV_FOLDER_PATH = "../saved_data/"

consumer = KafkaConsumer('velib_data', bootstrap_servers='localhost:9092')

for msg in consumer:
    station_dict = json.loads(msg.value.decode('utf-8'))
    
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
            writer.writerow(list(station_dict.keys()))
            print("Header row written to the CSV file.")
        
        # Write the values of the station dictionary as a row in the CSV file
        writer.writerow(station_dict.values())
        print("Data written to the CSV file.")