from os import path
import requests

URL = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&lang=fr&rows={}&sort=duedate"
SINGLE_STATION_URL = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&lang=fr&rows=9999&sort=duedate&facet=stationcode&refine.stationcode=25005"

def get_velib_data(nrows):
    url = URL.format(nrows)
    response = requests.get(url)
    return response.json()

def get_single_station_data():
    response = requests.get(SINGLE_STATION_URL)
    return response.json()
