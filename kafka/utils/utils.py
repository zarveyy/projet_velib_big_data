from os import path
import requests

URL = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&lang=fr&rows={}&sort=duedate"

def get_velib_data(nrows):
    url = URL.format(nrows)
    response = requests.get(url)
    return response.json()
