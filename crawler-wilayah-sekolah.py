import requests
import json
import os
from functools import lru_cache

# Base URL
url_default = 'https://dapo.kemdikbud.go.id/rekap/dataSekolah'
url_level3= 'https://dapo.kemdikbud.go.id/rekap/progresSP'

# Specify the directory where the JSON files will be saved
base_path = 'json'
dir_path_wilayah = os.path.join(os.getcwd(), base_path,'wilayah')
dir_path_sekolah = os.path.join(os.getcwd(), base_path,'sekolah')

@lru_cache(maxsize=None)
def file_exists(file_path):
    return os.path.exists(file_path)

def crawl_data(id_level_wilayah,  kode_wilayah):
    # Prepare the URL and parameters
    params = {
        'id_level_wilayah': id_level_wilayah,
        'kode_wilayah': kode_wilayah,
        'semester_id': '20232'  # Assuming the semester_id stays the same
    }

    if id_level_wilayah == 3:
        url = url_level3
        dir_path = dir_path_sekolah
    else:
        url = url_default
        dir_path = dir_path_wilayah

    # Construct the filename and file path
    filename = f"{id_level_wilayah}_{kode_wilayah}.json"
    filename = filename.replace(" ", "")  # Remove spaces
    file_path = os.path.join(dir_path, filename)

    # If a file for this id_level_wilayah and kode_wilayah already exists, read the data from the file
    if file_exists(file_path):
        if id_level_wilayah == 3:
            print(f"skip data already exist: {file_path}")
            return
        print(f"Read data from: {file_path}")
        with open(file_path, 'r') as f:
            data = json.load(f)
    else:
        # Prepare the URL
        request_url = requests.Request('GET', url, params=params).prepare().url

        # Echo the URL
        print(f"Fetching data from: {request_url}")

        # Fetch the data
        response = requests.get(url, params=params)
        data = response.json()

        # Save the data to a JSON file
        with open(file_path, 'w') as f:
            json.dump(data, f)

    # If we're not at the deepest level, crawl the next level
    if id_level_wilayah < 3 and data:
        for item in data:
            crawl_data(id_level_wilayah + 1, item['kode_wilayah'].strip())

# Start crawling from the top level
crawl_data(0, '000000')