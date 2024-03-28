import requests
import json
import os


# Base URL
url = 'https://dapo.kemdikbud.go.id/rekap/dataPD'

# Specify the directory where the JSON files will be saved
dir_path = 'json'

# Fetch the initial data
response = requests.get(url, params={'id_level_wilayah': 0, 'kode_wilayah': '000000', 'semester_id': '20232'})
data = response.json()

# Generate new URLs and fetch the data
for item in data:
    new_url = requests.Request('GET', url, params={
        'id_level_wilayah': item['id_level_wilayah'],
        'kode_wilayah': item['kode_wilayah'].strip(),  # Remove trailing spaces
        'semester_id': '20232'  # Assuming the semester_id stays the same
    }).prepare().url

    # Fetch the data
    response = requests.get(new_url)
    new_data = response.json()

    # Save the data to a JSON file in the specified directory
    filename = f"{item['id_level_wilayah']}_{item['mst_kode_wilayah']}_{item['kode_wilayah'].strip()}.json"

    file_path = os.path.join(dir_path, filename)
    with open(file_path, 'w') as f:
        json.dump(new_data, f)