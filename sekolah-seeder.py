# <https://referensi.data.kemdikbud.go.id/pendidikan/dikmen/050725/3/jf/all/all>
# <https://referensi.data.kemdikbud.go.id/pendidikan/dikdas/050725/3/jf/all/all>
# <https://dapo.kemdikbud.go.id/sp/3/050725>

#  given the path where the file is located, the file will be read and the data will be inserted into the database
# the file is in json format
# the file contains data about sekolah
# the data will be inserted into the database
# the data will be inserted into the table ref_sekolah
# the data is an array arranged in the following example format
# [{"nama": "TK AISYIYAH BUSTANUL ATHFAL IV", "sekolah_id": "EEEA6C1F-DBDC-4D83-8930-DAF922CF3C6B", "npsn": 69897229, "jumlah_kirim": 6, "ptk": 2, "pegawai": 1, "pd": 30, "rombel": 2, "jml_rk": 2, "jml_lab": 0, "jml_perpus": 0, "induk_kecamatan": "Kec. Nanggung", "kode_wilayah_induk_kecamatan": "020501  ", "induk_kabupaten": "Kab. Bogor", "kode_wilayah_induk_kabupaten": "020500  ", "induk_provinsi": "Prov. Jawa Barat", "kode_wilayah_induk_provinsi": "020000  ", "bentuk_pendidikan": "TK", "status_sekolah": "Swasta", "sinkron_terakhir": "13 Mar 2024 13:45:13", "sekolah_id_enkrip": "C26F7E82C8DD08B62CA9                                                                                "},]
# I only need the following data
# nama, sekolah_id, npsn,  induk_kecamatan, kode_wilayah_induk_kecamatan, induk_kabupaten, kode_wilayah_induk_kabupaten, induk_provinsi, kode_wilayah_induk_provinsi, bentuk_pendidikan, status_sekolah, sinkron_terakhir, sekolah_id_enkrip
# please generate DDL for the table ref_sekolah
# here is DDl for the table ref_sekolah

# CREATE TABLE ref_sekolah (
# 	sekolah_id UUID PRIMARY KEY,	
#     nama VARCHAR(255),
#     npsn VARCHAR(10),
#     kode_wilayah_induk_kecamatan VARCHAR(255),
#     bentuk_pendidikan VARCHAR(255),
#     status_sekolah VARCHAR(255),
#     sinkron_terakhir TIMESTAMP,
#     sekolah_id_enkrip VARCHAR(255)
# );

# please generate python code to insert the data into the table ref_sekolah from the file in path/dir sekolah, the sequence of the data must be maintained by the file name in the directory

import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dateutil.parser import parse

# Function to check if a string is a valid timestamp
def is_valid_timestamp(timestamp_str):
    try:
        parse(timestamp_str)
        return True
    except ValueError:
        return False

# Connect to your postgres DB
conn = psycopg2.connect(
    dbname="smartppdb", 
    user="postgres", 
    password="secret", 
    host="localhost", 
    port="5432"
)

cur = conn.cursor()

# Get list of all files in a directory
path_to_json = 'sekolah'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

# Sort files to maintain the sequence
json_files.sort()

keys = ['sekolah_id','nama',  'npsn',  'kode_wilayah_induk_kecamatan', 'bentuk_pendidikan', 'status_sekolah', 'sinkron_terakhir', 'sekolah_id_enkrip']

for index, js in enumerate(json_files):
    with open(os.path.join(path_to_json, js)) as json_file:
        data = json.load(json_file)
        # Extract only the data we need, ensure all keys are present, and strip whitespace
        filtered_data = [{k: None if k == 'sinkron_terakhir' and not is_valid_timestamp(str(item.get(k, '')).strip()) else str(item.get(k, '')).strip() for k in keys} for item in data]
        # print(filtered_data)
        print(filtered_data)
        # Convert each dictionary in filtered_data to a tuple and ensure the order of values
        tuple_data = [tuple(item[k] for k in keys) for item in filtered_data]
        # Insert data into the database
        execute_values(cur, "INSERT INTO ref_sekolah (sekolah_id, nama, npsn,  kode_wilayah_induk_kecamatan, bentuk_pendidikan, status_sekolah, sinkron_terakhir, sekolah_id_enkrip) VALUES %s", tuple_data)
        conn.commit()

# Close the cursor and connection
cur.close()
conn.close()