# given the path where the file is located, the file will be read and the data will be inserted into the database
# the file is in json format
# the file contains data about wilayah sekolah
# the data will be inserted into the database
# the data will be inserted into the table dapo_wilayah
# the data is an array arranged in the following example format
# [
#     {"nama": "Prov. Jawa Timur", "kode_wilayah": "050000  ", "id_level_wilayah": 1, "mst_kode_wilayah": 0, "induk_provinsi": null, "kode_wilayah_induk_provinsi": null, "induk_kabupaten": null, "kode_wilayah_induk_kabupaten": null, "tk_n": 188, "tk_s": 18637, "tk": 18825, "kb_n": 1, "kb_s": 14974, "kb": 14975, "tpa_n": 1, "tpa_s": 395, "tpa": 396, "sps_n": 0, "sps_s": 4158, "sps": 4158, "pkbm_n": 0, "pkbm_s": 924, "pkbm": 924, "skb_n": 19, "skb_s": 0, "skb": 19, "sd_n": 16906, "sd_s": 2107, "sd": 19013, "smp_n": 1737, "smp_s": 3304, "smp": 5041, "sma_n": 423, "sma_s": 1096, "sma": 1519, "smk_n": 298, "smk_s": 1863, "smk": 2161, "slb_n": 67, "slb_s": 330, "slb": 397, "sekolah_n": 19640, "sekolah_s": 47788, "sekolah": 67428},
# ]
# I only need the following data
# nama, kode_wilayah, id_level_wilayah, mst_kode_wilayah, induk_provinsi, kode_wilayah_induk_provinsi, induk_kabupaten, kode_wilayah_induk_kabupaten
# CREATE TABLE dapo_wilayah (
#     kode_wilayah CHAR(8) PRIMARY KEY,
#     nama VARCHAR(255),    
#     id_level_wilayah INTEGER,
#     mst_kode_wilayah CHAR(8),
#     induk_provinsi VARCHAR(255),
#     kode_wilayah_induk_provinsi CHAR(8),
#     induk_kabupaten VARCHAR(255),
#     kode_wilayah_induk_kabupaten CHAR(8)
# );
# the data will be inserted into the database
# please generate DDL for the table dapo_wilayah
# please generate python code to insert the data into the table dapo_wilayah from the file in path/dir wilayah, the sequence of the data must be maintained by the file name in the directory

import os
import json
import psycopg2

# Establish a connection to the database
conn = psycopg2.connect(
    dbname="crawl_kemendikbud",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

# Create a cursor object
cur = conn.cursor()

# Directory containing the JSON files
directory = os.path.join('json', 'wilayah')


# Get a list of all files in the directory, sorted by name
files = sorted(os.listdir(directory))

# Iterate over each file in the directory
for file in files:
    # Construct the full file path
    file_path = os.path.join(directory, file)

    # Open the file and load the JSON data
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Iterate over each item in the data
    for item in data:
        # Extract the necessary fields and strip whitespace
        nama = item['nama'].strip()
        kode_wilayah = item['kode_wilayah'].strip()
        id_level_wilayah = item['id_level_wilayah']
        mst_kode_wilayah = item['mst_kode_wilayah'].strip() if item['mst_kode_wilayah'] else None
        induk_provinsi = item['induk_provinsi'].strip() if item['induk_provinsi'] else None
        kode_wilayah_induk_provinsi = item['kode_wilayah_induk_provinsi'].strip() if item['kode_wilayah_induk_provinsi'] else None
        induk_kabupaten = item['induk_kabupaten'].strip() if item['induk_kabupaten'] else None
        kode_wilayah_induk_kabupaten = item['kode_wilayah_induk_kabupaten'].strip() if item['kode_wilayah_induk_kabupaten'] else None

        # Print the extracted data
        print(nama, kode_wilayah, id_level_wilayah, mst_kode_wilayah, induk_provinsi, kode_wilayah_induk_provinsi, induk_kabupaten, kode_wilayah_induk_kabupaten)

        # Insert the data into the wilayah table
        cur.execute(
            "INSERT INTO dapo_wilayah ( kode_wilayah, nama,id_level_wilayah, mst_kode_wilayah, induk_provinsi, kode_wilayah_induk_provinsi, induk_kabupaten, kode_wilayah_induk_kabupaten) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            ( kode_wilayah,nama, id_level_wilayah, mst_kode_wilayah, induk_provinsi, kode_wilayah_induk_provinsi, induk_kabupaten, kode_wilayah_induk_kabupaten)
        )

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()