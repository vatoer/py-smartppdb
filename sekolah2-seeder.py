import os
import json
import psycopg2
import logging

# Set up logging
logging.basicConfig(filename='duplicate_keys.log', level=logging.INFO)
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

# Define the directory
directory = 'json/dikdas'

# Iterate over all the files in the directory
for filename in os.listdir(directory):
    # Check if the file is a JSON file
    if filename.endswith('.json'):
        # Extract the kode_wilayah_induk_kecamatan from the filename
        kode_wilayah_induk_kecamatan = filename[:-5]

        # Open the file
        with open(os.path.join(directory, filename), 'r') as f:
            # Load the data from the file
            data = json.load(f)
            
            # Iterate over the data
            for row in data:
                # Check if the row is not empty and does not contain "No data available in table"
                if row and row != ["No data available in table"]:
                    # Extract the data
                    npsn = row[1]
                    nama = row[2]
                    alamat = row[3]
                    kelurahan_desa = row[4]
                    status_sekolah = row[5]

                    try:
                        # Insert the data into the database
                        cur.execute(
                            "INSERT INTO referensi_sekolah (npsn, nama, alamat, kelurahan_desa, kode_wilayah_induk_kecamatan, status_sekolah) VALUES (%s, %s, %s, %s, %s, %s)",
                            (npsn, nama, alamat, kelurahan_desa, kode_wilayah_induk_kecamatan, status_sekolah)
                        )
                        # Commit the changes
                        conn.commit()
                        # print(f'Inserted {npsn} into database')
                        print(f'Inserted {npsn} into database')
                    except psycopg2.errors.UniqueViolation:
                        # If a UniqueViolation error is raised, rollback the transaction and continue with the next iteration
                        conn.rollback()
                        logging.info(f'Duplicate key error on npsn: {npsn} in file: {filename}')
                        continue

# Close the connection
conn.close()