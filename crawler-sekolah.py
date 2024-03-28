# pip install beautifulsoup4
# <https://googlechromelabs.github.io/chrome-for-testing/#stable>
from functools import lru_cache
import json
import os
import requests
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tabulate import tabulate
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

# Execute the query
cur.execute("SELECT kode_wilayah FROM dapo_wilayah WHERE dapo_wilayah.id_level_wilayah = 3")

# Fetch all the rows
kode_wilayahs = cur.fetchall()

# Create a FirefoxOptions object
options = Options()

# Set the headless option firefox
# options.headless = True 
# Set the headless option chrome
options.add_argument("--headless")

# Create a new instance of the Chrome driver
driver = webdriver.Chrome(options=options)

# Specify the directory where the JSON files will be saved
base_path = 'json'
dir_path_dikdas = os.path.join(os.getcwd(), base_path,'sekolah-dikdas')
dir_path_dikmen = os.path.join(os.getcwd(), base_path,'sekolah-dikmen')

@lru_cache(maxsize=None)
def file_exists(file_path):
    return os.path.exists(file_path)


base_url = f'https://referensi.data.kemdikbud.go.id/pendidikan/'
tingkats = ['dikdas', 'dikmen']

def crawl_sekolah(tingkat):
    # Iterate over all kode_wilayah
    for kode_wilayah in kode_wilayahs:
        kode = kode_wilayah[0].strip()

        # Construct the URL
        url= f'{base_url}{tingkat}/{kode}/3'

        # Construct the filename and file path
        
        filename = f"{kode}.json"
        filename = filename.replace(" ", "")  # Remove spaces
        dir_path =  os.path.join(os.getcwd(), base_path,tingkat)
        file_path = os.path.join(dir_path, filename)

        if file_exists(file_path):
            print(f"skip data already exist: {file_path}")
            continue # skip the rest of the code in the loop

        # print the url
        print('Fetching data from: ',url)

        # Go to the page that we want to scrape
        driver.get(url)

        # Get the page source and create a BeautifulSoup object
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Find the div with the number of entries
        entries_div = soup.find('div', {'id': 'table1_info'})

        # Extract the number of entries from the div's text
        entries_text = entries_div.get_text()
        max_entries = entries_text.split()[-2]

        # Find the dropdown element by its name
        dropdown = driver.find_element(By.NAME, 'table1_length')

        # Add an option to the dropdown with the maximum number of entries
        driver.execute_script(f"arguments[0].add(new Option('{max_entries}', '{max_entries}'));", dropdown)

        # Create a Select object
        select = Select(dropdown)

        # Select the new option by its value
        select.select_by_value(max_entries)

        # Trigger the change event
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", dropdown)

        # Wait for the JavaScript to load the new data
        wait = WebDriverWait(driver, 10)  # Wait up to 10 seconds
        wait.until(EC.presence_of_element_located((By.ID, 'table1')))

        # Get the updated page source and create a new BeautifulSoup object
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Find the table
        table = soup.find('table', {'id': 'table1'})

        # Extract the table data
        data = []

        # Check if the table was found
        if table is not None:
            # Extract the table data
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])
            # Now 'data' contains the table data
            # print(data)
        else:
            print("Table with class 'dataTable' was not found")

        # Print the data
        print(tabulate(data, headers="firstrow", tablefmt="grid"))

        # Save the data to a JSON file
        print(f"Save data to: {file_path}")
        with open(file_path, 'w') as f:
            json.dump(data, f)



# Start crawling from the top level
for tingkat in tingkats:
    crawl_sekolah(tingkat)

# Close the browser
driver.quit()