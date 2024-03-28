# pip install beautifulsoup4
# <https://googlechromelabs.github.io/chrome-for-testing/#stable>
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
cur.execute("SELECT kode_wilayah FROM dapo_wilayah WHERE dapo_wilayah.id_level_wilayah = 3 limit 3")

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

# Iterate over all kode_wilayah
for kode_wilayah in kode_wilayahs:
    kode = kode_wilayah[0].strip()
    # Fetch the HTML content
    # dikdas
    url = f'https://referensi.data.kemdikbud.go.id/pendidikan/dikdas/{kode}/3'
    # dikmen
    url = f'https://referensi.data.kemdikbud.go.id/pendidikan/dikmen/{kode}/3'

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

# Close the browser
driver.quit()