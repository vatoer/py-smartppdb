import pandas as pd

# Prompt the user for the filename
filename = input("Please enter the filename: ")

# Load the data
with open(filename) as f:
    data = pd.read_json(f)

# Print the data
print(data)