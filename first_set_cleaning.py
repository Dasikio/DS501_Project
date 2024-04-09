import pandas as pd

#Retrieve data from csv
coordinate_based_data = pd.read_csv("file_address")

#Clean rows with empty data for Address and Bedrooms
cleaned_coordinate_based_data = coordinate_based_data.dropna(subset=['longitude', 'latitude'])

#Check data
print(f"Full data:\n{cleaned_coordinate_based_data}")

