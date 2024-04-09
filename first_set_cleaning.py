import pandas as pd

#Retrieve data from csv
coordinate_based_data = pd.read_csv("C:/Users/danie/Desktop/UK_Code/DS501/Project/California_House_Information/California_House_Info.csv")

#Clean rows with empty data for Address and Bedrooms
cleaned_coordinate_based_data = coordinate_based_data.dropna(subset=['longitude', 'latitude'])

#Check data
print(f"Full data:\n{cleaned_coordinate_based_data}")

