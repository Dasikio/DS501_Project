import pandas as pd

#Retrieve both parts of data set
train_data = pd.read_csv("data addres")
test_data = pd.read_csv("data addres")

#Check data structure
print(f"Train data:\n{train_data}")
print(f"Test data:\n{test_data}")

#Join both sets to have complete set
full_data = pd.concat([train_data, test_data], axis = 0)

#Clean rows with empty data for Address and Bedrooms
full_data = full_data.dropna(subset=['Address', 'Bedrooms'])

#Check final data
print(f"Full data:\n{full_data}")

