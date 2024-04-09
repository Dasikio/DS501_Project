import pandas as pd

#Retrieve both parts of data set
train_data = pd.read_csv("C:/Users/danie/Desktop/UK_Code/DS501/Project/california_house_prices/train.csv")
test_data = pd.read_csv("C:/Users/danie/Desktop/UK_Code/DS501/Project/california_house_prices/test.csv")

print(f"Train data:\n{train_data}")
print(f"Test data:\n{test_data}")

#Join both sets to have complete set
full_data = pd.concat([train_data, test_data], axis = 0)
print(f"Full data:\n{full_data}")

#Clean rows with empty data
full_data = full_data.dropna(subset=['Address', 'Bedrooms'])
print(f"Full data:\n{full_data}")