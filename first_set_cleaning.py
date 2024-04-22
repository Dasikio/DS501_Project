import pandas as pd
import requests
import time

# Retrieve data from csv
coordinate_based_data = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/DS 501/California_House_Info.csv")

# Clean rows with empty data for Address and Bedrooms
cleaned_coordinate_based_data = coordinate_based_data.dropna(subset=['longitude', 'latitude'])

# Filter the DataFrame based on longitude and latitude conditions
filtered_data = cleaned_coordinate_based_data[(cleaned_coordinate_based_data['longitude'] > -118.96) &
                                              (cleaned_coordinate_based_data['latitude'] < 35.82)]

# Replace duplicate rows with average values for other columns
duplicate_coords = filtered_data.groupby(['longitude', 'latitude']).size() > 1

for coords, is_duplicate in duplicate_coords.items():
    if is_duplicate:
        avg_values = filtered_data.loc[(filtered_data['longitude'] == coords[0]) &
                                       (filtered_data['latitude'] == coords[1])].select_dtypes(include='number').mean()

        # Assign average values to each column separately
        for col in avg_values.index:
            filtered_data.loc[(filtered_data['longitude'] == coords[0]) &
                              (filtered_data['latitude'] == coords[1]), col] = avg_values[col]

# Drop duplicate rows
filtered_data.drop_duplicates(subset=['longitude', 'latitude'], keep='first', inplace=True)

# Dictionary to map county codes to county names
county_names = {
    '037': 'Los Angeles',
    '073': 'San Diego',
    '059': 'Orange',
    '065': 'Riverside',
    '071': 'San Bernardino'
}

# Iterate over rows to populate the 'county' column with county codes
for index, row in filtered_data.iterrows():
    longitude = row['longitude']
    latitude = row['latitude']

    retry_attempts = 3  # Number of retry attempts
    retry_delay = 2      # Delay in seconds between retries

    for attempt in range(1, retry_attempts + 1):
        try:
            # Construct the API endpoint URL for geocoding
            geocode_api = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={longitude}&y={latitude}&benchmark=Public_AR_Census2020&vintage=Census2020_Census2020&layers=6&format=json"

            # Make a GET request to the API endpoint for geocoding
            response = requests.get(geocode_api)
            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes

            # Convert the response to JSON format
            geocode_api_data = response.json()

            # Access the county code information and store it in the 'county' column
            county_code = geocode_api_data['result']['geographies']['Census Tracts'][0]['COUNTY']
            filtered_data.at[index, 'county'] = county_code

            break  # Exit the retry loop if successful
        except (requests.RequestException, KeyError, IndexError) as e:
            print(f"Error occurred for coordinates ({longitude}, {latitude}) on attempt {attempt}: {str(e)}")
            if attempt < retry_attempts:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue  # Retry the request
            else:
                print("Maximum retry attempts reached. Skipping...")
                break  # Exit the retry loop

# Iterate over each county code in the 'county' column
indices_to_drop = []  # List to store indices of rows to drop
for index, county_code in filtered_data['county'].items():
    # Check if the county code exists in the dictionary keys
    if county_code in county_names:
        # Replace the county code with the corresponding county name
        filtered_data.at[index, 'county'] = county_names[county_code]
    else:
        # Add the index to the list of indices to drop
        indices_to_drop.append(index)

# Drop rows with indices from the list
filtered_data.drop(indices_to_drop, inplace=True)

# Save the filtered data to a CSV file
filtered_data.to_csv('/content/drive/MyDrive/Colab Notebooks/DS 501/set1_filtered_data.csv', index=False)

# Read the saved CSV file back into a DataFrame
filtered_data = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/DS 501/set1_filtered_data.csv')

# Print the filtered data to verify
print(filtered_data)