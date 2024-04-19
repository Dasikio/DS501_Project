import pandas as pd
import requests

# Function to get geocode (longitude and latitude) from address
def get_geocode(address, city, zip_code, state):
    # Format the address for the API request
    formatted_address = f"{address.replace(' ', '+')},+{city.replace(' ', '+')},+{state}+{zip_code}"
    formatted_address = formatted_address.replace('#','')
    geocoding_url = f"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address={formatted_address}&benchmark=2020&format=json"
    
    try:
        response = requests.get(geocoding_url)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        geocoding_data = response.json()
        
        # Extract latitude and longitude from the API response
        if 'result' in geocoding_data and 'addressMatches' in geocoding_data['result'] and len(geocoding_data['result']['addressMatches']) > 0:
            latitude = geocoding_data['result']['addressMatches'][0]['coordinates']['y']
            longitude = geocoding_data['result']['addressMatches'][0]['coordinates']['x']
            return latitude, longitude
        else:
            return None, None
    except requests.RequestException as e:
        print(f"Error occurred while getting geocode for address {formatted_address}: {e}")
        return None, None

# Function to get county from geocode
def get_county(latitude, longitude):
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
        return county_code

    except (requests.RequestException, KeyError, IndexError) as e:
        print(f"Error occurred for coordinates ({longitude}, {latitude}): {str(e)}")
        return None

# Retrieve both parts of data set
train_data = pd.read_csv("train.csv")
test_data = pd.read_csv("test.csv")

# Join both sets to have complete set
full_data = pd.concat([train_data, test_data], axis=0)


#-----------------------------------------------------------
full_data = full_data.head(500) #Restrict data for testing
#-----------------------------------------------------------


# Clean rows with empty data for Address and Bedrooms
full_data = full_data.dropna(subset=['Address', 'Bedrooms'])

# Add longitude and latitude columns
full_data['latitude'], full_data['longitude'] = zip(*full_data.apply(lambda row: get_geocode(row['Address'], row['City'], row['Zip'], row['State']), axis=1))

#Define County codes for data cleaning
county_names = {
    '037': 'Los Angeles',
    '073': 'San Diego',
    '059': 'Orange',
    '065': 'Riverside',
    '071': 'San Bernardino'
}

# Add county column
full_data['county'] = full_data.apply(lambda row: get_county(row['latitude'], row['longitude']), axis=1)

filtered_set2_data = full_data.copy()

# Iterate over each county code in the 'county' column
indices_to_drop = []  # List to store indices of rows to drop
for index, county_code in full_data['county'].items():
    # Check if the county code exists in the dictionary keys
    if county_code in county_names:
        # Replace the county code with the corresponding county name
        full_data.at[index, 'county'] = county_names[county_code]
    else:
        # Add the index to the list of indices to drop
        indices_to_drop.append(index)

# Drop rows with indices from the list and round values
full_data.drop(indices_to_drop, inplace=True)
full_data['latitude'] = full_data['latitude'].round(2)
full_data['longitude'] = full_data['longitude'].round(2)

full_data_backup = full_data.copy()

#Agregate data for merging
# Define aggregation functions for each column
aggregation_functions = {
    'Sold Price': 'mean',  # Average price of homes
    'Year built': 'mean',
    'Bedrooms': 'sum',  # Total number of bedrooms
    'Bathrooms': 'sum',
    'Full bathrooms': 'sum',
    'Total interior livable area': 'mean',
    'Elementary School Distance': 'mean',
    'Middle School Distance': 'mean',
    'High School Distance': 'mean',
    'Last Sold Price': 'mean'  # Average distance to schools
}

# Group by longitude and latitude columns and apply aggregation functions
mod_full_data = full_data.groupby(['longitude', 'latitude']).agg(aggregation_functions).reset_index()

#-----------------------------------------------
#add command to save csv of completed processed data set
#-----------------------------------------------


print(mod_full_data)