import pandas as pd
import requests

# Function to get geocode (longitude and latitude) from address
def get_geocode(address, city, zip_code, state):
    # Format the address for the API request
    formatted_address = f"{address.replace(' ', '+')},+{city.replace(' ', '+')},+{state}+{zip_code}"
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

# Clean rows with empty data for Address and Bedrooms
full_data = full_data.dropna(subset=['Address', 'Bedrooms'])

# Add longitude and latitude columns
full_data['Latitude'], full_data['Longitude'] = zip(*full_data.apply(lambda row: get_geocode(row['Address'], row['City'], row['Zip'], row['State']), axis=1))

county_names = {
    '037': 'Los Angeles',
    '073': 'San Diego',
    '059': 'Orange',
    '065': 'Riverside',
    '071': 'San Bernardino'
}

# Add county column
full_data['County'] = full_data.apply(lambda row: get_county(row['Latitude'], row['Longitude']), axis=1)

# Check final data
print(f"Full data with geocode and county:\n{full_data}")
