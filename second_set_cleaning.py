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
    county_url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={longitude}&y={latitude}&benchmark=Public_AR_Census2020&vintage=Census2020_Census2020&layers=6&format=json"
    county_response = requests.get(county_url)
    county_data = county_response.json()
    if 'result' in county_data and 'geographies' in county_data['result'] and 'Counties' in county_data['result']['geographies'] and len(county_data['result']['geographies']['Counties']) > 0:
        county_name = county_data['result']['geographies']['Counties'][0]['NAME']
        return county_name
    else:
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

# Add county column
full_data['County'] = full_data.apply(lambda row: get_county(row['Latitude'], row['Longitude']), axis=1)

# Check final data
print(f"Full data with geocode and county:\n{full_data}")
