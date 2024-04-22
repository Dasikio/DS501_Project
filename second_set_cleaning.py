import pandas as pd
import requests

# Function to get geocode (longitude and latitude) from address
def get_coordinates(address, city, zip_code, state):
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

#Function to filter data from wanted counties
def county_filter(full_data,county_names):
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
    return full_data

#Function to aggregate desired columns
def data_aggregation(full_data,aggregation_functions):
    mod_full_data = full_data.groupby(['longitude', 'latitude']).agg(aggregation_functions).reset_index()


#Function to process two or more subsets of a dataSet
def complete_set2(susbset_list):
    full_data = pd.DataFrame()
    for idx, file_name in enumerate(susbset_list):
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_name)
        if idx == 0:
            full_data = df
        else:
            full_data = pd.concat([full_data, df], ignore_index=True)

    # Clean the DataFrame
    full_data = full_data.dropna(subset=['Address', 'Bedrooms'])

    #Get coordinates (Longitude and Latitude)
    full_data['latitude'], full_data['longitude'] = zip(*full_data.apply(lambda row: get_coordinates(row['Address'], row['City'], row['Zip'], row['State']), axis=1))
    full_data['latitude'] = full_data['latitude'].round(2)
    full_data['longitude'] = full_data['longitude'].round(2)

    #Get counties
    full_data['county'] = full_data.apply(lambda row: get_county(row['latitude'], row['longitude']), axis=1)

    return full_data

def clean_set2(full_data, county_names, aggregation_functions):
    filtered_data = county_filter(full_data,county_names)
    cleaned_dataset2 = data_aggregation(full_data,aggregation_functions)

    return cleaned_dataset2


def main():
    # List of subsets to join for dataSet
    subset_list = ['train.csv', 'test.csv']

    # Define County codes wanted
    county_names = {
        '037': 'Los Angeles',
        '073': 'San Diego',
        '059': 'Orange',
        '065': 'Riverside',
        '071': 'San Bernardino'
    }

    # Agregate data for merging
    # Define aggregation dictionary for desired columns
    aggregation_functions = {
        'Sold Price': 'mean',  # Average price of homes - Use mean to get average of values in same area
        'Year built': 'mean',
        'Bedrooms': 'sum',  # Total number of bedrooms - Use sum to add values in same area
        'Bathrooms': 'sum',
        'Full bathrooms': 'sum',
        'Total interior livable area': 'mean', #Average house area
        'Elementary School Distance': 'mean', # Average distance to schools
        'Middle School Distance': 'mean', # Average distance to schools
        'High School Distance': 'mean', # Average distance to schools
        'Last Sold Price': 'mean'  #Average house price
    }

    # Get complete set 2 (join subsets, find coordinates and counties)
    full_data = complete_set2(subset_list)

    # Copy in case of error
    filtered_set2_data = full_data.copy()

    # Clean data
    cleaned_dataset2 = clean_set2(full_data, county_names)

    return cleaned_dataset2

# Call the main function
if __name__ == "__main__":
    #Modify main with the necesary entries to use this dataSet cleaning framework
    cleaned_dataset2 = main()

    #Save csv of completed processed data set to current directory
    cleaned_dataset2.to_csv('cleaned_dataset2.csv', index=False)
    print(cleaned_dataset2)

#-----------------------------------------------

#-----------------------------------------------
