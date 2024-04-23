import pandas as pd
import requests
import time

# Function to get county from geocode
def get_county(latitude, longitude):
    try:
        geocode_api = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={longitude}&y={latitude}&benchmark=Public_AR_Census2020&vintage=Census2020_Census2020&layers=6&format=json"
        response = requests.get(geocode_api)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        geocode_api_data = response.json()
        county_code = geocode_api_data['result']['geographies']['Census Tracts'][0]['COUNTY']
        return county_code
    except (requests.RequestException, KeyError, IndexError) as e:
        print(f"Error occurred for coordinates ({longitude}, {latitude}): {str(e)}")
        return None

#Function to process a dataSet
def complete_set1(file_path):
    coordinate_based_data = pd.read_csv(file_path)
    cleaned_coordinate_based_data = coordinate_based_data.dropna(subset=['longitude', 'latitude'])
    filtered_data = cleaned_coordinate_based_data[(cleaned_coordinate_based_data['longitude'] > -118.96) &
                                                  (cleaned_coordinate_based_data['latitude'] < 35.82)]

    duplicate_coords = filtered_data.groupby(['longitude', 'latitude']).size() > 1
    for coords, is_duplicate in duplicate_coords.items():
        if is_duplicate:
            avg_values = filtered_data.loc[(filtered_data['longitude'] == coords[0]) &
                                           (filtered_data['latitude'] == coords[1])].select_dtypes(include='number').mean()
            for col in avg_values.index:
                filtered_data.loc[(filtered_data['longitude'] == coords[0]) &
                                  (filtered_data['latitude'] == coords[1]), col] = avg_values[col]
    filtered_data.drop_duplicates(subset=['longitude', 'latitude'], keep='first', inplace=True)

    return filtered_data

#Function to filter data from wanted counties
def county_filter(filtered_data, county_names):
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

        # Drop rows with indices from the list and round values
        filtered_data.drop(indices_to_drop, inplace=True)
    return filtered_data


def main(file_path, county_names):
    cleaned_dataset1 = complete_set1(file_path)
    cleaned_dataset1 = county_filter(cleaned_dataset1, county_names)
    return cleaned_dataset1

if __name__ == "__main__":
    #File path to dataSet
    file_path = "local_path/California_House_Info.csv"
    
    # Define County codes wanted
    county_names = {
        '037': 'Los Angeles',
        '073': 'San Diego',
        '059': 'Orange',
        '065': 'Riverside',
        '071': 'San Bernardino'
    }
    #Load and Clean data for merging
    cleaned_dataset1 = main(file_path, county_names)
    cleaned_dataset1.to_csv('cleaned_dataset1.csv', index=False)
    print(cleaned_dataset1)
