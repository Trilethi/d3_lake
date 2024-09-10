import requests
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
from time import sleep, time, strftime
from credentials import GMAP_API_KEY, GS2Q_TOKEN_URL, GS2Q_DATA_URL, GS2Q_USERNAME, GS2Q_PASSWORD, CENSUS_API_KEY, CENSUS_URL
from mi_fips_data import data as fips_data  # Importing the FIPS data

STATE_FIPS = '26'  # Michigan

# Age groups for census data
variables = {
    '0-3': 'B01001_003E,B01001_027E',
    '3-5': 'B01001_004E,B01001_028E',
    '6-11': 'B01001_005E,B01001_006E,B01001_029E,B01001_030E'
}

# Function to fetch the access token
def get_access_token():
    payload = {
        'grant_type': 'password',
        'username': GS2Q_USERNAME,
        'password': GS2Q_PASSWORD
    }
    response = requests.post(GS2Q_TOKEN_URL, data=payload)
    response.raise_for_status()
    token_data = response.json()
    access_token = token_data.get('access_token')
    expires_in = token_data.get('expires_in')  # Typically in seconds
    if not access_token:
        raise ValueError("Failed to obtain access token")
    return access_token, time() + expires_in - 60  # Subtract 60 seconds as a buffer

def get_valid_access_token():
    global access_token, token_expiry
    if time() >= token_expiry:
        access_token, token_expiry = get_access_token()
    return access_token

def fetch_data():
    headers = {
        'Authorization': f'Bearer {get_valid_access_token()}'
    }
    response = requests.get(GS2Q_DATA_URL, headers=headers)
    response.raise_for_status()
    data = response.json()
    if 'value' not in data:
        raise KeyError("'value' key not found in the JSON response")
    return data['value']

def clean_address(address):
    return re.sub(' +', ' ', address.strip())

def geocode_address(address, api_key):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={requests.utils.quote(address)}&key={api_key}"
    for _ in range(3):  # Retry up to 3 times
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'OK' and data['results']:
                    location = data['results'][0]['geometry']['location']
                    print(f"Geocoded {address} -> {location['lat']}, {location['lng']}")  # Logging
                    return location['lat'], location['lng']
                else:
                    print(f"Failed to geocode {address}: {data['status']}")  # Logging
            else:
                print(f"Failed to geocode {address}: HTTP {response.status_code}")  # Logging
        except requests.RequestException as e:
            print(f"Error geocoding address '{address}': {e}")
            sleep(1)  # Wait a second before retrying
    return None, None

def geocode_addresses_concurrently(df, api_key):
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda addr: geocode_address(addr, api_key), df['Full Address']))
    return results

def fetch_census_data(state_fips):
    params = {
        'get': ','.join(variables.values()),
        'for': 'county:*',  # Get data for all counties
        'in': f'state:{state_fips}',
        'key': CENSUS_API_KEY
    }
    response = requests.get(CENSUS_URL, params=params)
    print(f"Fetching data for all counties in state {state_fips}...")
    if response.status_code == 200:
        try:
            data = response.json()
            print(f"Received data for all counties: {data[:5]}...")  # Show a preview of the data
            return data[1:]  # Skip the header row
        except requests.exceptions.JSONDecodeError:
            print(f"Error decoding JSON: {response.text}")
            return None
    else:
        print(f"Error: Received status code {response.status_code}")
        return None

# Use the loaded FIPS data instead of reading from CSV
def get_county_name_from_fips(state_fips, county_fips, fips_data):
    for entry in fips_data:
        if entry['State'] == state_fips and entry['County_Code'] == county_fips:
            return entry['County_Name']
    return 'Unknown County'

def get_place_name_from_fips(state_fips, place_fips, fips_data):
    for entry in fips_data:
        if entry['State'] == state_fips and entry['Place_Code'] == place_fips:
            return entry['Place_Name']
    return 'Unknown City/Town'

def process_census_data_with_names(data, fips_data):
    try:
        # Summing the children counts for various age groups
        children_0_3 = sum(int(data[i]) for i in range(2))
        children_3_5 = sum(int(data[i]) for i in range(2, 4))  # Sum for 3-5 years
        children_6_11 = sum(int(data[i]) for i in range(4, 8))  # Sum for 6-11 years

        # FIPS codes are usually in the last columns for county and place (city)
        county_fips = data[-2]  # Second-to-last column is the county FIPS
        place_fips = data[-1]  # Last column is the place FIPS

        # Get the human-readable names from the FIPS codes
        county_name = get_county_name_from_fips(STATE_FIPS, county_fips, fips_data)
        place_name = get_place_name_from_fips(STATE_FIPS, place_fips, fips_data)

        print(
            f"Processed data: County: {county_name}, City: {place_name}, 0-3: {children_0_3}, 3-5: {children_3_5}, 6-11: {children_6_11}")
        return county_name, place_name, children_0_3, children_3_5, children_6_11
    except (ValueError, IndexError) as e:
        print(f"Error processing data: {e}")
        return 'Unknown County', 'Unknown City/Town', 0, 0, 0

def process_data(api_key):
    global access_token, token_expiry
    access_token, token_expiry = get_access_token()

    # Fetch provider data
    provider_data = fetch_data()
    df = pd.DataFrame(provider_data)

    # Ensure the required columns exist
    required_columns = {'AddressLine1', 'City', 'ZipCode'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

    # Create a full address column for geolocation
    df['Full Address'] = df.apply(lambda row: f"{row['AddressLine1']}, {row['City']}, Michigan {row['ZipCode']}",
                                  axis=1)
    df['Full Address'] = df['Full Address'].apply(clean_address)

    # Geocode addresses concurrently
    geocode_results = geocode_addresses_concurrently(df, api_key)
    df['Latitude'], df['Longitude'] = zip(*geocode_results)

    # Fetch census data for all counties and cities in Michigan
    census_data = fetch_census_data(STATE_FIPS)

    # Process census data and map to names
    census_results = []
    if census_data:
        for row in census_data:
            county_name, place_name, children_0_3, children_3_5, children_6_11 = process_census_data_with_names(row, fips_data)
            census_results.append({
                'county': county_name,
                'city': place_name,
                'children_0_3': children_0_3,
                'children_3_5': children_3_5,
                'children_6_11': children_6_11
            })

    # Convert census results to DataFrame
    census_df = pd.DataFrame(census_results)

    # Timestamp for file saving
    timestamp = strftime("%Y%m%d_%H%M%S")

    # Save provider and census data
    df.to_csv(f'processed_providers_{timestamp}.csv', index=False)
    census_df.to_csv(f'processed_census_{timestamp}.csv', index=False)

    print(f"Data processing complete. Provider data saved to 'processed_providers_{timestamp}.csv'.")
    print(f"Census data saved to 'processed_census_{timestamp}.csv'.")

if __name__ == '__main__':
    process_data(GMAP_API_KEY)
