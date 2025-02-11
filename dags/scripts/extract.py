import requests
import json
import os
import boto3


def get_coordinate(province: str, api_key: str) -> float:
    """
    Get latitude and longitude of a specific province.
    
    Parameter:
        province: Province name.
        api_key: API key.
        
    Return:
        lat: Latitude of location.
        lon: Longitude of location.
    """
    
    try:
        coordinate_url = f'http://api.openweathermap.org/geo/1.0/direct?q={province},TH&limit=1&appid={api_key}'
        coordinate_response = requests.get(coordinate_url)
        
        # return error if not success
        coordinate_response.raise_for_status()

        return {
            'lat': coordinate_response.json()[0]['lat'],
            'lon': coordinate_response.json()[0]['lon']
        }
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f'Coordinate request failed: {e}') from e
    

def get_air_pollution(lat: float, lon: float, start: int, end: int, api_key: str) -> dict:
    """
    Get air polltion data as json format.
    
    Parameter:
        lat: Latitude of a province.
        lon: Longitude of a province.
        start: Start timestamp (Unix time).
        end: End timestamp (Unix time).
        api_key: API key.
        
    Return:
        Air pollution data.
    """
    
    try:
        air_pollution_url = f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'
        air_pollution_response = requests.get(air_pollution_url)
        
        # return error if not success
        air_pollution_response.raise_for_status()
        
        return air_pollution_response.json()
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f'Air pollution request failed: {e}') from e
    

def extract_data(province_list: list, start: int, end: int) -> None:
    """
    Get air pollution data from each province and write to file.

    Parameter:
        province_list: List of all provinces.
        start: Start timestamp (Unix time).
        end: End timestamp (Unix time).

    Return:
        None.
    """

    # define variable
    api_key = os.getenv('API_KEY')
    bucket = os.getenv('RAW_BUCKET')
    s3 = boto3.client('s3')
    counter = 0
    
    for p in province_list:
        
        # get coordinates of current province
        lat = get_coordinate(p, api_key)['lat']
        lon = get_coordinate(p, api_key)['lon']
        
        # get air pollution data
        air_pollution = get_air_pollution(lat, lon, start, end, api_key)
        
        # write output data
        s3.put_object(
            Body=json.dumps(air_pollution),
            Bucket=bucket,
            Key=f'air_quality/{p}.json'
        )

        counter += 1

    print(f'Successfully wrote {counter} files into S3 bucket.')


def file_check(province_list: list) -> None:
    """
    Check each province file in S3 bucket after data extraction that it exists or not.

    Parameter:
        province_list: List of all provinces.

    Return:
        None.
    """
    
    # define variables
    bucket = os.getenv('RAW_BUCKET')
    file_counter = 0
    missing = []

    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket, Prefix='air_quality')

    # check if contents in s3 location exist
    if 'Contents' in objects:
    
        # loop through each province to check in S3 bucket
        for province in province_list:
            
            # indicator
            found = False
            
            # for each object in specified S3 location
            for obj in objects['Contents']:
                
                # compare province name with object name
                if province in obj['Key']:
                    found = True
                    file_counter += 1
                    break
            
            # if object not found
            if found == False:
                missing.append(province)

    else:
        raise ValueError('No contents in the bucket.')
        
    if file_counter == len(province_list):
        print(f'Validation passed, found all {file_counter} files in the bucket.')
        
    else:
        raise ValueError(f'Failed, {len(missing)} missing: {missing}')