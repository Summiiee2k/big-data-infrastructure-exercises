import os
from typing import Annotated
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
import pandas as pd
import json


from fastapi import APIRouter, status, HTTPException
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    
    # TODO Implement download
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
    os.makedirs(download_dir, exist_ok=True)

    try:
        # Fetch the list of files from the URL mentioned
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching file list: {e}")

    
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    file_links = [link.get('href') for link in links if link.get('href').endswith('.json.gz')]

    if not file_links:
        raise HTTPException(status_code=404, detail="No JSON files found at the specified URL.")

    # Limiting!
    files_to_download = file_links[:file_limit]

    for file_name in tqdm(files_to_download, desc="Downloading files"):
        file_url = base_url + file_name
        file_path = os.path.join(download_dir, file_name)

        try:
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except requests.RequestException as e:
            raise HTTPException(status_code=500, detail=f"Error downloading {file_name}: {e}")

    return "Download completed successfully."


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """
    Prepares the raw aircraft data by processing it and saving the processed data
    into the prepared data directory.
    """
    # Define the exact paths
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    # Clean the prepared folder before writing again
    if os.path.exists(prepared_dir):
        for file in os.listdir(prepared_dir):
            os.remove(os.path.join(prepared_dir, file))
    else:
        os.makedirs(prepared_dir)

    # Process each raw file
    for file_name in os.listdir(raw_dir):
        print(f"Processing file: {file_name}")
        raw_file_path = os.path.join(raw_dir, file_name)
        prepared_file_path = os.path.join(prepared_dir, file_name.replace(".json.gz", ".json"))

        try:
            # Open and read the raw file
            with open(raw_file_path, "r", encoding="utf-8") as raw_file:
                data = json.load(raw_file)
                timestamp = data.get("now", None)
                aircraft_data = data.get("aircraft", [])
                
                # Extract and process relevant fields
                processed_data = []
                for record in aircraft_data:
                    processed_data.append({
                        "icao": record.get("hex"),
                        "registration": record.get("r"),
                        "type": record.get("t"),
                        "lat": record.get("lat"),
                        "lon": record.get("lon"),
                        "alt_baro": record.get("alt_baro"),
                        "timestamp": timestamp,
                        "max_altitude_baro": record.get("alt_baro"),
                        "max_ground_speed": record.get("gs"),
                        "had_emergency": record.get("alert", 0) == 1,
                    })

                # Save the processed data
                with open(prepared_file_path, "w", encoding="utf-8") as prepared_file:
                    json.dump(processed_data, prepared_file)
                print(f"Processed and saved: {prepared_file_path}")

        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error processing file {file_name}: {e}")
            continue

    return f"Prepared data saved to {prepared_dir}"

PREPARED_DATA_PATH = os.path.join(settings.prepared_dir, "day=20231101")
@s1.get("/aircraft/")
def list_aircraft() -> list:
    """
    Returns a list of all unique aircraft (ICAO codes) from the prepared data.
    """
    aircraft_set = set()

    
    for file_name in os.listdir(PREPARED_DATA_PATH):
        prepared_file_path = os.path.join(PREPARED_DATA_PATH, file_name)

        with open(prepared_file_path, "r", encoding="utf-8") as prepared_file:
            data = json.load(prepared_file)
            for record in data:
                if record["icao"]:  
                    aircraft_set.add(record["icao"])

    return sorted(aircraft_set)

@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str) -> dict:
    """
    Returns the latest known position (latitude, longitude, altitude) of the aircraft with the given ICAO code.
    """
    latest_position = None

    
    for file_name in os.listdir(PREPARED_DATA_PATH):
        prepared_file_path = os.path.join(PREPARED_DATA_PATH, file_name)

        with open(prepared_file_path, "r", encoding="utf-8") as prepared_file:
            data = json.load(prepared_file)
            for record in data:
                if record["icao"] == icao:
                    
                    latest_position = {
                        "lat": record["lat"],
                        "lon": record["lon"],
                        "alt_baro": record["alt_baro"]
                    }

    return latest_position



@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics() -> dict:
    """
    Returns statistics about the aircraft data, such as the max altitude, max speed, and emergency counts.
    """
    max_altitude = float("-inf")
    max_speed = float("-inf")
    emergency_count = 0


    for file_name in os.listdir(PREPARED_DATA_PATH):
        prepared_file_path = os.path.join(PREPARED_DATA_PATH, file_name)

        with open(prepared_file_path, "r", encoding="utf-8") as prepared_file:
            data = json.load(prepared_file)
            for record in data:
                if record["alt_baro"] is not None:
                    max_altitude = max(max_altitude, record["alt_baro"])
                if record["max_ground_speed"] is not None:
                    max_speed = max(max_speed, record["max_ground_speed"])
                if record["had_emergency"]:
                    emergency_count += 1

    return {
        "max_altitude": max_altitude if max_altitude != float("-inf") else None,
        "max_speed": max_speed if max_speed != float("-inf") else None,
        "emergency_count": emergency_count
    }