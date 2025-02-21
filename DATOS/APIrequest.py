import requests
import json
from datetime import datetime
import math
from dotenv import load_dotenv
import os

load_dotenv()

# Idealista API configuration
APIKEY = os.getenv("APIKEY")
SECRET = os.getenv("SECRET")
AUTH_URL = "https://api.idealista.com/oauth/token"
SEARCH_URL = "https://api.idealista.com/3.5/es/search"


# Function to obtain the access token
def get_access_token():
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": APIKEY,
        "client_secret": SECRET,
        "scope": "read",
    }
    response = requests.post(AUTH_URL, headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]


# Function to search for properties
def search_properties(access_token, center, property_type, distance, operation):
    headers = {"Authorization": f"Bearer {access_token}"}
    data = {
        "center": center,
        "propertyType": property_type,
        "distance": distance,
        "operation": operation,
    }
    response = requests.post(SEARCH_URL, headers=headers, data=data)
    response.raise_for_status()
    return response.json()


# Function to save data to a local JSON file
def save_json_local(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Saved data to {filename}")


# Function to calculate new coordinates from an initial point, distance, and angle
def calculate_new_coordinate(lat, lon, distance_km, angle_degrees):
    R = 6371.0  # Earth's radius in km
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    angle_rad = math.radians(angle_degrees)
    new_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(distance_km / R)
        + math.cos(lat_rad) * math.sin(distance_km / R) * math.cos(angle_rad)
    )
    new_lon_rad = lon_rad + math.atan2(
        math.sin(angle_rad) * math.sin(distance_km / R) * math.cos(lat_rad),
        math.cos(distance_km / R) - math.sin(lat_rad) * math.sin(new_lat_rad),
    )
    new_lat = math.degrees(new_lat_rad)
    new_lon = math.degrees(new_lon_rad)
    return new_lat, new_lon


# Main function to perform requests in a grid
def main():
    """
    Main function to search for properties in a grid pattern around an initial center point.
    This function performs the following steps:
    1. Retrieves an access token for authentication.
    2. Defines the initial center coordinates and grid parameters.
    3. Iterates over a grid of points, calculating new coordinates for each point.
    4. Searches for properties around each grid point within a specified distance.
    5. Saves the search results to a local JSON file.
    The grid is defined by the number of rows and columns, and the distance between points is calculated based on a diagonal distance of 2 km.
    Raises:
        Exception: If there is an error during the property search or saving process, it will be caught and printed.
    Note:
        The initial center coordinates and other parameters can be adjusted as needed.
    """
    access_token = get_access_token()

    # Initial center
    # initial_center = [40.378685, -3.702]
    # initial_center = [40.279844, -3.820807]
    initial_center = [40.350367, -3.573238]

    # Distance between points in the grid (diagonal of 2 km, each side is sqrt(2) km)
    distance_km = math.sqrt(2)

    # Grid size (e.g., 9x5)
    rows = 9
    cols = 5

    property_type = "homes"
    operation = "sale"
    distance = "1000"  # 1 km

    for row in range(rows):
        for col in range(cols):
            # Calculate displacements
            dx_km = col * distance_km
            dy_km = row * distance_km

            # Calculate new coordinates
            new_lat, new_lon = calculate_new_coordinate(
                initial_center[0], initial_center[1], dx_km, 90
            )
            new_lat, new_lon = calculate_new_coordinate(new_lat, new_lon, dy_km, 0)

            center = f"{new_lat},{new_lon}"

            try:
                properties_data = search_properties(
                    access_token, center, property_type, distance, operation
                )
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"properties_{center.replace(',', '_')}_{distance}_{property_type}_{operation}_{timestamp}.json"
                save_json_local(properties_data, filename)

            except Exception as e:
                print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
