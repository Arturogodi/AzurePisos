import folium
import math


# Function to calculate new coordinate from an initial point, distance, and angle
def calculate_new_coordinate(lat, lon, distance_km, angle_degrees):
    """
    Calculate the new geographic coordinate given an initial coordinate, distance, and angle.
    Args:
        lat (float): Initial latitude in degrees.
        lon (float): Initial longitude in degrees.
        distance_km (float): Distance to travel from the initial coordinate in kilometers.
        angle_degrees (float): Angle in degrees from the initial coordinate (clockwise from north).
    Returns:
        tuple: A tuple containing the new latitude and longitude in degrees.
    """
    # Radius of the Earth in km
    R = 6371.0

    # Convert angles to radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    angle_rad = math.radians(angle_degrees)

    # Calculate new latitude
    new_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(distance_km / R)
        + math.cos(lat_rad) * math.sin(distance_km / R) * math.cos(angle_rad)
    )

    # Calculate new longitude
    new_lon_rad = lon_rad + math.atan2(
        math.sin(angle_rad) * math.sin(distance_km / R) * math.cos(lat_rad),
        math.cos(distance_km / R) - math.sin(lat_rad) * math.sin(new_lat_rad),
    )

    # Convert to degrees
    new_lat = math.degrees(new_lat_rad)
    new_lon = math.degrees(new_lon_rad)

    return new_lat, new_lon


# Initial center
initial_center = [40.378685, -3.702]

# Distance between points in the grid (diagonal of 2 km, each side is sqrt(2) km)
distance_km = math.sqrt(2)

# Grid size (e.g., 5x5)
rows = 9
cols = 5

# Create a map
m = folium.Map(location=initial_center, zoom_start=14)

# Generate and add circles of the grid to the map
for row in range(rows):
    for col in range(cols):
        # Calculate displacements
        dx_km = col * distance_km
        dy_km = row * distance_km

        # Calculate new coordinate
        new_lat, new_lon = calculate_new_coordinate(
            initial_center[0], initial_center[1], dx_km, 90
        )
        new_lat, new_lon = calculate_new_coordinate(new_lat, new_lon, dy_km, 0)

        # Add circle to the map
        folium.Circle(
            location=[new_lat, new_lon],
            radius=1000,  # Radius of 1 km
            color="blue",
            fill=True,
            fill_opacity=0.2,
        ).add_to(m)

        # Add marker for the center of the circle
        folium.Marker(
            location=[new_lat, new_lon], icon=folium.Icon(color="red")
        ).add_to(m)

# Save the map to an HTML file
m.save("malla_simulacion.html")
