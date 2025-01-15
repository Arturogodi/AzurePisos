import folium
import math


# Función para calcular nueva coordenada a partir de una inicial, distancia y ángulo
def calculate_new_coordinate(lat, lon, distance_km, angle_degrees):
    # Radio de la Tierra en km
    R = 6371.0

    # Convertir ángulos a radianes
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    angle_rad = math.radians(angle_degrees)

    # Calcular nueva latitud
    new_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(distance_km / R)
        + math.cos(lat_rad) * math.sin(distance_km / R) * math.cos(angle_rad)
    )

    # Calcular nueva longitud
    new_lon_rad = lon_rad + math.atan2(
        math.sin(angle_rad) * math.sin(distance_km / R) * math.cos(lat_rad),
        math.cos(distance_km / R) - math.sin(lat_rad) * math.sin(new_lat_rad),
    )

    # Convertir a grados
    new_lat = math.degrees(new_lat_rad)
    new_lon = math.degrees(new_lon_rad)

    return new_lat, new_lon


# Centro inicial
initial_center = [40.378685, -3.702]

# Distancia entre puntos en la malla (diagonal de 2 km, cada lado es sqrt(2) km)
distance_km = math.sqrt(2)

# Tamaño de la malla (ej. 5x5)
rows = 9
cols = 5

# Crear un mapa
m = folium.Map(location=initial_center, zoom_start=14)

# Generar y añadir círculos de la malla al mapa
for row in range(rows):
    for col in range(cols):
        # Calcular desplazamientos
        dx_km = col * distance_km
        dy_km = row * distance_km

        # Calcular nueva coordenada
        new_lat, new_lon = calculate_new_coordinate(
            initial_center[0], initial_center[1], dx_km, 90
        )
        new_lat, new_lon = calculate_new_coordinate(new_lat, new_lon, dy_km, 0)

        # Añadir círculo al mapa
        folium.Circle(
            location=[new_lat, new_lon],
            radius=1000,  # Radio de 1 km
            color="blue",
            fill=True,
            fill_opacity=0.2,
        ).add_to(m)

        # Añadir marcador para el centro del círculo
        folium.Marker(
            location=[new_lat, new_lon], icon=folium.Icon(color="red")
        ).add_to(m)

# Guardar el mapa en un archivo HTML
m.save("malla_simulacion.html")
