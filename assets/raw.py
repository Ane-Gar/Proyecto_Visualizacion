import pandas as pd
import json
from dagster import asset

@asset
def raw_renta_media():
    """Carga los datos de renta media y mediana por secciones"""
    # Asegúrate de que el nombre del archivo coincide con el que subiste
    return pd.read_csv("rentamedia-sc-3.csv")

@asset
def raw_ocupacion():
    """Carga los datos de ocupación por sección y sexo"""
    return pd.read_csv("ocupacion-sc-3.csv")

@asset
def raw_renta_ingresos():
    """Carga la distribución de renta según fuente de ingresos"""
    return pd.read_csv("distribucion-renta-ingresos.csv")

@asset
def raw_geojsons():
    """Carga los archivos cartográficos en un diccionario por año"""
    years = ["2021", "2022", "2023","2024"]
    geo_data = {}
    for year in years:
        # Aquí cargamos cada JSON. Asegúrate de que los archivos estén 
        # en la misma carpeta donde ejecutas Dagster
        file_name = f"secciones_{year}0101_tenerife.json"
        with open(file_name, "r", encoding='utf-8') as f:
            geo_data[year] = json.load(f)
    return geo_data