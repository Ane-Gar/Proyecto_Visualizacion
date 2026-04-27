import pandas as pd
from dagster import asset

# Regex: Municipio(5) + Distrito(2) + Sección(3)
SECTION_REGEX = r'.*_(\d{5})_D(\d{2})_S(\d{3})'

@asset
def cleansed_renta_media(raw_renta_media):
    df = raw_renta_media.copy()
    
    extracted = df['TERRITORIO_CODE'].str.extract(SECTION_REGEX)
    df['section_id'] = extracted[0] + extracted[1] + extracted[2]
    
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    return df[(df['MEDIDAS_CODE'] == 'RENTA_BRUTA_MEDIA_PERSONA') & (df['section_id'].notna())]

@asset
def cleansed_ocupacion(raw_ocupacion):
    df = raw_ocupacion.copy()
    extracted = df['geocode'].str.extract(SECTION_REGEX)
    df['section_id'] = extracted[0] + extracted[1] + extracted[2]
    
    df['num_casos'] = pd.to_numeric(df['num_casos'], errors='coerce').fillna(0)
    return df[df['section_id'].notna()].groupby(['año', 'section_id', 'ocupacion', 'municipio'])['num_casos'].sum().reset_index()

@asset
def cleansed_renta_ingresos(raw_renta_ingresos):
    df = raw_renta_ingresos.copy()
    extracted = df['TERRITORIO_CODE'].str.extract(SECTION_REGEX)
    df['section_id'] = extracted[0] + extracted[1] + extracted[2]
    
    if df['OBS_VALUE'].dtype == object:
        df['OBS_VALUE'] = df['OBS_VALUE'].str.replace(',', '.').astype(float)
        
    df = df[df['section_id'].str.startswith('38', na=False)]
    
    return df[df['section_id'].notna()]