import pandas as pd
from dagster import asset

@asset
def master_socioeconomic_data(cleansed_renta_media, cleansed_ocupacion, cleansed_renta_ingresos):
    """
    Crea el dataset maestro unificando renta, ocupación e ingresos.
    Evita la duplicación de datos mediante el pivotado de categorías.
    """

    df_ocup_pivot = cleansed_ocupacion.pivot_table(
        index=['section_id', 'año', 'municipio'],
        columns='ocupacion',
        values='num_casos',
        aggfunc='sum'
    ).reset_index()

    df_ingresos_pivot = cleansed_renta_ingresos.pivot_table(
        index=['section_id', 'año'],
        columns='MEDIDAS_CODE', 
        values='OBS_VALUE',
        aggfunc='first'
    ).reset_index()

    master = pd.merge(
        cleansed_renta_media[['section_id', 'año', 'municipio', 'OBS_VALUE']], 
        df_ocup_pivot.drop(columns=['municipio'], errors='ignore'), 
        on=['section_id', 'año'], 
        how='left'
    )
    
    master = master.rename(columns={'OBS_VALUE': 'renta_bruta_media'})

    master = pd.merge(
        master,
        df_ingresos_pivot,
        on=['section_id', 'año'],
        how='left'
    )

    columnas_ocup = df_ocup_pivot.columns.drop(['section_id', 'año', 'municipio'])
    master[columnas_ocup] = master[columnas_ocup].fillna(0)

    return master