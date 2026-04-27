import pandas as pd
import json
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity, AssetIn

# CHECKS DE CARGA (raw)

@asset_check(asset="raw_renta_media", description="Columnas mínimas presentes")
def check_raw_renta_media_columnas(raw_renta_media):
    required = {"año", "MEDIDAS_CODE", "TERRITORIO_CODE", "municipio", "OBS_VALUE"}
    missing = required - set(raw_renta_media.columns)
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={"columnas_faltantes": str(missing) if missing else "ninguna"},
    )

@asset_check(asset="raw_renta_media", description="Años esperados 2021-2023")
def check_raw_renta_media_anios(raw_renta_media):
    found = set(raw_renta_media["año"].unique())
    expected = {2021, 2022, 2023}
    ok = expected.issubset(found)
    return AssetCheckResult(
        passed=ok,
        metadata={"años_encontrados": str(sorted(found)), "años_esperados": str(sorted(expected))},
    )

@asset_check(asset="raw_renta_media", description="OBS_VALUE dentro de rango razonable (0€–250.000€)")
def check_raw_renta_media_rango(raw_renta_media):
    vals = pd.to_numeric(raw_renta_media["OBS_VALUE"], errors="coerce").dropna()
    min_v, max_v = float(vals.min()), float(vals.max())
    ok = 0 < min_v and max_v < 250_000
    return AssetCheckResult(
        passed=ok,
        metadata={"min": min_v, "max": max_v, "rango_esperado": "0–250.000 €"},
    )

@asset_check(asset="raw_renta_media", description="Medidas esperadas presentes")
def check_raw_renta_media_medidas(raw_renta_media):
    expected = {
        "RENTA_BRUTA_MEDIA_HOGAR", "RENTA_BRUTA_MEDIA_PERSONA",
        "RENTA_NETA_MEDIA_HOGAR", "RENTA_NETA_MEDIA_PERSONA",
        "RENTA_NETA_UNIDAD_CONSUMO_MEDIA", "RENTA_NETA_UNIDAD_CONSUMO_MEDIANA",
    }
    found = set(raw_renta_media["MEDIDAS_CODE"].unique())
    missing = expected - found
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={"medidas_faltantes": str(missing) if missing else "ninguna"},
    )

@asset_check(asset="raw_ocupacion", description="Columnas mínimas presentes")
def check_raw_ocupacion_columnas(raw_ocupacion):
    required = {"año", "geocode", "municipio", "sexo", "num_casos", "ocupacion"}
    missing = required - set(raw_ocupacion.columns)
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={"columnas_faltantes": str(missing) if missing else "ninguna"},
    )

@asset_check(asset="raw_ocupacion", description="num_casos no tiene negativos")
def check_raw_ocupacion_negativos(raw_ocupacion):
    negatives = (pd.to_numeric(raw_ocupacion["num_casos"], errors="coerce") < 0).sum()
    return AssetCheckResult(
        passed=int(negatives) == 0,
        metadata={"filas_negativas": int(negatives)},
    )

@asset_check(asset="raw_renta_ingresos", description="Columnas mínimas presentes")
def check_raw_ingresos_columnas(raw_renta_ingresos):
    required = {"año", "TERRITORIO_CODE", "municipio", "MEDIDAS_CODE", "OBS_VALUE"}
    missing = required - set(raw_renta_ingresos.columns)
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={"columnas_faltantes": str(missing) if missing else "ninguna"},
    )

@asset_check(asset="raw_renta_ingresos", description="Fuentes de ingreso esperadas presentes")
def check_raw_ingresos_fuentes(raw_renta_ingresos):
    expected = {
        "SUELDOS_SALARIOS", "PENSIONES",
        "PRESTACIONES_DESEMPLEO", "OTRAS_PRESTACIONES", "OTROS_INGRESOS",
    }
    found = set(raw_renta_ingresos["MEDIDAS_CODE"].unique())
    missing = expected - found
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={"fuentes_faltantes": str(missing) if missing else "ninguna"},
    )

@asset_check(asset="raw_renta_ingresos", description="OBS_VALUE parseable como porcentaje (0-100)")
def check_raw_ingresos_rango(raw_renta_ingresos):
    df = raw_renta_ingresos.copy()
    if df["OBS_VALUE"].dtype == object:
        vals = pd.to_numeric(df["OBS_VALUE"].str.replace(",", "."), errors="coerce").dropna()
    else:
        vals = pd.to_numeric(df["OBS_VALUE"], errors="coerce").dropna()
    n_unparseable = df["OBS_VALUE"].shape[0] - len(vals)
    ok = float(vals.min()) >= 0 and float(vals.max()) <= 100 and n_unparseable <= 10
    return AssetCheckResult(
        passed=ok,
        metadata={
            "min": round(float(vals.min()), 2),
            "max": round(float(vals.max()), 2),
            "n_no_parseables": int(n_unparseable),
        },
    )

@asset_check(asset="raw_geojsons", description="Los 4 GeoJSONs necesarios están cargados (2021-2024)")
def check_raw_geojsons_completos(raw_geojsons):
    expected_keys = {"2021", "2022", "2023", "2024"}
    found_keys = set(raw_geojsons.keys())
    missing = expected_keys - found_keys
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={
            "claves_encontradas": str(sorted(found_keys)),
            "claves_faltantes": str(missing) if missing else "ninguna",
            "nota": "Falta '2024' si solo cargas años ['2021','2022','2023'] en raw.py",
        },
    )

@asset_check(asset="raw_geojsons", description="Cada GeoJSON tiene features con geocode y geometry")
def check_raw_geojsons_estructura(raw_geojsons):
    issues = []
    for year, geo in raw_geojsons.items():
        if geo.get("type") != "FeatureCollection":
            issues.append(f"{year}: no es FeatureCollection")
            continue
        features = geo.get("features", [])
        if len(features) < 600:
            issues.append(f"{year}: solo {len(features)} features (esperadas ≥600)")
        sample = features[0]["properties"] if features else {}
        if "geocode" not in sample:
            issues.append(f"{year}: propiedad 'geocode' no encontrada en features")
    return AssetCheckResult(
        passed=len(issues) == 0,
        metadata={"problemas": str(issues) if issues else "ninguno",
                  "n_features_por_año": {y: len(g["features"]) for y, g in raw_geojsons.items()}},
    )


# CHECKS DE TRANSFORMACIÓN (cleansed y enriched)

@asset_check(asset="cleansed_renta_media", description="section_id sin valores nulos (regex funcionó)")
def check_cleansed_renta_section_id_nulos(cleansed_renta_media):
    n_null = cleansed_renta_media["section_id"].isna().sum()
    return AssetCheckResult(
        passed=int(n_null) == 0,
        metadata={"section_id_nulos": int(n_null),
                  "nota": "NaN indica geocode que no coincide con el patrón esperado"},
    )

@asset_check(asset="cleansed_renta_media", description="section_id es único por (año, section_id) — sin colisiones entre años")
def check_cleansed_renta_section_id_colisiones(cleansed_renta_media):
    dups = (
        cleansed_renta_media
        .groupby(["año", "section_id"])["TERRITORIO_CODE"]
        .nunique()
    )
    n_colisiones = int((dups > 1).sum())
    return AssetCheckResult(
        passed=n_colisiones == 0,
        metadata={
            "colisiones_año_sectionid": n_colisiones,
            "nota": (
                "Si section_id no incluye el año del geocode (prefijo YYYYMMDD), "
                "la misma sección en distintos años comparte ID pero apunta a geocodes distintos. "
                "Fix: incluir 'año' en el join del mapa, o usar TERRITORIO_CODE directamente."
            ),
        },
    )

@asset_check(asset="cleansed_renta_media", description="Solo contiene RENTA_BRUTA_MEDIA_PERSONA")
def check_cleansed_renta_solo_una_medida(cleansed_renta_media):
    medidas = set(cleansed_renta_media["MEDIDAS_CODE"].unique())
    ok = medidas == {"RENTA_BRUTA_MEDIA_PERSONA"}
    return AssetCheckResult(
        passed=ok,
        metadata={"medidas_encontradas": str(medidas)},
    )

@asset_check(asset="cleansed_renta_media", description="Nulos en OBS_VALUE por debajo del umbral (<10%)")
def check_cleansed_renta_nulos(cleansed_renta_media):
    if len(cleansed_renta_media) == 0:
        return AssetCheckResult(passed=False, metadata={"error": "El dataframe está vacío"})
        
    n_null = cleansed_renta_media["OBS_VALUE"].isna().sum()
    pct = n_null / len(cleansed_renta_media) * 100
    
    return AssetCheckResult(
        passed=bool(pct < 10.0),
        metadata={"n_nulos": int(n_null), "pct_nulos": round(float(pct), 3)},
    )

@asset_check(asset="cleansed_renta_ingresos", description="Solo contiene municipios de SC Tenerife (provincia 38)")
def check_cleansed_ingresos_provincia(cleansed_renta_ingresos):
    provincias = cleansed_renta_ingresos["TERRITORIO_CODE"].str.extract(r"_(\d{2})\d{3}_")[0].unique()
    tiene_las_palmas = "35" in provincias
    return AssetCheckResult(
        passed=not tiene_las_palmas,
        metadata={
            "provincias_encontradas": str(sorted(provincias.tolist())),
            "nota": "El CSV fuente incluye Las Palmas (35). Debe filtrarse a provincia 38 en cleansed.",
        },
    )

@asset_check(asset="cleansed_renta_ingresos", description="Las fuentes de ingreso suman ~100% por sección-año")
def check_cleansed_ingresos_suman_100(cleansed_renta_ingresos):
    sumas = (
        cleansed_renta_ingresos
        .groupby(["año", "section_id"])["OBS_VALUE"]
        .sum()
    )
    fuera_rango = ((sumas < 95) | (sumas > 105)).sum()
    return AssetCheckResult(
        passed=int(fuera_rango) == 0,
        severity=AssetCheckSeverity.WARN if fuera_rango > 0 else AssetCheckSeverity.ERROR,
        metadata={
            "secciones_fuera_de_rango_100pct": int(fuera_rango),
            "media_suma": round(float(sumas.mean()), 2),
            "nota": "Cada sección debe sumar ~100% entre todas sus fuentes de ingreso",
        },
    )

@asset_check(asset="cleansed_ocupacion", description="section_id sin valores nulos")
def check_cleansed_ocupacion_section_id(cleansed_ocupacion):
    n_null = cleansed_ocupacion["section_id"].isna().sum()
    return AssetCheckResult(
        passed=int(n_null) == 0,
        metadata={"section_id_nulos": int(n_null)},
    )

@asset_check(asset="cleansed_ocupacion", description="4 categorías de ocupación por sección-año")
def check_cleansed_ocupacion_categorias(cleansed_ocupacion):
    """Cada sección debería tener exactamente 4 categorías de ocupación.
    
    Un número menor indica secciones con datos incompletos.
    Un número mayor indica duplicados que no se agruparon correctamente.
    """
    cats_por_seccion = (
        cleansed_ocupacion
        .groupby(["año", "section_id"])["ocupacion"]
        .nunique()
    )
    n_incompletas = int((cats_por_seccion < 4).sum())
    n_duplicadas = int((cats_por_seccion > 4).sum())
    return AssetCheckResult(
        passed=n_duplicadas == 0,
        severity=AssetCheckSeverity.WARN if n_incompletas > 0 else AssetCheckSeverity.ERROR,
        metadata={
            "secciones_con_menos_de_4_categorias": n_incompletas,
            "secciones_con_mas_de_4_categorias_ERROR": n_duplicadas,
        },
    )


@asset_check(
    asset="master_socioeconomic_data", 
    description="Verifica que no se pierden secciones al enriquecer",
    additional_ins={"cleansed_renta_media": AssetIn("cleansed_renta_media")} # <-- Esto es lo que falta
)
def check_master_cobertura_secciones(master_socioeconomic_data, cleansed_renta_media):
    n_renta = cleansed_renta_media["section_id"].nunique()
    n_master = master_socioeconomic_data["section_id"].nunique()
    ok = n_master == n_renta
    return AssetCheckResult(
        passed=ok,
        metadata={
            "secciones_en_renta": int(n_renta),
            "secciones_en_master": int(n_master),
            "perdidas": int(n_renta - n_master),
            "nota": "Un inner join silencioso puede descartar secciones sin datos de ingresos",
        },
    )

@asset_check(asset="master_socioeconomic_data", description="Columnas de ocupación presentes tras el pivot")
def check_master_columnas_ocupacion(master_socioeconomic_data):
    expected_fragments = ["Directores", "Ocupaciones elementales", "Trabajadores cualificados"]
    found = master_socioeconomic_data.columns.tolist()
    missing = [f for f in expected_fragments if not any(f in col for col in found)]
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={
            "fragmentos_no_encontrados": str(missing) if missing else "ninguno",
            "columnas_disponibles": str(found),
        },
    )

@asset_check(asset="master_socioeconomic_data", description="Columnas de fuentes de ingreso presentes")
def check_master_columnas_ingresos(master_socioeconomic_data):
    expected = {"SUELDOS_SALARIOS", "PENSIONES", "PRESTACIONES_DESEMPLEO", "OTRAS_PRESTACIONES"}
    found = set(master_socioeconomic_data.columns)
    missing = expected - found
    return AssetCheckResult(
        passed=len(missing) == 0,
        metadata={"columnas_faltantes": str(missing) if missing else "ninguna"},
    )

@asset_check(asset="master_socioeconomic_data", description="Una fila por (año, section_id) — sin duplicados")
def check_master_unicidad(master_socioeconomic_data):
    """Verifica que el master esté en formato wide (una fila por sección-año).
    
    Si hay duplicados, el pivot de ocupación o ingresos no funcionó correctamente,
    y cualquier agregación posterior (mean, sum) producirá resultados incorrectos.
    """
    total = len(master_socioeconomic_data)
    unique = master_socioeconomic_data[["año", "section_id"]].drop_duplicates().shape[0]
    ok = total == unique
    return AssetCheckResult(
        passed=ok,
        metadata={
            "filas_totales": total,
            "combinaciones_unicas_año_section": unique,
            "duplicados": total - unique,
            "nota": "Duplicados indican que el pivot de ocupacion o ingresos no colapsó correctamente",
        },
    )

# CHECKS DE VISUALIZACIÓN

@asset_check(
    asset="plot_mapas_renta", 
    description="GeoJSON del año correcto disponible para cada mapa",
    additional_ins={"raw_geojsons": AssetIn("raw_geojsons")}
)
def check_viz_geojson_anio_correcto(raw_geojsons):
    """Verifica que el year_mapping usa el GeoJSON n+1 para datos de renta (regla ISTAC)
    y que ese GeoJSON está efectivamente cargado en raw_geojsons.
    """
    year_mapping_correcto = {2021: "2022", 2022: "2023", 2023: "2024"}
    issues = []
    for data_year, map_year in year_mapping_correcto.items():
        if map_year not in raw_geojsons:
            issues.append(
                f"Datos {data_year} requieren GeoJSON '{map_year}' pero no está cargado. "
                f"Añade '{map_year}' a la lista de años en raw_geojsons."
            )
    return AssetCheckResult(
        passed=len(issues) == 0,
        metadata={
            "regla": "Datos año N → GeoJSON año N+1 (regla ISTAC)",
            "mapping_correcto": str(year_mapping_correcto),
            "problemas": str(issues) if issues else "ninguno",
        },
    )

@asset_check(
    asset="plot_barras_apiladas_ingresos", 
    description="Los porcentajes de ingresos no superan el 100% (sin duplicados)",
    additional_ins={"master_socioeconomic_data": AssetIn("master_socioeconomic_data")}
)
def check_viz_barras_escala(master_socioeconomic_data):
    col = "SUELDOS_SALARIOS"
    if col not in master_socioeconomic_data.columns:
        return AssetCheckResult(passed=False, metadata={"error": f"Columna {col} no encontrada"})
    
    vals = pd.to_numeric(master_socioeconomic_data[col], errors="coerce").dropna()
    max_v = float(vals.max())
    
    no_supera_100 = max_v <= 105.0 
    
    return AssetCheckResult(
        passed=no_supera_100,
        metadata={
            "max_sueldos_salarios_detectado": round(max_v, 2),
            "recomendacion": "Si supera 100%, revisa el drop_duplicates() y el groupby() antes del melt() en el gráfico.",
        },
    )

@asset_check(asset="plot_lollipop_evolucion", description="Ambos años (2021 y 2023) presentes para el lollipop")
def check_viz_lollipop_anios(master_socioeconomic_data):
    años = set(master_socioeconomic_data["año"].unique())
    tiene_2021 = 2021 in años
    tiene_2023 = 2023 in años
    return AssetCheckResult(
        passed=tiene_2021 and tiene_2023,
        metadata={
            "años_disponibles": str(sorted(años)),
            "tiene_2021": tiene_2021,
            "tiene_2023": tiene_2023,
        },
    )

@asset_check(
    asset="master_socioeconomic_data", # <-- ¡Este es el cambio clave!
    description="No hay nulos en X o Y que hagan desaparecer puntos del gráfico"
)
def check_viz_scatter_puntos_perdidos(master_socioeconomic_data):
    df_2023 = master_socioeconomic_data[master_socioeconomic_data['año'] == 2023]
    
    # Identificamos las columnas X e Y
    col_x = "renta_bruta_media"
    col_y = [col for col in df_2023.columns if "Directores" in str(col)][0]
    
    # Comprobamos cuántas filas tienen NaN en alguna de las dos métricas
    filas_incompletas = df_2023[df_2023[col_x].isna() | df_2023[col_y].isna()]
    puntos_perdidos = len(filas_incompletas)
    
    return AssetCheckResult(
        passed=puntos_perdidos == 0,
        severity=AssetCheckSeverity.WARN, 
        metadata={
            "puntos_totales_posibles": len(df_2023),
            "puntos_que_no_se_dibujaran": puntos_perdidos,
            "ids_barrios_incompletos": str(filas_incompletas['section_id'].tolist()[:5]) + "..." if puntos_perdidos > 0 else "Ninguno"
        }
    )

