import pandas as pd
from plotnine import *
from dagster import asset
import os
import geopandas as gpd

if not os.path.exists("output"):
    os.makedirs("output")

@asset
def plot_scatter_renta_ocupacion(master_socioeconomic_data):
    df = master_socioeconomic_data.copy()
    df = df[df['año'] == 2023]

    
    col_directores = [col for col in df.columns if 'Directores' in str(col)][0]
    df = df.rename(columns={col_directores: 'num_directores'})

    p = (
        ggplot(df, aes(x='renta_bruta_media', y='num_directores'))
        + geom_point(alpha=0.6, color="#2c3e50")
        + geom_smooth(method="lm", color="#e74c3c", se=False) 
        + labs(
            title="Relación entre renta media y perfiles directivos",
            subtitle="Por sección censal (2023)",
            x="Renta bruta media (€)",
            y="Número de Directores/Gerentes"
        )
        + theme_minimal()
    )
    
    p.save("output/scatter_renta_ocupacion.png")
    return "output/scatter_renta_ocupacion.png"

# --- MAPAS COROPLÉTICOS ---

@asset
def plot_mapas_renta(master_socioeconomic_data, raw_geojsons):
    """Genera 3 mapas de renta siguiendo la regla de años del enunciado"""
    
    year_mapping = {2021: "2022", 2022: "2023", 2023: "2024"} 
    
    for data_year, map_year in year_mapping.items():
        gdf = gpd.GeoDataFrame.from_features(raw_geojsons[map_year]["features"])
        gdf['section_id'] = gdf['geocode'].str.replace(r'.*_(\d{5})_D(\d{2})_S(\d{3})', r'\1\2\3', regex=True)
        
    
        df_year = master_socioeconomic_data[master_socioeconomic_data['año'] == data_year].copy()
        
    
        map_data = gdf.merge(df_year, on='section_id', how='left')
        
        p = (
            ggplot(map_data, mapping=aes(fill='renta_bruta_media')) 
            + geom_map()
            + theme_void()
            + theme(figure_size=(18, 14))
            + labs(
                title=f"Distribución de Renta en Santa Cruz de Tenerife por secciones censales ({data_year})",
                fill="Renta bruta \nmedia (€)" 
            )
            + theme(
                # 1. 'margin': 't' (top) da espacio por arriba, 'b' (bottom) lo separa del mapa
                plot_title=element_text(
                    size=20, 
                    family="Tahoma", 
                    face="bold", 
                    margin={'t': 20, 'b': 15} 
                ), 
                # 2. EL TRUCO MÁGICO: Baja el techo del gráfico para que quepa todo el título
                subplots_adjust={'top': 0.90} 
            )
            + theme(legend_position=(0.95, 0.5))
        )

        p.save(f"output/mapa_renta_{data_year}.png")

# --- BARRAS APILADAS (Fuente de Ingresos) ---

@asset
def plot_barras_apiladas_ingresos(master_socioeconomic_data):
    df = master_socioeconomic_data.copy()
    
    cols_ingresos = [
        'PENSIONES', 'SUELDOS_SALARIOS', 'PRESTACIONES_DESEMPLEO', 
        'OTRAS_PRESTACIONES', 'OTROS_INGRESOS'
    ]
    
    df_ingresos = df[['municipio', 'año', 'section_id', 'renta_bruta_media'] + cols_ingresos].drop_duplicates()
    
    df_muni = df_ingresos.groupby(['municipio', 'año'])[cols_ingresos + ['renta_bruta_media']].mean().reset_index()
    
    df_long = df_muni.melt(
        id_vars=['municipio', 'año', 'renta_bruta_media'], 
        value_vars=cols_ingresos,
        var_name='fuente_ingreso',
        value_name='valor_ingreso'
    )
    
    top_10_renta = df_muni[df_muni['año'] == 2023].nlargest(10, 'renta_bruta_media')['municipio']
    df_plot = df_long[(df_long['municipio'].isin(top_10_renta)) & (df_long['año'] == 2023)].copy()

    rename_map = {
    'PENSIONES': 'Pensiones',
    'SUELDOS_SALARIOS': 'Sueldos/Salarios',
    'PRESTACIONES_DESEMPLEO': 'Prest. Desempleo',
    'OTRAS_PRESTACIONES': 'Otras Prestaciones',
    'OTROS_INGRESOS': 'Otros Ingresos'
    }
    df_plot['fuente_ingreso'] = df_plot['fuente_ingreso'].map(rename_map)

    p = (
        ggplot(df_plot, aes(x='reorder(municipio, renta_bruta_media)', y='valor_ingreso', fill='fuente_ingreso'))
        + geom_col(position='fill') 
        + scale_y_continuous(labels=lambda l: [f"{int(x*100)}%" for x in l])
        + coord_flip()
        + scale_fill_brewer(type='qual', palette='Paired')
        + labs(
            title="Origen de los ingresos: 10 municipios con ingresos más altos",
            subtitle="Composición porcentual de la renta (2023)",
            x="Municipio",
            y="Proporción de la fuente de ingreso",
            fill="Fuente"
        )
        + theme_minimal()
        + theme(
            figure_size=(10, 6),
            legend_position='bottom',
            axis_text_y=element_text(size=9),
            subplots_adjust={'right': 0.3}
        )
    )
    
    p.save("output/ingresos_municipios_ricos.png")
    return "output/ingresos_municipios_ricos.png"

@asset
def plot_barras_apiladas_ingresos_pobres(master_socioeconomic_data):
    df = master_socioeconomic_data.copy()
    
    cols_ingresos = ['PENSIONES', 'SUELDOS_SALARIOS', 'PRESTACIONES_DESEMPLEO', 'OTRAS_PRESTACIONES', 'OTROS_INGRESOS']
    
    df_ingresos = df[['municipio', 'año', 'section_id'] + cols_ingresos].drop_duplicates()
    df_muni = df_ingresos.groupby(['municipio', 'año'])[cols_ingresos].mean().reset_index()
    
    bottom_10_renta = (
        df[['municipio', 'año', 'section_id', 'renta_bruta_media']]
        .drop_duplicates()
        .groupby(['municipio', 'año'])['renta_bruta_media'].mean()
        .reset_index()
    )
    bottom_10_muni = bottom_10_renta[bottom_10_renta['año'] == 2023].nsmallest(10, 'renta_bruta_media')['municipio']
    
    df_long = df_muni.melt(
        id_vars=['municipio', 'año'], 
        value_vars=cols_ingresos,
        var_name='fuente_ingreso',
        value_name='valor_ingreso'
    )
    
    df_plot = df_long[(df_long['municipio'].isin(bottom_10_muni)) & (df_long['año'] == 2023)].copy()
    rename_map = {
    'PENSIONES': 'Pensiones',
    'SUELDOS_SALARIOS': 'Sueldos/Salarios',
    'PRESTACIONES_DESEMPLEO': 'Prest. Desempleo',
    'OTRAS_PRESTACIONES': 'Otras Prestaciones',
    'OTROS_INGRESOS': 'Otros Ingresos'
    }
    df_plot['fuente_ingreso'] = df_plot['fuente_ingreso'].map(rename_map)

    p = (
        ggplot(df_plot, aes(x='reorder(municipio, valor_ingreso)', y='valor_ingreso', fill='fuente_ingreso'))
        # FIX: Volvemos a 'fill'
        + geom_col(position='fill')
        + scale_y_continuous(labels=lambda l: [f"{int(x*100)}%" for x in l])
        + coord_flip()
        + scale_fill_brewer(type='qual', palette='Paired')
        + labs(
            title="Origen de los ingresos: 10 municipios con ingresos más bajos",
            subtitle="Composición porcentual de la renta (2023)",
            x="Municipio",
            y="Proporción de la fuente de ingreso",
            fill="Fuente"
        )
        + theme_minimal()
        + theme(
            figure_size=(10, 6),
            legend_position='bottom',
            axis_text_y=element_text(size=9),
            subplots_adjust={'right': 0.3}
        )
    )
    
    p.save("output/ingresos_municipios_pobres.png")
    return "output/ingresos_municipios_pobres.png"   

# -- GRÁFICOS DE LOLLIPOP --

@asset
def plot_evolucion_renta_lollipop(master_socioeconomic_data):
    df = master_socioeconomic_data.copy()
    
    df_evol = df[df['año'].isin([2021, 2023])]
    df_muni = df_evol.groupby(['municipio', 'año'])['renta_bruta_media'].mean().unstack().reset_index()
    df_muni = df_muni.rename(columns={2021: 'renta_2021', 2023: 'renta_2023'})
    df_plot = df_muni.nlargest(10, 'renta_2023').copy()

    p = (
        ggplot(df_plot)
        + geom_segment(
            aes(x='reorder(municipio, renta_2023)', xend='reorder(municipio, renta_2023)', 
                y='renta_2021', yend='renta_2023'), 
            color='#a1a1a1', size=1.5
        )
        + geom_point(aes(x='municipio', y='renta_2021'), color='#e74c3c', size=3) 
        + geom_point(aes(x='municipio', y='renta_2023'), color='#27ae60', size=3) 
        + coord_flip()
        + labs(
            title="Evolución de renta: 10 municipios con ingresos más altos (2021 vs 2023)",
            subtitle="Brecha de crecimiento anual por municipio. Rojo: 2021 | Verde: 2023",
            x="Muncipio", y="Renta Bruta Media (€)"
        )
        + theme_minimal()
        + theme(figure_size=(10, 8))
    )
    
    p.save("output/lollipop_evolucion_renta.png")
    return "output/lollipop_evolucion_renta.png"
    
@asset
def plot_lollipop_evolucion_pobres(master_socioeconomic_data):
    df = master_socioeconomic_data.copy()
    
    df_evol = (
        df[df['año'].isin([2021, 2023])]
        .groupby(['municipio', 'año'])['renta_bruta_media']
        .mean()
        .unstack()
        .reset_index()
    )
    
    df_evol = df_evol.rename(columns={2021: 'renta_2021', 2023: 'renta_2023'})
    df_plot = df_evol.nsmallest(10, 'renta_2023').copy()

    p = (
        ggplot(df_plot)
        + geom_segment(
            aes(x='reorder(municipio, renta_2023)', xend='municipio', y='renta_2021', yend='renta_2023'), 
            color="#95a5a6", size=1
        )
        + geom_point(aes(x='municipio', y='renta_2021'), color="#e74c3c", size=3) 
        + geom_point(aes(x='municipio', y='renta_2023'), color="#2ecc71", size=3) 
        + coord_flip()
        + labs(
            title="Evolución de renta: 10 municipios con ingresos más bajos (2021 vs 2023)",
            subtitle="Brecha de crecimiento anual por municipio. Rojo: 2021 | Verde: 2023",
            x="Muncipio", y="Renta Bruta Media (€)"
        )
        + theme_minimal()
        + theme(figure_size=(10, 8))
    )
    
    p.save("output/lollipop_evolucion_pobres.png")
    return "output/lollipop_evolucion_pobres.png"