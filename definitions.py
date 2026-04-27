from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from assets import raw, cleansed, enriched, visualizations, checks 

all_assets = load_assets_from_modules([raw, cleansed, enriched, visualizations])

all_checks = load_asset_checks_from_modules([checks])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks, # <-- ¡La magia ocurre aquí!
)