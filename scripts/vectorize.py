import pandas as pd
from sqlalchemy import create_engine

dataset = pd.read_parquet("results/dataset.parquet")

dataset.info()

state = dataset[
    [
        "timestamp",
        "fullbarcode",
        "digital_biomass",
        "greenness_average",
        "leaf_area_index",
        "light_pene_depth",
        "ndvi_average",
        "npci_average",
        "psri_average",
    ]
]

control = dataset[
    [
        "timestamp",
        "fullbarcode",
        "max_temp",
        "avg_temp",
        "min_temp",
        "max_humi",
        "avg_humi",
        "min_humi",
    ]
]

dataset.info()

measurement = dataset[
    [
        "timestamp",
        "fullbarcode",
        "pdi",
    ]
]

measurement.info()

barcodes = dataset[
    [
        "fullbarcode",
        "plants/pot",
        "treatment",
    ]
].drop_duplicates()


username = "root"
host = "localhost"
port = 3306
database = "disease_prediction"

engine = create_engine(f"mysql+pymysql://{username}@{host}:{port}/{database}")

# barcodes.to_sql("barcodes", con=engine, if_exists="append", index=False)

state.to_sql("states", con=engine, if_exists="append", index=False)

measurement.to_sql("measurements", con=engine, if_exists="append", index=False)

control.to_sql("controls", con=engine, if_exists="append", index=False)
