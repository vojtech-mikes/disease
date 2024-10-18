import pandas as pd
from sqlalchemy import create_engine

dataset = pd.read_parquet("results/dataset.parquet")

grouped_dataset = dataset.groupby("fullbarcode", as_index=True).mean(
    numeric_only=True
)

grouped_dataset = grouped_dataset.dropna()

measurement_vectors = grouped_dataset["pdi"].to_frame()

state_vector = grouped_dataset[
    [
        "digital_biomass",
        "greenness_average",
        "leaf_area_index",
        "light_pene_depth",
        "ndvi_average",
        "npci_average",
        "psri_average",
    ]
]

control_vector = grouped_dataset[
    [
        "max_temp",
        "avg_temp",
        "min_temp",
        "max_humi",
        "avg_humi",
        "min_humi",
    ]
]


measurement_vectors.to_parquet("results/measurement_v.parquet")
control_vector.to_parquet("results/control_v.parquet")
state_vector.to_parquet("results/state_v.parquet")

measurement_vectors.to_excel("results/measurement_v.xlsx")
control_vector.to_excel("results/control_v.xlsx")
state_vector.to_excel("results/state_v.xlsx")

username = "root"
host = "localhost"
port = 3306
database = "disease_prediction"

engine = create_engine(f"mysql+pymysql://{username}@{host}:{port}/{database}")

measurement_vectors = measurement_vectors.reset_index()
control_vector = control_vector.reset_index()
state_vector = state_vector.reset_index()

measurement_vectors.to_sql("measurement", con=engine, index=True)
control_vector.to_sql("control", con=engine, index=True)
state_vector.to_sql("state", con=engine, index=True)
