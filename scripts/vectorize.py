import pandas as pd

dataset = pd.read_parquet("results/dataset.parquet")

dataset.info()

grouped_dataset = dataset.groupby("fullbarcode").mean(numeric_only=True)

print(grouped_dataset.head())
