import os

import pandas as pd


def write_to_data_warehouse(
    df: pd.DataFrame,
    year: str,
    month: str,
    filepath: str,
) -> None:
    from db import Data_warehouse

    df["__year__"] = year
    df["__month__"] = month
    df["__file__"] = os.path.basename(filepath)

    filter = {
        "__year__": year,
        "__month__": month,
        "__file__": os.path.basename(filepath),
    }

    print(f"Deleting data for {year}-{month} from Data Warehouse")
    Data_warehouse.delete_many(filter)

    data = df.to_dict(orient="records")

    print(f"Writing data for {year}-{month} to Data Warehouse")
    Data_warehouse.insert_many(data)
