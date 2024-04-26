import os

import pandas as pd

concordance_and_mms = {
    "float_cols": [32, 33, 34, 35, 36],
    "date_col": 23,
    "requires_cost_calc": True,
    "col_map": {
        "contract": 37,
        "part": 25,
        "name": 12,
        "address": 13,
        "address_2": 14,
        "city": 15,
        "state": 16,
        "postal": 17,
        "invoice_nbr": 20,
        "invoice_date": 23,
        "quantity": 32,
        "uom": 31,
        "sale": 34,
        "unit_rebate": 35,
        "rebate": 36,
    },
}

current_file_maps = {
    "concordance": concordance_and_mms,
    "concordance_mms": concordance_and_mms,
    "mckesson": {},
    "cardinal": {},
    "henryschein": {},
    "medline": {},
    "owensminor": {},
    "atlantic_medical": {},
    "ameri_med": {},
    "avid": {},
    "mohawk": {},
    "dealmed": {
        "float_cols": [9, 11, 12, 13, 14, 15, 16],
        "date_col": 7,
        "requires_cost_calc": False,
        "col_map": {
            "contract": 17,
            "part": 8,
            "name": 1,
            "address": 2,
            "address_2": None,
            "city": 3,
            "state": 4,
            "postal": 5,
            "invoice_nbr": 6,
            "invoice_date": 7,
            "quantity": 9,
            "uom": 10,
            "sale": 14,
            "unit_rebate": 15,
            "rebate": 16,
        },
    },
    "ndc": {},
}


def read(file_path: str) -> pd.DataFrame:
    base_name = os.path.basename(file_path)
    extension = os.path.splitext(base_name)[1]

    if extension == ".csv":
        return pd.read_csv(file_path, dtype=str)
    elif extension == ".xlsx":
        return pd.read_excel(file_path, dtype=str)


def ingest(
    distributor: str,
    month: str,
    year: str,
    file_path: str,
    float_cols: list[int],
    date_col: int,
    requires_cost_calc: bool,
    col_map: dict[str, int],
    dw: bool = True,
) -> pd.DataFrame:
    from cast_cols import cast_date, cast_float
    from clean_cols import clean_contract_col, clean_part_col, clean_uom_col
    from convert_raw_df_to_tracings import convert_raw_to_tracing

    df = read(file_path)

    df = cast_float(df, float_cols)
    df = cast_date(df, date_col)
    df = clean_part_col(df, col_map["part"])
    df = clean_contract_col(df, col_map["contract"])
    df = clean_uom_col(df, col_map["uom"])

    df.fillna("", inplace=True)

    if dw:
        from write_to_data_warehouse import write_to_data_warehouse

        write_to_data_warehouse(df, year, month, file_path)

    tracings_df = convert_raw_to_tracing(
        df, distributor, month, year, requires_cost_calc, col_map
    )

    return tracings_df


def transform(
    distributor: str,
    month: str | int = None,
    year: str | int = None,
    tracings_df: pd.DataFrame = None,
):
    if len(tracings_df) == 0:
        from group_by import find_tracings

        tracings_df = find_tracings(
            year=year,
            month=month,
            distributor=distributor,
        )

    from group_by import group_tracings

    return group_tracings(tracings_df)


if __name__ == "__main__":
    # test
    dealmed = r"X:\rebate_trace_files\testing\DealMed_2024_03.csv"

    df = ingest(
        "dealmed",
        "05",
        "2024",
        dealmed,
        current_file_maps["dealmed"]["float_cols"],
        current_file_maps["dealmed"]["date_col"],
        current_file_maps["dealmed"]["col_map"],
        False,
    )
