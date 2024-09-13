import os
import re

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

# itemnmbr	Item Class Code	vnditnum	itemdesc	itemtype	docdate	sopnumbe	qty_in_uofm	uofm	unitcost	ext cost	Contract Cost	Unit Rbt	Ext Rbt	Contract	custnmbr	Vizient ID	Premier ID	custname	shiptoname	address1	address2	city	state	zipcode	locncode


current_file_maps = {
    "concordance": concordance_and_mms,
    "concordance_mms": concordance_and_mms,
    "mckesson": {
        "float_cols": [13, 14, 15, 16, 17],
        "date_col": 6,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 9,
            "part": 10,
            "name": 19,
            "address": 20,
            "address_2": 21,
            "city": 22,
            "state": 23,
            "postal": 24,
            "invoice_nbr": 4,
            "invoice_date": 6,
            "quantity": 16,
            "uom": 12,
            "sale": 14,
            "unit_rebate": 15,
            "rebate": 17,
        },
    },
    "cardinal": {
        "float_cols": [56, 66, 67, 68],
        "date_col": 70,
        "requires_cost_calc": False,
        "col_map": {
            "contract": 15,
            "part": 51,
            "name": 21,
            "address": 22,
            "address_2": None,
            "city": 23,
            "state": 24,
            "postal": 25,
            "invoice_nbr": 69,
            "invoice_date": 70,
            "quantity": 56,
            "uom": 57,
            "sale": 66,
            "unit_rebate": 67,
            "rebate": 68,
        },
    },
    "henryschein": {
        "float_cols": [10, 11, 12, 13, 14],
        "date_col": 16,
        "requires_cost_calc": False,
        "col_map": {
            "contract": 1,
            "part": 5,
            "name": 24,
            "address": 27,
            "address_2": 28,
            "city": 29,
            "state": 30,
            "postal": 31,
            "invoice_nbr": 18,
            "invoice_date": 17,
            "quantity": 10,
            "uom": 8,
            "sale": 12,
            "unit_rebate": 13,
            "rebate": 14,
        },
    },
    "medline": {
        "float_cols": [20, 25, 26, 27, 28, 29, 30, 32],
        "date_col": 6,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 2,
            "part": 7,
            "name": 11,
            "address": 12,
            "address_2": None,
            "city": 13,
            "state": 14,
            "postal": 15,
            "invoice_nbr": 4,
            "invoice_date": 6,
            "quantity": 20,
            "uom": 21,
            "sale": 27,
            "unit_rebate": None,
            "rebate": 26,
        },
    },
    "medline_retro": {
        "float_cols": [20, 30, 31, 33, 34, 35, 36],
        "date_col": 5,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 28,
            "part": 6,
            "name": 11,
            "address": 12,
            "address_2": None,
            "city": 13,
            "state": 14,
            "postal": 15,
            "invoice_nbr": 3,
            "invoice_date": 5,
            "quantity": 20,
            "uom": 21,
            "sale": 31,
            "unit_rebate": None,
            "rebate": 36,
        },
    },
    "owensminor": {
        "float_cols": [15, 16, 17, 22, 26],
        "date_col": 21,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 13,
            "part": 25,
            "name": 6,
            "address": 7,
            "address_2": 8,
            "city": 10,
            "state": 11,
            "postal": 12,
            "invoice_nbr": 20,
            "invoice_date": 21,
            "quantity": 22,
            "uom": 23,
            "sale": 15,
            "unit_rebate": 17,
            "rebate": 26,
        },
    },
    "atl_med": {
        "float_cols": [7, 8, 9, 10],
        "date_col": 5,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 0,
            "part": 1,
            "name": 3,
            "address": 11,
            "address_2": 12,
            "city": 13,
            "state": 14,
            "postal": 15,
            "invoice_nbr": 6,
            "invoice_date": 5,
            "quantity": 7,
            "uom": 2,
            "sale": 9,
            "unit_rebate": None,
            "rebate": 10,
        },
    },
    "ameri_med": {
        "float_cols": [10, 11, 12, 13],
        "date_col": 9,
        "requires_cost_calc": False,
        "col_map": {
            "contract": 0,
            "part": 6,
            "name": 1,
            "address": 2,
            "address_2": None,
            "city": 3,
            "state": 4,
            "postal": 5,
            "invoice_nbr": 8,
            "invoice_date": 9,
            "quantity": 10,
            "uom": 14,
            "sale": 12,
            "unit_rebate": None,
            "rebate": 13,
        },
    },
    "avid": {
        "float_cols": [7, 9, 10, 11, 12],
        "date_col": 6,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 2,
            "part": 3,
            "name": 13,
            "address": 14,
            "address_2": None,
            "city": 15,
            "state": 16,
            "postal": 17,
            "invoice_nbr": 5,
            "invoice_date": 6,
            "quantity": 9,
            "uom": 8,
            "sale": 11,
            "unit_rebate": None,
            "rebate": 12,
        },
    },
    "mohawk": {
        "float_cols": [6, 8, 9, 10],
        "date_col": 1,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 2,
            "part": 4,
            "name": 11,
            "address": 12,
            "address_2": 13,
            "city": 14,
            "state": 15,
            "postal": 16,
            "invoice_nbr": 0,
            "invoice_date": 1,
            "quantity": 6,
            "uom": 7,
            "sale": 9,
            "unit_rebate": None,
            "rebate": 10,
        },
    },
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
    "ndc": {
        "float_cols": [44, 45, 47, 48, 49, 50, 51, 52],
        "date_col": 34,
        "requires_cost_calc": False,
        "col_map": {
            "contract": 7,
            "part": 40,
            "name": 22,
            "address": 23,
            "address_2": 24,
            "city": 26,
            "state": 27,
            "postal": 28,
            "invoice_nbr": 33,
            "invoice_date": 34,
            "quantity": 45,
            "uom": 46,
            "sale": 48,
            "unit_rebate": 51,
            "rebate": 52,
        },
    },
    "tri_anim": {
        "float_cols": [22, 23, 24, 25, 26],
        "date_col": 15,
        "requires_cost_calc": False,
        "col_map": {
            "contract": 2,
            "part": 19,
            "name": 4,
            "address": 6,
            "address_2": 7,
            "city": 8,
            "state": 9,
            "postal": 10,
            "invoice_nbr": 12,
            "invoice_date": 15,
            "quantity": 22,
            "uom": 20,
            "sale": 24,
            "unit_rebate": 23,
            "rebate": 25,
        },
    },
    "twin_med": {
        "float_cols": [7, 9, 10, 11, 12, 13],
        "date_col": 5,
        "requires_cost_calc": True,
        "col_map": {
            "contract": 14,
            "part": 2,
            "name": 19,
            "address": 20,
            "address_2": 21,
            "city": 22,
            "state": 23,
            "postal": 24,
            "invoice_nbr": 6,
            "invoice_date": 5,
            "quantity": 7,
            "uom": 8,
            "sale": 11,
            "unit_rebate": 12,
            "rebate": 13,
        },
    },
}


def read(file_path: str) -> pd.DataFrame:
    base_name = os.path.basename(file_path)
    extension = os.path.splitext(base_name)[1]

    match_csv = re.compile(r".csv$", re.IGNORECASE)
    match_xls = re.compile(r".xls$", re.IGNORECASE)
    match_xlsx = re.compile(r".xlsx$", re.IGNORECASE)

    if match_csv.match(extension):
        return pd.read_csv(file_path, dtype=str)
    elif match_xls.match(extension) or match_xlsx.match(extension):
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
    day: str | int = None,
) -> pd.DataFrame:
    from cast_cols import cast_date, cast_float
    from clean_cols import clean_contract_col, clean_part_col, clean_uom_col
    from convert_raw_df_to_tracings import convert_raw_to_tracing

    df = read(file_path)

    df.columns = [str(col) for col in df.columns]

    df = clean_part_col(df, col_map["part"])
    df = clean_contract_col(df, col_map["contract"])
    df = clean_uom_col(df, col_map["uom"])
    df = cast_float(df, float_cols)
    df = cast_date(df, date_col)

    df.fillna("", inplace=True)

    if distributor == "dealmed":
        # iterate over each row and if ['Extended Contract Price'] is 0 or NaN, set ['Extended Contract Price'] = ['Extended Purchase Price']
        df["Extended Contract Price"] = df.apply(
            lambda row: row["Extended Purchase Price"]
            if row["Extended Contract Price"] == "0"
            or row["Extended Contract Price"] == ""
            or pd.isna(row["Extended Contract Price"])
            else row["Extended Contract Price"],
            axis=1,
        )
        # iterate over each row and if ['Extended Rebate Requested'] is "" or NaN then set to float $0.00
        df["Extended Rebate Requested"] = df.apply(
            lambda row: 0.00
            if row["Extended Rebate Requested"] == ""
            or pd.isna(row["Extended Rebate Requested"])
            else row["Extended Rebate Requested"],
            axis=1,
        )

        # df.to_csv("dealmed.test.csv", index=False)

    if dw:
        from write_to_data_warehouse import write_to_data_warehouse

        write_to_data_warehouse(df, year, month, file_path)

    tracings_df = convert_raw_to_tracing(
        df=df,
        distributor=distributor,
        month=month,
        year=year,
        requires_cost_calc=requires_cost_calc,
        col_map=col_map,
        day=day,
    )

    # tracings_df.to_csv("tracings.test.csv", index=False)

    return tracings_df


def transform(
    distributor: str,
    month: str | int = None,
    year: str | int = None,
    tracings_df: pd.DataFrame = None,
):
    if tracings_df is None:
        from group_by import find_tracings

        tracings_df = find_tracings(
            year=year,
            month=month,
            distributor=distributor,
        )

        print(tracings_df.head())

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
