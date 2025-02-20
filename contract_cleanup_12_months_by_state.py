import re
from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

from db import Tracings as t


def get_gpo_by_contract(contract: str) -> str:
    gpo_contract_map = {
        "R12304": "NORTHWELL",
        "R9873": "MAGNET",
        "R12546": "MEDIGROUP",
        "R11956": "APTITUDE",
        "R13970": "APTITUDE",
        "R2102": "HEALTHTRUST",
        "R2105": "INJX",
        "R506T": "VIZIENT",
        "R705T": "VIZIENT",
        "R8643": "PREMIER",
        "R6521": "PREMIER",
        "R5060": "PREMIER",
    }
    return gpo_contract_map.get(contract, "UNKNOWN")


def find_many_by_date_minus_12_months(dt: datetime, contract: str) -> list:
    # Calculate start_date: Last day of the current month
    start_date = dt.replace(day=1) - timedelta(days=1)

    # Calculate end_date: First day of the month from 12 months ago
    end_date = (dt - relativedelta(months=12)).replace(day=1)

    print(f"start_date: {start_date}, end_date: {end_date}")

    return (
        list(
            t.find(
                {
                    "__date__": {"$gte": end_date, "$lte": start_date},
                    "contract": {"$regex": f"^{contract}", "$options": "i"},
                }
            )
        ),
        start_date,
        end_date,
    )


def utility_fn_uppercase(df: pd.DataFrame, col: str) -> pd.DataFrame:
    try:
        df.loc[:, col] = df[col].astype(str).str.upper()
        return df
    except Exception as e:
        raise e


def utility_fn_split_df_on_state(df: pd.DataFrame) -> dict:
    col = "state"
    states = df[col].unique()
    return {state: df[df[col] == state] for state in states}


def utility_fn_convert_datetime_to_str_with_fallback(row) -> str:
    invoice_date = row["invoice_date"]
    __date__ = row["__date__"]

    try:
        return pd.to_datetime(invoice_date).strftime("%m/%d/%Y")
    except Exception:
        # print(e)
        return pd.to_datetime(__date__).strftime("%m/%d/%Y")


def utility_fn_extract_state(state: str) -> str:
    result = re.findall(r"[A-Z]+", state)
    try:
        return result[0]
    except Exception:
        # print(e)
        # print(state)
        pass


def cleanup_df(contract: str, df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df[
        [
            "contract",
            "license",
            "name",
            "addr",
            "city",
            "state",
            "gpo",
            "invoice_date",
            "invoice_nbr",
            "part",
            "ship_qty_as_cs",
            "uom",
            "cost",
            # "rebate",
            "__date__",
        ]
    ]

    df.columns = [
        "contract",
        "license",
        "name",
        "addr",
        "city",
        "state",
        "gpo",
        "invoice_date",
        "invoice_nbr",
        "part",
        "ship_qty",
        "uom",
        "sale",
        # "rebate",
        "__date__",
    ]

    df["uom"] = "CS"
    # df["invoice_date"] = pd.to_datetime(df["invoice_date"]).dt.strftime("%m/%d/%Y")
    df["invoice_date"] = df.apply(
        utility_fn_convert_datetime_to_str_with_fallback, axis=1
    )

    df["state"] = df["state"].astype(str).str.strip()

    df = df[
        [
            "contract",
            "license",
            "name",
            "addr",
            "city",
            "state",
            "gpo",
            "invoice_date",
            "invoice_nbr",
            "part",
            "ship_qty",
            "uom",
            "sale",
            # "__date__",
        ]
    ]

    df.fillna("", inplace=True)

    columns_to_convert = [
        "contract",
        "license",
        "name",
        "addr",
        "city",
        "state",
        # "gpo",
    ]

    for column in columns_to_convert:
        utility_fn_uppercase(df, column)  # Don't reassign df

    df["gpo"] = contract

    df["state"] = df["state"].apply(lambda x: utility_fn_extract_state(x))

    df = df.sort_values(by=["state", "contract", "part"])
    df = df.reset_index(drop=True)

    return df


def write_to_xlsx_split_by_state(dfs: dict, output_file_name: str) -> None:
    # save into one workbook, different sheets by state key
    # dfs = {state: df}
    with pd.ExcelWriter(output_file_name) as writer:
        for state, df in dfs.items():
            if state:
                df.to_excel(writer, sheet_name=state, index=False)


def main():
    import os
    import sys

    skip_user_input = False

    if len(sys.argv) > 1:
        run_all = sys.argv[1]
        save_all = sys.argv[2]
        if run_all == "true" and save_all == "true":
            skip_user_input = True

    if skip_user_input:
        contracts = [
            "R12304",
            "R9873",
            "R12546",
            "R11956",
            "R13970",
            "R2102",
            "R2105",
            "R506T",
            "R705T",
            "R8643",
            "R6521",
            "R5060",
        ]

        output_path = r"C:\temp\contract_spend_reports"
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        try:
            arg_start_date = sys.argv[3]  # %Y-%m-%d
            arg_start_date = datetime.strptime(arg_start_date, "%Y-%m-%d")
        except IndexError:
            arg_start_date = None
        except Exception as e:
            print(" ".join(sys.argv))
            raise e

        if not arg_start_date:
            arg_start_date = datetime.now()

        print("Gathering sales 12 months trailing a/o", arg_start_date)

        for contract in contracts:
            print(f"Processing {contract}...")

            docs, end_dt, start_dt = find_many_by_date_minus_12_months(
                arg_start_date, contract=contract
            )

            if docs:
                print(len(docs))
                df = pd.DataFrame(docs)
                df = cleanup_df(get_gpo_by_contract(contract), df)

                sum_of_sales = df["sale"].sum()

                dfs = utility_fn_split_df_on_state(df)
                gpo = None
                for state, df in dfs.items():
                    if not gpo:
                        gpo = get_gpo_by_contract(contract)

                output_file_name = f"{gpo}_{contract}_tracings_{start_dt:%Y%m%d}-{end_dt:%Y%m%d}_{datetime.now():%Y%m%d%H%M%S}.xlsx"
                output_file_name = os.path.join(output_path, output_file_name)

                write_to_xlsx_split_by_state(dfs, output_file_name)

                print(f"Saved to {output_file_name}")

    else:
        contract = input("CONTRACT: ")
        contract = contract.upper() if contract else "R705T"

        docs, end_dt, start_dt = find_many_by_date_minus_12_months(
            datetime.now(), contract=contract
        )

        if docs:
            print(len(docs))
            df = pd.DataFrame(docs)
            df = cleanup_df(df)

            # print(df.head())

            sum_of_sales = df["sale"].sum()
            print(f"Sum of sales: {sum_of_sales}")

            save = input("Save to excel? (y/n): ")
            if save.lower() == "y":
                dfs = utility_fn_split_df_on_state(df)
                gpo = None
                for state, df in dfs.items():
                    if not gpo:
                        gpo = df["gpo"].unique()[0]
                    sum_of_sales_by_state = df["sale"].sum()
                    print(f"{state}: {sum_of_sales_by_state}")

                output_file_name = f"{gpo}_{contract}_tracings_{start_dt:%Y%m%d}-{end_dt:%Y%m%d}_{datetime.now():%Y%m%d%H%M%S}.xlsx"

                write_to_xlsx_split_by_state(dfs, output_file_name)

                print(f"Saved to {output_file_name}")


if __name__ == "__main__":
    main()
