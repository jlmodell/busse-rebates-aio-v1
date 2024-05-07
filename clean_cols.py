import pandas as pd


def clean_part_col(df: pd.DataFrame, part_col: int) -> pd.DataFrame:
    df.iloc[:, part_col] = (
        df.iloc[:, part_col]
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(".0", "")
        .str.replace(" ", "")
        .str.replace("BW", "")
        .str.replace("-", "")
        .str.replace('"', "")
        .str.replace("=", "")
        .str.replace("BUS", "")
        .str.replace("IMC", "")
        .str.replace("NAN", "")
    )

    df = df[
        (df.iloc[:, part_col] != "")
        & (df.iloc[:, part_col] != "nan")
        & (df.iloc[:, part_col] != "NAN")
        & (df.iloc[:, part_col] != "None")
    ]

    # print(df)

    return df


def clean_contract_col(df: pd.DataFrame, contract_col: int) -> pd.DataFrame:
    df.iloc[:, contract_col] = (
        df.iloc[:, contract_col]
        .astype(str)
        .str.strip()
        .str.replace(" ", "")
        .str.replace('"', "")
        .str.replace("=", "")
        .str.upper()
    )

    return df


def clean_uom_col(df: pd.DataFrame, uom_col: int) -> pd.DataFrame:
    # regex [a-ZA-Z] to remove any non-alphabet characters
    df.iloc[:, uom_col] = (
        df.iloc[:, uom_col]
        .str.replace(r"[^a-zA-Z]+", "", regex=True)
        .fillna(0)
        .str.upper()
    )

    return df
