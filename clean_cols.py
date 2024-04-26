import pandas as pd


def clean_part_col(df: pd.DataFrame, part_col: int) -> pd.DataFrame:
    df.iloc[:, part_col] = (
        df.iloc[:, part_col]
        .astype(str)
        .str.strip()
        .str.replace(" ", "")
        .str.replace("BW", "")
        .str.replace("-", "")
        .str.replace('"', "")
        .str.replace("=", "")
        .str.upper()
    )

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
    df.iloc[:, uom_col] = df.iloc[:, uom_col].str.replace("[^a-zA-Z]", "").str.upper()

    return df
