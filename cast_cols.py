import pandas as pd


def cast_float(df: pd.DataFrame, float_cols: list[int]) -> pd.DataFrame:
    for col in float_cols:
        df.iloc[:, col] = df.iloc[:, col].astype(float)

    return df


def cast_date(df: pd.DataFrame, date_col: int) -> pd.DataFrame:
    df.iloc[:, date_col] = pd.to_datetime(df.iloc[:, date_col], errors="coerce")

    return df
