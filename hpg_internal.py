import pandas as pd

# SELECT SA WITH F1 BETWEEN "START""END" AND F4 BETWEEN "9900""9923" AND F13 EQ "SL"
# LISTB SA F1 F2 F3 F4 CUST_NAME F5 F6 F7

# HEALTHTRUST GPOID or COid (from membership list)	GPOid	WAREHOUSE  NAME
# 27700	H057933	Asheville Supply Chain
# 27069	H003733	Austin Supply Chain
# 6858	H000108	Charleston Supply Chain
# 7846	H000116	Dallas Supply Chain
# 26346	H000452	Denver Supply Chain
# 27278	H000302	El Paso Supply Chain
# 8751	H000448	Ft. Lauderdale Supply Chain
# 8750	H000447	Houston Supply Chain
# 8749	H000446	Jacksonville Supply Chain
# 9752	H002871	Kansas City Supply Chain
# 27770	H072418	Lakeland Supply Chain RDC
# 9493	H002850	Las Vegas Supply Chain
# 7847	H000117	Nashville Supply Chain
# 8494	H000145	Portsmouth Supply Chain
# 7848	H000118	Richmond Supply Chain
# 25946	H015066	Rio Grande Supply Chain
# 8754	H000451	Salt Lake City Supply Chain
# 25748	H015295	San Antonio Supply Chain
# 26662	H027122	Southern California Supply Chain
# 8752	H000449	Tampa Supply Chain


def load_data():
    df = pd.read_clipboard(header=None)

    return df


def title_cols(df: pd.DataFrame):
    df.columns = [
        "id",
        "so_date",
        "so_nbr",
        "cust_po",
        "cust_id",
        "cust_name",
        "item",
        "qty",
        "sale",
    ]

    return df


def map_warehouse(df: pd.DataFrame):
    MAPPED_WH_LOCS = {
        "DENVER SUPPLY CHAIN": {
            "GPOID": "H000452",
            "WAREHOUSE NAME": "DENVER SUPPLY CHAIN",
            "HEALTHTRUST GPOID": "26346",
        },
        "SALT LAKE CITY CDC": {
            "GPOID": "H000451",
            "WAREHOUSE NAME": "SALT LAKE CITY SUPPLY CHAIN",
            "HEALTHTRUST GPOID": "8754",
        },
        "RIO GRANDE VALLEY SUPPLY": {
            "GPOID": "H015066",
            "WAREHOUSE NAME": "RIO GRANDE SUPPLY CHAIN",
            "HEALTHTRUST GPOID": "25946",
        },
        "LAKELAND SUPPLY CHAIN": {
            "GPOID": "H072418",
            "WAREHOUSE NAME": "Lakeland Supply Chain RDC",
            "HEALTHTRUST GPOID": "27770",
        },
        "HOUSTON SUPPLY CHAIN": {
            "GPOID": "H000447",
            "WAREHOUSE NAME": "Houston Supply Chain",
            "HEALTHTRUST GPOID": "8750",
        },
        "RICHMOND CSC": {
            "GPOID": "H000118",
            "WAREHOUSE NAME": "Richmond Supply Chain",
            "HEALTHTRUST GPOID": "7848",
        },
        "PORTSMOUTH SUPPLY CHAIN": {
            "GPOID": "H000145",
            "WAREHOUSE NAME": "Portsmouth Supply Chain",
            "HEALTHTRUST GPOID": "8494",
        },
        "CHARLESTON CDC": {
            "GPOID": "H000108",
            "WAREHOUSE NAME": "Charleston Supply Chain",
            "HEALTHTRUST GPOID": "6858",
        },
        "ASHEVILLE CDC": {
            "GPOID": "H057933",
            "WAREHOUSE NAME": "Asheville Supply Chain",
            "HEALTHTRUST GPOID": "27700",
        },
        "AUSTIN CDC": {
            "GPOID": "H003733",
            "WAREHOUSE NAME": "Austin Supply Chain",
            "HEALTHTRUST GPOID": "27069",
        },
        "DALLAS CSC": {
            "GPOID": "H000116",
            "WAREHOUSE NAME": "Dallas Supply Chain",
            "HEALTHTRUST GPOID": "7846",
        },
        "EL PASO CDC": {
            "GPOID": "H000302",
            "WAREHOUSE NAME": "El Paso Supply Chain",
            "HEALTHTRUST GPOID": "27278",
        },
        "FT. LAUDERDALE SUPPLY CHAIN": {
            "GPOID": "H000448",
            "WAREHOUSE NAME": "Ft. Lauderdale Supply Chain",
            "HEALTHTRUST GPOID": "8751",
        },
        "JACKSONVILLE SUP CHN OP": {
            "GPOID": "H000446",
            "WAREHOUSE NAME": "Jacksonville Supply Chain",
            "HEALTHTRUST GPOID": "8749",
        },
        "KANSAS CITY CDC": {
            "GPOID": "H002871",
            "WAREHOUSE NAME": "Kansas City Supply Chain",
            "HEALTHTRUST GPOID": "9752",
        },
        "LAS VEGAS CDC": {
            "GPOID": "H002850",
            "WAREHOUSE NAME": "Las Vegas Supply Chain",
            "HEALTHTRUST GPOID": "9493",
        },
        "NASHVILLE CSC": {
            "GPOID": "H000117",
            "WAREHOUSE NAME": "Nashville Supply Chain",
            "HEALTHTRUST GPOID": "7847",
        },
        "SAN ANTONIO CDC": {
            "GPOID": "H015295",
            "WAREHOUSE NAME": "San Antonio Supply Chain",
            "HEALTHTRUST GPOID": "25748",
        },
        "S CALIFORNIA CDC": {
            "GPOID": "H027122",
            "WAREHOUSE NAME": "Southern California Supply Chain",
            "HEALTHTRUST GPOID": "26662",
        },
        "TAMPA CDC": {
            "GPOID": "H000449",
            "WAREHOUSE NAME": "Tampa Supply Chain",
            "HEALTHTRUST GPOID": "8752",
        },
        "OVIEDO MEDICAL CENTER": {
            "GPOID": "",
            "WAREHOUSE NAME": "Oviedo Medical Center",
            "HEALTHTRUST GPOID": "",
        },
        "ATLANTA CDC": {
            "GPOID": "",
            "WAREHOUSE NAME": "Atlanta Supply Chain",
            "HEALTHTRUST GPOID": "",
        },
    }

    df["warehouse"] = df["cust_name"].apply(
        lambda row: MAPPED_WH_LOCS.get(row, {}).get("WAREHOUSE NAME", "UNKNOWN")
    )
    df["gpo_id"] = df["cust_name"].apply(
        lambda row: MAPPED_WH_LOCS.get(row, {}).get("GPOID", "UNKNOWN")
    )
    df["healthtrust_gpo_id"] = df["cust_name"].apply(
        lambda row: MAPPED_WH_LOCS.get(row, {}).get("HEALTHTRUST GPOID", "UNKNOWN")
    )

    return df


def set_sale_col_type(df: pd.DataFrame):
    df["sale"] = df["sale"].str.replace(",", "").astype(float)

    return df


def aggregate_by_gpo_id(df: pd.DataFrame):
    df = df.groupby(["healthtrust_gpo_id", "gpo_id", "warehouse"]).agg(
        {
            "sale": "sum",
        }
    )

    return df


def add_3pct_fee_col(df: pd.DataFrame):
    df["3pct_fee"] = df["sale"] * 0.03

    return df


def copy_tab_delimited_data_to_clipboard(df: pd.DataFrame):
    df.to_clipboard(sep="\t")

    return None


def main():
    df = load_data()
    df = title_cols(df)
    df = map_warehouse(df)
    df = set_sale_col_type(df)
    df = aggregate_by_gpo_id(df)
    df = add_3pct_fee_col(df)

    copy_tab_delimited_data_to_clipboard(df)

    print(df)

    return df


if __name__ == "__main__":
    try:
        df = main()
    except Exception as e:
        import calendar
        from datetime import datetime

        # Current date and time
        now = datetime.now()

        # Calculate the start and end of the previous quarter
        current_month = now.month
        current_year = now.year

        # Determine the quarter of the current date
        if current_month in [1, 2, 3]:
            # If we're in Q1, the previous quarter is Q4 of the previous year
            start_month = 10
            start_year = current_year - 1
            end_month = 12
            end_year = current_year - 1
        elif current_month in [4, 5, 6]:
            # Q2: Previous quarter is Q1
            start_month = 1
            start_year = current_year
            end_month = 3
            end_year = current_year
        elif current_month in [7, 8, 9]:
            # Q3: Previous quarter is Q2
            start_month = 4
            start_year = current_year
            end_month = 6
            end_year = current_year
        else:
            # Q4: Previous quarter is Q3
            start_month = 7
            start_year = current_year
            end_month = 9
            end_year = current_year

        # Calculate start and end dates of the previous quarter
        start = datetime(start_year, start_month, 1)  # First day of the quarter
        # Correctly calculate the last day of the quarter
        end = datetime(
            end_year, end_month, calendar.monthrange(end_year, end_month)[1]
        )  # Last day of the quarter

        # Format as mm-dd-yy
        start_formatted = start.strftime("%m-%d-%y")
        end_formatted = end.strftime("%m-%d-%y")

        print(e)
        print("\n" * 3)
        print(
            f"""Solution: run the following queries in Manage2k IBM db2 database & copy results to clipboard ~

    SELECT SA WITH F1 BETWEEN "{start_formatted}""{end_formatted}" AND F4 BETWEEN "9900""9923" AND F13 EQ "SL"
    
    LISTB SA F1 F2 F3 F4 CUST_NAME F5 F6 F7
"""
        )
