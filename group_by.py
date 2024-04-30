import os
from datetime import date, datetime
from functools import lru_cache
from math import floor

import pandas as pd
from bson import ObjectId

from db import Tracings


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError("Type %s not serializable" % type(obj))


@lru_cache(maxsize=None)
def find_distinct_parts():
    import json

    from db import Sched_data

    return json.dumps(list(Sched_data.find({}, {"_id": 0})), default=json_serial)


@lru_cache(maxsize=None)
def find_contracts():
    import json

    from db import Contracts

    return json.dumps(list(Contracts.find({}, {"_id": 0})), default=json_serial)


def find_tracings(
    year: str | int, month: str | int, distributor: str = None
) -> pd.DataFrame:
    date = datetime(int(year), int(month), 1)

    distributor = distributor.upper()

    month_str = date.strftime("%B").upper()
    year_str = date.strftime("%Y")
    period = f"{month_str}{year_str}-{distributor}"

    filter = {"$match": {"period": {"$regex": period, "$options": "i"}}}

    docs = Tracings.aggregate([filter])

    if docs:
        return pd.DataFrame(list(docs))

    return pd.DataFrame()


def group_tracings(df: pd.DataFrame):
    import json

    distinct_parts_dict = find_distinct_parts()
    if isinstance(distinct_parts_dict, str):
        distinct_parts_dict = {
            each["part"]: each for each in json.loads(distinct_parts_dict)
        }

    contracts = find_contracts()
    if isinstance(contracts, str):
        contracts = {each["contractnumber"]: each for each in json.loads(contracts)}

    @lru_cache(maxsize=None)
    def contract_price(contract: str, part: str) -> float:
        contract = contracts.get(contract, None)

        if contract is None:
            return 0.0

        price = next(
            (
                x.get("price")
                for x in contract.get("pricingagreements", [])
                if x.get("item") == part
            ),
            None,
        )

        if price is None:
            return 0.0

        return float(price) if isinstance(price, str) else price

    @lru_cache(maxsize=None)
    def contract_name(contract: str) -> str:
        contract = contracts.get(contract, None)

        if contract is None:
            return ""

        return contract.get("contractname")

    @lru_cache(maxsize=None)
    def contract_end(contract: str) -> datetime:
        contract = contracts.get(contract, None)

        if contract is None:
            return ""

        contractend = contract.get("contractend")

        if isinstance(contractend, str):
            contractend = datetime.strptime(contractend.split("T")[0], "%Y-%m-%d")

        return contractend

    df["contract_price"] = df.apply(
        lambda x: contract_price(x.get("contract"), x.get("part")), axis=1
    )

    df["contract_name"] = df.apply(lambda x: contract_name(x.get("contract")), axis=1)

    df["contract_end"] = df.apply(lambda x: contract_end(x.get("contract")), axis=1)

    df = pd.DataFrame(
        df[df["contract"] != ""]
        .groupby(
            [
                "period",
                "contract",
                "part",
                "uom",
                "contract_price",
                "contract_name",
                "contract_end",
            ],
            as_index=False,
        )
        .sum(["ship_qty", "ship_qty_as_cs", "rebate", "cost"])
    )

    df["unit_rebate_as_cs"] = df.apply(
        lambda x: x.get("rebate") / x.get("ship_qty_as_cs")
        if x.get("ship_qty_as_cs") != 0
        else 0.0,
        axis=1,
    )

    df["ship_qty_as_cs_whole"] = df["ship_qty_as_cs"].apply(lambda x: floor(x))

    df["ship_qty_as_cs_partial"] = df.apply(
        lambda x: x["ship_qty_as_cs"] - x["ship_qty_as_cs_whole"], axis=1
    )

    df["rebate_whole"] = df.apply(
        lambda x: x["ship_qty_as_cs_whole"] * x["unit_rebate_as_cs"], axis=1
    )

    df["rebate_partial"] = df.apply(
        lambda x: x["ship_qty_as_cs_partial"] * x["unit_rebate_as_cs"], axis=1
    )

    df["each_size"] = df["part"].apply(
        lambda x: distinct_parts_dict.get(x, {"each_per_case": 0}).get("each_per_case")
    )

    df = df[
        [
            "period",
            "each_size",
            "contract",
            "contract_name",
            "contract_end",
            "part",
            "contract_price",
            "ship_qty",
            "uom",
            "ship_qty_as_cs_whole",
            "rebate_whole",
            "ship_qty_as_cs_partial",
            "rebate_partial",
            "ship_qty_as_cs",
            "rebate",
            "cost",
        ]
    ].copy()

    unique_periods = df["period"].unique()

    for period in unique_periods:
        df_with_summary = pd.DataFrame()

        df_temp = df.loc[df["period"] == period, :].copy()

        df_temp_sum_row = pd.DataFrame(
            data=df_temp[
                [
                    "ship_qty",
                    "ship_qty_as_cs_whole",
                    "rebate_whole",
                    "ship_qty_as_cs_partial",
                    "rebate_partial",
                    "ship_qty_as_cs",
                    "rebate",
                    "cost",
                ]
            ].sum()
        ).T

        df_temp_sum_row["period"] = period

        df_temp_sum_row = df_temp_sum_row.reindex(columns=df_temp.columns)

        df_temp = pd.concat([df_temp, df_temp_sum_row], axis=0)

        df_with_summary = pd.concat([df_with_summary, df_temp], axis=0)

        df_with_summary.to_excel(
            os.path.join("completed", f"{period}.xlsx"), index=False
        )

        print(f"Saved {period}.xlsx")

    return df


# if distributor == "CONCORDANCE_MMS":
#     count = Tracings.count_documents(
#         {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#     )

#     if count == 0:
#         distributor = "MMS"

#         period = f"{month_str}{year_str}-{distributor}"

#         count = Tracings.count_documents(
#             {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#         )

#         if count == 0:
#             return pd.DataFrame()

# elif distributor == "CONCORDANCE":
#     count = Tracings.count_documents(
#         {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#     )

#     if count == 0:
#         return pd.DataFrame()

# elif distributor == "MCKESSON":
#     count = Tracings.count_documents(
#         {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#     )

#     if count == 0:
#         distributor = "MGM"

#         period = f"{month_str}{year_str}-{distributor}"

#         count = Tracings.count_documents(
#             {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#         )

#         if count == 0:
#             return pd.DataFrame()
# elif distributor == "OWENSMINOR":
#     count = Tracings.count_documents(
#         {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#     )

#     if count == 0:
#         distributor = "OM"

#         period = f"{month_str}{year_str}-{distributor}"

#         count = Tracings.count_documents(
#             {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#         )

#         if count == 0:
#             return pd.DataFrame()

# else:
#     count = Tracings.count_documents(
#         {"period": {"$regex": f"^{period}.*", "$options": "i"}}
#     )

#     if count == 0:
#         return pd.DataFrame()
