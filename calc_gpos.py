from datetime import datetime
from functools import lru_cache

import pandas as pd
from dateutil.relativedelta import relativedelta

from find_license import find_license


def adhoc_license_finder(row):
    try:
        group = row["gpo"]
        if group == "VHA":
            group = "MEDASSETS"
        name = row["name"]
        addr = row["addr"]
        city = row["city"]
        state = row["state"]
    except KeyError:
        return "0"

    lic, _ = find_license(
        group=group,
        name=name.strip().lower(),
        address=addr.strip().lower(),
        city=city.strip().lower(),
        state=state.strip().lower(),
    )

    return lic


def rerun_find_license(df: pd.DataFrame) -> None:
    from db import Tracings

    for idx, row in df.iterrows():
        print(idx + 1, "out of", len(df))

        license = adhoc_license_finder(row)

        print(license)

        row["license"] = license

        Tracings.update({"_id": row["_id"]}, {"$set": {"license": row["license"]}})

    return df


def postal_code(row):
    if len(row) == 0:
        return ""
    if not isinstance(row[0], dict):
        return ""

    details = row[0]

    if len(details["postal"]) == 4:
        return "0" + details["postal"]

    return details["postal"]


contracts_mapping = {}


@lru_cache(maxsize=None)
def check_if_item_in_contract(contract: str, part_number: str):
    from db import Contracts

    global contracts_mapping

    if contract not in contracts_mapping:
        doc = Contracts.find_one(
            {
                "contractnumber": contract,
            }
        )

        if doc is None:
            return False

        if len(doc.get("items", [])) != len(doc["pricingagreements"]):
            doc["items"] = [f["item"] for f in doc["pricingagreements"]]
            Contracts.update(
                {
                    "contractnumber": contract,
                },
                {
                    "$set": {
                        "items": doc["items"],
                    }
                },
            )

        contracts_mapping[contract] = doc["items"]

        # print(contracts_mapping[contract])

    return part_number in contracts_mapping[contract]


@lru_cache(maxsize=None)
def get_contract_price(contract: str, part_number: str, gpo: str):
    from db import Contracts, GPO_contracts

    doc = Contracts.find_one(
        {
            "contract": contract,
        }
    )

    if doc is None or len(doc["agreement"]) == 0:
        doc = Contracts.find_one(
            {
                "contractnumber": contract,
            }
        )
        if doc is None:
            return 0.0

        if len(doc["pricingagreements"]) == 0:
            return 0.0

        pricing = {f["item"]: f["price"] for f in doc["pricingagreements"]}

        GPO_contracts.update(
            {
                "contract": contract,
            },
            {
                "$set": {
                    "contract": contract,
                    "valid": True,
                    "gpo": gpo,
                    "agreement": pricing,
                }
            },
            upsert=True,
        )

        return pricing.get(part_number, 0.0)
    else:
        return doc["agreement"].get(part_number, 0.0)


@lru_cache(maxsize=None)
def get_roster(license: str, group_name: str):
    from db import Roster

    print(
        {
            "group_name": group_name,
            "member_id": license,
        }
    )

    doc = Roster.find_one(
        {
            "group_name": group_name,
            "member_id": license,
        }
    )

    if doc is None:
        return {"postal": "", "buyer_id": ""}

    return doc


@lru_cache(maxsize=None)
def get_part(part: str):
    from db import Sched_data

    doc = Sched_data.find_one(
        {
            "part": part,
        }
    )

    if doc is None:
        return None

    return doc


def fix_distributor_name(name: str):
    if name.startswith("CAR"):
        return "CARDINAL"
    elif name.startswith("CONC"):
        return "CONCORDANCE"
    elif name.startswith("NDC"):
        return "NDC"
    elif name.startswith("MCK"):
        return "MCKESSON"
    elif name.startswith("OWE"):
        return "OWENS & MINOR"
    elif name.startswith("MED"):
        return "MEDLINE"
    elif name.startswith("HEN"):
        return "HENRY SCHEIN"
    else:
        return name


def get_tracings_into_df(period: str, gpo: str, rerun_license: bool = False):
    # example)
    # period = "APRIL2024"
    # gpo = "aptitude"
    # rerun_license = False
    from db import Tracings

    gpo_report_map = {
        "aptitude": {
            "gpo": ["APTITUDE"],
            # "period": "APRIL2024",
            "contractMap": {
                "R11956A": "9998825",
                "R11956B": "9998825",
                "R11956C": "9998825",
                "R11956D": "9998825",
                "R13970A": "10008792",
            },
            "fee": 0.02,
            "fields": [
                "Contract ID",
                "Ship-To GLN ",
                "Ship-To LIC ",
                "Ship-To Member ID ",
                "Ship-To DEA Number ",
                "Ship-To Customer ID",
                "Ship-To Customer Name",
                "Ship-To Customer Address 1",
                "Ship-To Customer Address 2",
                "Ship-To Customer City",
                "Ship-To Customer State",
                "Ship-To Customer ZIP",
                "Invoice Number",
                "Invoice Date",
                "Quantity Invoiced",
                "Unit Price",
                "Sales Amount",
                "Transaction Fee Paid",
                "Transaction Fee Percentage",
                "Sale Indicator",
                "PO Number",
                "PO Date",
                "UOM Code",
                "Quantity per UOM",
                "Supplier Product Description",
                "Manufacturer Name",
                "Manufacturer GLN",
                "Manufacturer Assigned Product Number",
                "GTIN",
                "Chargeback ID",
                "Chargeback Description",
                "Distributor Assigned Product Number",
                "Distributor Name",
                "Internal Order Number",
                "HIN",
                "36-340B ID",
            ],
            "Contract ID": "contractMap",
            "Ship-To LIC ": "buyer_id",
            "Ship-To Member ID ": "license",
            "Ship-To Customer Name": "name",
            "Ship-To Customer Address 1": "addr",
            "Ship-To Customer City": "city",
            "Ship-To Customer State": "state",
            "Ship-To Customer ZIP": "postal",
            "Invoice Number": "invoice_nbr",
            "Invoice Date": "invoice_date",
            "Quantity Invoiced": "ship_qty_as_cs",
            "Unit Price": "cost_per_unit",
            "Sales Amount": "cost",
            "Transaction Fee Paid": "fee_paid",
            "Transaction Fee Percentage": "fee_pct",
            "UOM Code": "uom_cs",
            "Quantity per UOM": "case_size",
            "Supplier Product Description": "product_desc",
            "Manufactruer Name": "manufacturer_name",
            "Manufacturer GLN": "manufacturer_gln",
            "Manufacturer Assigned Product Number": "part",
            "GTIN": "gtin",
            "Distributor Name": "dist_name",
        },
        "medigroup": {
            "gpo": ["MEDIGROUP"],
            "fee": 0.03,
            "fee_string": "3",
            "agreement": "MG01154",
            "fields": [
                "Supplier Name",
                "Contract Number",
                "MediGroup ID (If tracked by Supplier)",
                "Primary Affiliate GPO ID (if applicable and if tracked by Supplier)",
                "Invoice #",
                "Inv Date",
                "Bill-To Account Number",
                "Bill-To Name",
                "Bill-To Address 1",
                "Bill-To Address 2",
                "Bill-To City",
                "Bill-To State",
                "Bill-To Zip",
                "Ship-To Account Number",
                "Ship-To Name",
                "Ship-To Address 1",
                "Ship-To Address 2",
                "Ship-To City",
                "Ship-To State",
                "Ship-To Zip",
                "Item Number",
                "Description",
                "UM",
                "Qty",
                "Price",
                "Extd Price",
                "Admin Fee %",
                "Admin Fee",
                "Distributor (if applicable)",
                "Rebate Contract Number (if applicable)",
            ],
            "Supplier Name": "manufacturer_name",
            "Contract Number": "agreement",
            "Invoice #": "invoice_nbr",
            "Inv Date": "invoice_date",
            "Bill-To Account Number": "license",
            "Bill-To Name": "name",
            "Bill-To Address 1": "addr",
            "Bill-To City": "city",
            "Bill-To State": "state",
            "Bill-To Zip": "postal",
            "Ship-To Account Number": "license",
            "Ship-To Name": "name",
            "Ship-To Address 1": "addr",
            "Ship-To City": "city",
            "Ship-To State": "state",
            "Ship-To Zip": "postal",
            "Item Number": "part",
            "Description": "product_desc",
            "UM": "uom_cs",
            "Qty": "ship_qty_as_cs",
            "Price": "cost_per_unit",
            "Extd Price": "cost",
            "Admin Fee %": "fee_string",
            "Admin Fee": "fee_paid",
            "Distributor (if applicable)": "dist_name",
            "Rebate Contract Number (if applicable)": "contract",
        },
        "northwell": {
            "gpo": ["LIJ"],
            "fee": 0.02,
            "fee_string": "2",
            "fields": [
                "Vendor Name",
                "Hospital Name",
                "Address",
                "City",
                "State",
                "Zip",
                "HIN (if applicable)",
                "Catalog# (Product #)",
                "Item Description",
                "InvoiceNo",
                "InvoiceDate",
                "PO Number",
                "SalesAmt",
                "PayOutAmount",
                "CAF %",
            ],
            "Vendor Name": "manufacturer_name",
            "Hospital Name": "name",
            "Address": "addr",
            "City": "city",
            "State": "state",
            "Zip": "postal",
            "Catalog# (Product #)": "part",
            "Item Description": "product_desc",
            "InvoiceNo": "invoice_nbr",
            "InvoiceDate": "invoice_date",
            "SalesAmt": "cost",
            "PayOutAmount": "fee_paid",
            "CAF %": "fee_string",
        },
        "premier": {
            "gpo": ["PREMIER"],
            "fee": 0.03,
            "direct_indirect": "I",
            "test_vs_production": "P",
            "entity": "601994",
            "uom_cs": "CA",
            "version_control": "867010710",
            "contractMap": {
                "R40605B": "PP-NS-1364",
                "R40605A": "PP-NS-1364",
                "R50605": "PP-NS-1789",
                "R65211": "ES-OR-2238",
                "R86430": "ES-NS-1868",
            },
            "fields": [
                "Version_control_#",
                "Test_vs_Production",
                "Vendor_Name",
                "Vendor_GLN",
                "Vendor_Premier_Entity_Code",
                "Vendor_DEA",
                "Vendor_HIN",
                "Facility_GLN",
                "Facility_Premier_Entity_Code",
                "Facility_DEA",
                "Facility_HIN",
                "Premier_Agreement_#",
                "Period_Begin_Date",
                "Period_End_Date",
                "Facility_Name",
                "Facility_Address_1",
                "Facility_Address_2",
                "Facility_City",
                "Facility_State",
                "Facility_Zip_Code",
                "Manufacturer_Item_#",
                "Vendor_Item_Number",
                "GTIN",
                "UPN_Type",
                "UPN",
                "UPC_Type",
                "UPC",
                "NDC",
                "Product_Description",
                "Unit_of_Measure",
                "Unit_Price",
                "Total_Sales_Dollars",
                "Total_Sales_Qty",
                "Invoice_Date",
                "Invoice_Number",
                "Order_Date",
                "Delivery_Date",
                "Administration_Fee_Amount",
                "Direct_Indirect",
                "Internal_Customer_Number",
            ],
            "Version_control_#": "version_control",
            "Test_vs_Production": "test_vs_production",
            "Vendor_Name": "manufacturer_name",
            "Vendor_Premier_Entity_Code": "entity",
            "Facility_Premier_Entity_Code": "license",
            "Premier_Agreement_#": "agreement",
            "Period_Begin_Date": "start_date",
            "Period_End_Date": "end_date",
            "Facility_Name": "name",
            "Facility_Address_1": "addr",
            "Facility_City": "city",
            "Facility_State": "state",
            "Facility_Zip_Code": "postal",
            "Manufacturer_Item_#": "part",
            "GTIN": "gtin",
            "Product_Description": "description",
            "Unit_of_Measure": "uom_cs",
            "Unit_Price": "cost_per_unit",
            "Total_Sales_Dollars": "cost",
            "Total_Sales_Qty": "ship_qty_as_cs",
            "Invoice_Number": "invoice_nbr",
            "Invoice_Date": "invoice_date",
            "Administration_Fee_Amount": "fee_paid",
            "Direct_Indirect": "direct_indirect",
        },
        "vizient": {
            "gpo": ["VHA"],
            "contractMap": {
                "R405T1": "XR0642",
                "R405T2": "XR0642",
                "R505T1": "XR0642",
                "R505T2": "XR0642",
                "R705T1": "MS8822",
                "R705T2": "MS8822",
            },
            "fee": 0.03,
            "fee_string": "3%",
            "fields": [
                "Contract Id",
                "Ship-To GLN *",
                "Ship-To LIC *",
                "Ship-To Member ID *",
                "Ship-To DEA Number *",
                "Ship-To Customer ID",
                "Ship-To Customer Name",
                "Ship-To Customer Address 1",
                "Ship-To Customer Address 2",
                "Ship-To Customer City",
                "Ship-To Customer State",
                "Ship-To Customer ZIP",
                "Invoice Number",
                "Invoice Date",
                "Quantity Invoiced",
                "Unit Price",
                "Sales Amount",
                "Administrative Fee Paid",
                "Administrative Fee Percentage",
                "Sale Indicator",
                "PO Number",
                "PO Date",
                "UOM Code",
                "Quantity per UOM",
                "Supplier Product Description",
                "Manufacturer Name",
                "Manufacturer GLN",
                "Manufacturer Assigned Product Number",
                "GTIN",
                "Chargeback ID",
                "Chargeback Description",
                "Distributor Assigned Product Number",
                "Distributor Name",
                "Supplier Internal Order Number",
                "Do Not Use",
                "Do Not Use ",
                "Do Not Use  ",
                "Do Not Use   ",
                "Do Not Use    ",
                "Do Not Use     ",
                "Do Not Use      ",
                "Do Not Use       ",
            ],
            "Contract Id": "vizient",
            "Ship-To LIC *": "license",
            "Ship-To Member ID *": "buyer_id",
            "Ship-To Customer ID": "license",
            "Ship-To Customer Name": "name",
            "Ship-To Customer Address 1": "addr",
            "Ship-To Customer City": "city",
            "Ship-To Customer State": "state",
            "Ship-To Customer ZIP": "postal",
            "Invoice Number": "invoice_nbr",
            "Invoice Date": "invoice_date",
            "Quantity Invoiced": "ship_qty_as_cs",
            "Unit Price": "cost_per_unit",
            "Sales Amount": "cost",
            "Administrative Fee Paid": "fee_paid",
            "Administrative Fee Percentage": "fee_string",
            "UOM Code": "uom_cs",
            "Quantity per UOM": "case_size",
            "Supplier Product Description": "product_desc",
            "Manufacturer Name": "manufacturer_name",
            "Manufacturer GLN": "manufacturer_gln",
            "Manufacturer Assigned Product Number": "part",
            "GTIN": "gtin",
            "Distributor Name": "dist_name",
        },
    }
    assert gpo in gpo_report_map, "Period not found in gpo_report_map"

    data = gpo_report_map[gpo]

    beginning_of_period = datetime.strptime(period, "%B%Y")

    match = {
        "$match": {
            "period": {"$regex": period, "$options": "i"},
        }
    }

    lookup = {
        "$lookup": {
            "from": "roster",
            "let": {
                "license": "$license",
                "group_name": "$gpo",
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$member_id", "$$license"]},
                                {"$eq": ["$group_name", "$$group_name"]},
                            ]
                        }
                    },
                }
            ],
            "as": "roster_details",
        }
    }

    if data.get("items", None) is not None:
        match["$match"]["part"] = {"$in": data.get("items")}

    if data.get("contractMap", None) is not None:
        match["$match"]["contract"] = {"$in": list(data.get("contractMap").keys())}
    else:
        match["$match"]["gpo"] = {"$in": data.get("gpo")}

    if data.get("periods", None) is not None:
        temp = r"("
        for period in data.get("periods"):
            if period == data.get("periods")[-1]:
                temp += rf"{period})"
            else:
                temp += rf"{period}|"

        match["$match"]["period"] = {"$regex": temp, "$options": "i"}

    if data.get("gpo") == ["UNITY"]:
        match = {
            "$match": {
                "period": {"$in": [f"{data.get('period')}-{data.get('distributor')}"]},
                "part": {"$in": list(data.get("itemMap").keys())},
            }
        }

        lookup = {
            "$lookup": {
                "from": "roster",
                "let": {
                    "license": "$name",
                    "group_name": "UNITY",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$member_id", "$$license"]},
                                    {"$eq": ["$group_name", "$$group_name"]},
                                ]
                            }
                        },
                    }
                ],
                "as": "roster_details",
            }
        }

    if data.get("gpo") == ["VHA"]:
        match = {
            "$match": {
                "gpo": {"$in": ["VHA", "VIZIENT", "MEDASSETS"]},
                "period": {"$regex": period, "$options": "i"},
                "contract": {"$in": list(data.get("contractMap").keys())},
            }
        }

        lookup = {
            "$lookup": {
                "from": "roster",
                "let": {
                    "license": "$license",
                    "group_name": "MEDASSETS",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$member_id", "$$license"]},
                                    {"$eq": ["$group_name", "$$group_name"]},
                                ]
                            }
                        },
                    }
                ],
                "as": "roster_details",
            }
        }

    docs = list(
        Tracings.aggregate(
            [
                match,
                lookup,
                {
                    "$lookup": {
                        "from": "sched_data",
                        "localField": "part",
                        "foreignField": "part",
                        "as": "part_details",
                    }
                },
                {
                    "$project": {
                        "period": 1,
                        "name": 1,
                        "addr": 1,
                        "city": 1,
                        "state": 1,
                        "license": 1,
                        "contract": 1,
                        "part": 1,
                        "invoice_nbr": 1,
                        "invoice_date": 1,
                        "gpo": 1,
                        "ship_qty": 1,
                        "ship_qty_as_cs": 1,
                        "uom": 1,
                        "roster_details": 1,
                        "part_details": 1,
                    }
                },
                {
                    "$sort": {
                        "period": 1,
                        "part": 1,
                    }
                },
            ]
        )
    )

    df = pd.DataFrame(docs)

    # df.to_excel(f"{period} {gpo}.xlsx", index=None)

    if rerun_license:
        print("rerunning license finder")

        df = rerun_find_license(df)

        docs = docs = list(
            Tracings.aggregate(
                [
                    match,
                    lookup,
                    {
                        "$lookup": {
                            "from": "sched_data",
                            "localField": "part",
                            "foreignField": "part",
                            "as": "part_details",
                        }
                    },
                    {
                        "$project": {
                            "period": 1,
                            "name": 1,
                            "addr": 1,
                            "city": 1,
                            "state": 1,
                            "license": 1,
                            "contract": 1,
                            "part": 1,
                            "invoice_nbr": 1,
                            "invoice_date": 1,
                            "gpo": 1,
                            "ship_qty": 1,
                            "ship_qty_as_cs": 1,
                            "uom": 1,
                            "roster_details": 1,
                            "part_details": 1,
                        }
                    },
                    {
                        "$sort": {
                            "period": 1,
                            "part": 1,
                        }
                    },
                ]
            )
        )

        df = pd.DataFrame(docs)

    df.to_excel(f"testing/{period} {gpo}.after_agg.xlsx", index=None)

    if not data.get("skip_contract_verification", False):
        df["item_in_contract"] = df.apply(
            lambda x: check_if_item_in_contract(x["contract"], x["part"]), axis=1
        )

        errors = df[df["item_in_contract"] == False].copy()
        if len(errors) > 0:
            errors.to_excel(f"errors/{period}_{gpo}.not_in_contract.xlsx", index=None)

        df = df[df["item_in_contract"] == True].copy()

    try:
        df = df[df["roster_details"].apply(lambda x: bool(x))].copy()
    except Exception:
        print(Exception)
        return df

    df["postal"] = df["roster_details"].apply(postal_code)

    df["buyer_id"] = df["roster_details"].apply(lambda x: x[0].get("buyer_id", ""))

    df["cost_per_unit"] = df.apply(
        lambda x: get_contract_price(x["contract"], x["part"], data.get("gpo")[0])
        if x["ship_qty_as_cs"] != 0
        else 0.0,
        axis=1,
    )

    df["cost"] = df["cost_per_unit"] * df["ship_qty_as_cs"]

    df["fee_paid"] = df["cost"].apply(lambda x: x * float(data.get("fee", 0.0)))

    df["gtin"] = df["part_details"].apply(lambda x: str(x[0]["gtin"].lstrip("0")))

    df["product_desc"] = df["part_details"].apply(lambda x: x[0]["description"])

    df["case_size"] = df["part_details"].apply(lambda x: x[0]["each_per_case"])

    df["dist_name"] = df["period"].apply(
        lambda x: fix_distributor_name(
            x.split("-")[1] if "CARD" not in x else "CARDINAL"
        )
    )

    def correct_invoice_date_to_datetime(x):
        try:
            if x >= beginning_of_period or x < beginning_of_period + relativedelta(
                months=1
            ):
                return x
        except Exception:
            try:
                new_date = datetime.strptime(x.replace(".0", ""), "%Y%m%d")
                if (
                    new_date >= beginning_of_period
                    or new_date < beginning_of_period + relativedelta(months=1)
                ):
                    return new_date
            except Exception:
                return beginning_of_period

    df["invoice_date"] = df["invoice_date"].apply(correct_invoice_date_to_datetime)

    df["start_date"] = df["period"].apply(
        lambda x: pd.to_datetime(x.split("-")[0], format="%B%Y").strftime("%m/%d/%Y")
    )

    df["end_date"] = df["period"].apply(
        lambda x: pd.to_datetime(x.split("-")[0], format="%B%Y")
    )

    df["end_date"] = df["end_date"].apply(
        lambda x: (x + relativedelta(months=1) - relativedelta(days=1)).strftime(
            "%m/%d/%Y"
        )
    )

    df["manufacturer_name"] = df["_id"].apply(lambda _: "Busse Hospital Disposables")

    df["unity"] = df["part"].apply(lambda x: data.get("itemMap", {}).get(x, ""))

    df["vizient"] = df["contract"].apply(
        lambda x: data.get("contractMap", {}).get(x, "")
    )

    if data.get("contractMap", None) is not None:
        df["contractMap"] = df["contract"].apply(
            lambda x: data.get("contractMap").get(x)
        )

    df["manufacturer_gln"] = df["_id"].apply(lambda _: "84923000001")

    df["fee_pct"] = df["_id"].apply(lambda _: float(data.get("fee", 0.0)))

    df["fee_string"] = df["_id"].apply(lambda _: data.get("fee_string", ""))

    df["uom_cs"] = df["_id"].apply(lambda _: data.get("uom_cs", "CS"))

    df["contract_id"] = df["_id"].apply(lambda _: data.get("contract_id"))

    df["direct_indirect"] = df["_id"].apply(lambda _: data.get("direct_indirect", ""))

    df["version_control"] = df["_id"].apply(lambda _: data.get("version_control", ""))

    df["test_vs_production"] = df["_id"].apply(
        lambda _: data.get("test_vs_production", "")
    )

    df["entity"] = df["_id"].apply(lambda _: data.get("entity", ""))

    if data.get("gpo", None) is not None:
        if "PREMIER" in data.get("gpo", []):
            # print(data.get("contractMap"))
            df["agreement"] = df["contract"].apply(
                lambda x: data.get("contractMap").get(x)
            )
            # print(df["agreement"].head(20))

    df["category"] = df["_id"].apply(lambda _: data.get("category", ""))

    for col in data.get("fields"):
        try:
            df[col] = df[data.get(col)] if data.get(col, None) is not None else ""

        except Exception as e:
            print(e, col, data.get(col))
            df[col] = ""

    df = df[data.get("fields")].copy()

    if data.get("groupby", None) is not None:
        df = df.groupby(data.get("groupby"), as_index=False).sum().copy()

    df.to_excel(f"completed/{period} {gpo}.xlsx", index=None)

    return df


if __name__ == "__main__":
    gpos = {
        "0": "all",
        "1": "aptitude",
        "2": "medigroup",
        "3": "northwell",
        "4": "premier",
        "5": "vizient",
    }

    gpo = gpos[
        input(
            """
    Which gpo do you want to calculate?

        '0' for all
        '1' for aptitude
        '2' for medigroup    
        '3' for northwell
        '4' for premier
        '5' for vizient        

    > """
        )
    ]
    rerun_licenses = input("Rerun licenses? (y/n) > ").lower().strip()

    if rerun_licenses == "y":
        rerun_licenses = True
    elif rerun_licenses == "":
        rerun_licenses = False
    else:
        rerun_licenses = False

    period = input("Enter period (e.g. APRIL2024) > ")

    if gpo == "all":
        for gpo in gpos.values():
            if gpo != "all":
                get_tracings_into_df(period, gpo, rerun_license=rerun_licenses)
    else:
        get_tracings_into_df(period, gpo, rerun_license=rerun_licenses)
