from datetime import datetime

import pandas as pd


def convert_raw_to_tracing(
    df: pd.DataFrame,
    distributor: str,
    month: str | int,
    year: str | int,
    requires_cost_calc: bool,
    col_map: dict[str, int],
    day: str | int = None,
) -> pd.DataFrame:
    from convert_to_cs import convert_to_cs
    from db import find_gpo_by_contract
    from find_license import find_license_using_atlas_search
    from generate_period import generate_period

    if day:
        tracing_date = datetime(int(year), int(month), int(day))
    else:
        tracing_date = datetime(int(year), int(month), 1)

    cols = df.columns

    contract = cols[col_map["contract"]]
    part = cols[col_map["part"]]

    name = cols[col_map["name"]]
    address = cols[col_map["address"]]
    if col_map["address_2"]:
        address_2 = cols[col_map["address_2"]]
    else:
        address_2 = None
    city = cols[col_map["city"]]
    state = cols[col_map["state"]]
    postal = cols[col_map["postal"]]

    invoice_nbr = cols[col_map["invoice_nbr"]]
    invoice_date = cols[col_map["invoice_date"]]

    quantity = cols[col_map["quantity"]]
    uom = cols[col_map["uom"]]
    sale = cols[col_map["sale"]]
    rebate = cols[col_map["rebate"]]
    if col_map["unit_rebate"]:
        unit_rebate = cols[col_map["unit_rebate"]]
    else:
        unit_rebate = None

    period = generate_period(distributor, month, year, day)

    tracings = []

    for _, row in df.iterrows():
        if not row[part]:
            continue

        row_gpo = find_gpo_by_contract(row[contract])
        # print(row[contract], row[contract][:5], row_gpo)
        row_contract = row[contract]
        license_details = find_license_using_atlas_search(
            row_gpo, row[name], row[address], row[city], row[state]
        )
        license, search_score = license_details.split("|")

        if requires_cost_calc:
            total_sale = row[quantity] * row[sale]
        else:
            total_sale = row[sale]

        if unit_rebate:
            calculated_unit_rebate = row[unit_rebate]
        else:
            calculated_unit_rebate = row[rebate] / row[quantity]

        if col_map["uom"]:
            uom_val = row[uom]
        else:
            uom_val = "CA"

        tracing = {
            "period": period,
            "name": row[name],
            "addr": row[address] + " " + row[address_2] if address_2 else row[address],
            "city": row[city],
            "state": row[state],
            "postal": f"{row[postal]:05}",
            "gpo": row_gpo,
            "license": license,
            "searchScore": search_score,
            "contract": row_contract,
            "claim_nbr": "",
            "order_nbr": "",
            "invoice_nbr": row[invoice_nbr],
            "invoice_date": row[invoice_date],
            "part": row[part],
            "unit_rebate": calculated_unit_rebate,
            "ship_qty": row[quantity],
            "uom": uom_val,
            "ship_qty_as_cs": convert_to_cs(row[part], row[uom], row[quantity]),
            "rebate": row[rebate],
            "cost": total_sale,
            "check_license": False,
            "__date__": tracing_date,
        }

        tracings.append(tracing)

    average_search_score = sum(
        [float(tracing["searchScore"]) for tracing in tracings]
    ) / len(tracings)

    def check_license(score: float) -> bool:
        if score == 0.0 or score == 0:
            return False
        elif score < average_search_score * 0.5:
            return True
        else:
            return False

    for tracing in tracings:
        tracing["check_license"] = check_license(float(tracing["searchScore"]))

        # print(tracing)

    from db import Tracings

    print(f"Deleting data for {period} from Tracings")
    Tracings.delete_many(
        {
            "period": period,
        }
    )
    print(f"Inserting data for {period} into Tracings")
    Tracings.insert_many(tracings)

    tracings_df = pd.DataFrame(tracings)

    print(f"Total sum of rebates: {tracings_df['rebate'].sum()}")

    return tracings_df
