import re

from db import find_part_details_by_part

EACH = ["EA", "PR", "TR", "PK", "EACH", "Ea", "Pr", "Tr", "Pk", "Each", "KIT", "Kit"]
BOX = ["BOX", "BX", "Box", "Bx", "BG", "Bg", "Bag", "BAG"]
CASE = ["CS", "cs", "CA", "ca", "Ca", "Cs", "CASE", "Case", "CT", "ct", "Ct"]

box_exceptions = ["139", "153", "283", "284", "7190"]


def convert_to_cs(part: str, uom: str, quantity: float) -> float:
    if quantity == 0:
        return 0

    if part in box_exceptions and uom in BOX:
        return quantity

    doc = find_part_details_by_part(part)
    if doc:
        ea_per_case = doc["each_per_case"]
        bx_per_case = doc["num_of_dispenser_boxes_per_case"]
        if bx_per_case == 0:
            bx_per_case = 1

        try:
            if part == "164" and re.match(r"^pk", uom, re.IGNORECASE):
                return quantity / bx_per_case

            if part == "770" and re.match(r"^pk", uom, re.IGNORECASE):
                quantity *= 12
                return quantity / 48

            if part == "3220" and uom in CASE:
                return quantity / bx_per_case

            if part == "795" and re.match(r"^pk", uom, re.IGNORECASE):
                return quantity / ea_per_case

            if uom in EACH:
                return quantity / ea_per_case
            elif uom in BOX:
                return quantity / bx_per_case
            elif uom in CASE:
                return quantity
            else:
                return 0.0
        except ZeroDivisionError:
            print(part, uom, quantity, ea_per_case, bx_per_case)

            return 0.0
