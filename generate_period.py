def generate_period(
    distributor: str, month: str | int, year: str | int, day: str | int = None
) -> str:
    month_map = {
        "01": "JANUARY",
        "02": "FEBRUARY",
        "03": "MARCH",
        "04": "APRIL",
        "05": "MAY",
        "06": "JUNE",
        "07": "JULY",
        "08": "AUGUST",
        "09": "SEPTEMBER",
        "10": "OCTOBER",
        "11": "NOVEMBER",
        "12": "DECEMBER",
        "1": "JANUARY",
        "2": "FEBRUARY",
        "3": "MARCH",
        "4": "APRIL",
        "5": "MAY",
        "6": "JUNE",
        "7": "JULY",
        "8": "AUGUST",
        "9": "SEPTEMBER",
        1: "JANUARY",
        2: "FEBRUARY",
        3: "MARCH",
        4: "APRIL",
        5: "MAY",
        6: "JUNE",
        7: "JULY",
        8: "AUGUST",
        9: "SEPTEMBER",
        10: "OCTOBER",
        11: "NOVEMBER",
        12: "DECEMBER",
    }

    if month not in month_map:
        raise ValueError("Invalid month")

    day = f"{day:02}" if day else "01"

    return f"{month_map[month]}{year}-{distributor.upper()}_{year}{month}{day}"
