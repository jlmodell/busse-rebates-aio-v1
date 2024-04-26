from functools import lru_cache

ATLAS_SEARCH_INDEX_NAME = "find_license"

VALID_GPOS = [
    "APTITUDE",
    "HEALTHTRUST",
    "HPG",
    "LIJ",
    "MAGNET",
    "MEDASSETS",
    "MEDIGROUP",
    "MHA",
    "PREMIER",
    "SCMA",
    "TRG",
    "UNITY",
]


def build_should_query(
    name: str = None, addr: str = None, city: str = None, state: str = None
):
    should = []

    if name:
        should.append(
            {
                "text": {
                    "query": name,
                    "path": "alias",
                    "score": {
                        "boost": {"value": 8},
                    },
                }
            }
        )

    if addr:
        should.append(
            {
                "text": {
                    "query": addr,
                    "path": "address",
                    "score": {
                        "boost": {"value": 5},
                    },
                },
            }
        )

    if city:
        should.append(
            {
                "text": {
                    "query": city,
                    "path": "city",
                    "score": {
                        "boost": {"value": 4},
                    },
                },
            }
        )

    if state:
        should.append(
            {
                "text": {
                    "query": state,
                    "path": "state",
                    "score": {
                        "boost": {"value": 2},
                    },
                },
            }
        )

    return should


def build_aggregation_query(
    should: list = None,
    gpo: str = None,
):
    aggregation = []

    if should:
        aggregation.append(
            {
                "$search": {
                    "index": ATLAS_SEARCH_INDEX_NAME,
                    "compound": {
                        "should": should,
                    },
                },
            }
        )

    if gpo:
        aggregation.append(
            {
                "$match": {
                    "group_name": gpo.upper(),
                },
            }
        )

    aggregation.append(
        {
            "$limit": 1,
        }
    )

    aggregation.append(
        {
            "$project": {
                "_id": 1,
                "member_id": 1,
                "alias": 1,
                "name": 1,
                "address": 1,
                "city": 1,
                "score": {"$meta": "searchScore"},
            }
        }
    )

    return aggregation


@lru_cache(maxsize=None)
def find_license_using_atlas_search(
    gpo: str, name: str, addr: str, city: str, state: str
) -> str:
    from db import Roster

    if gpo not in VALID_GPOS:
        return "0|0.0"

    name = name.strip().lower()

    try:
        addr = addr.strip().lower()
    except AttributeError as ae:
        print(ae)
        print(gpo, name, addr, city, state)

        import sys

        sys.exit(1)

    city = city.strip().lower()
    state = state.strip().lower()

    should_query = build_should_query(name=name, addr=addr, city=city, state=state)
    aggregation_query = build_aggregation_query(should_query, gpo)

    result = list(Roster.aggregate(aggregation_query))

    if len(result) > 0:
        member = result[0]

        member_id = member["member_id"]
        score = member["score"]

    return f"{member_id}|{score}"
