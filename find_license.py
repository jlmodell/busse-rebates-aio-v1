from functools import lru_cache
from typing import Tuple

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
    "INJX",
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


def add_to_alias_array(name: str, original_name: str, doc: dict) -> Tuple[str, float]:
    from db import Roster

    if (
        original_name != name or original_name != doc["name"]
    ) and original_name not in doc["alias"]:
        Roster.update_one(
            {"_id": doc["_id"]}, {"$push": {"alias": original_name.lower().strip()}}
        )


@lru_cache(maxsize=1024)
def find_license(
    group: str = "",
    name: str = None,
    address: str = None,
    city: str = None,
    state: str = None,
    original_name: str = "",
    debug: bool = False,
) -> str:
    from db import Roster

    member_id: str = "0"
    score = 0.0

    if group not in VALID_GPOS:
        return member_id, score

    should = []

    if name:
        should.append(
            {
                "text": {
                    "query": name.lower().strip(),
                    "path": "alias",
                    "score": {
                        "boost": {"value": 8},
                    },
                }
            }
        )

    if address:
        should.append(
            {
                "text": {
                    "query": address.lower().strip(),
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
                    "query": city.lower().strip(),
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
                    "query": state.lower().strip(),
                    "path": "state",
                    "score": {
                        "boost": {"value": 2},
                    },
                },
            }
        )

    # print("should", should)

    docs = list(
        Roster.aggregate(
            [
                {
                    "$search": {
                        "index": ATLAS_SEARCH_INDEX_NAME,
                        "compound": {
                            "should": should,
                        },
                    },
                },
                {
                    "$match": {
                        "group_name": group.upper(),
                    },
                },
                {
                    "$limit": 1,
                },
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
                },
            ]
        )
    )

    # print("docs", docs)

    if docs:
        member_id = docs[0]["member_id"]
        score = docs[0]["score"]

        if debug:
            print("\n*************************************************************")
            print("*************************************************************\n")
            for doc in docs:
                print(doc)
                print("\n")
            print("\n*************************************************************")
            print("*************************************************************\n")

        # add_to_alias_array(name, original_name, docs[0])

    return member_id, score
