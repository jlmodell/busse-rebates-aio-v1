from enum import Enum

from pydantic import BaseModel

# {
#   "_id": {
#     "$oid": "62587222c37e81dca59a1916"
#   },
#   "period": "MARCH2022-AMERICANMEDSUPPLY",
#   "name": "the best u now",
#   "addr": "4101 nw third court",
#   "city": "plantation",
#   "state": "fl",
#   "gpo": "MEDASSETS",
#   "license": "1E2Z",
#   "searchScore": 115.37948608398438,
#   "contract": "R9510G1",
#   "claim_nbr": "1028213-1",
#   "order_nbr": "1028213-1",
#   "invoice_nbr": "1028213-1",
#   "invoice_date": {
#     "$date": "2022-03-18T00:00:00.000Z"
#   },
#   "part": "298",
#   "unit_rebate": 20.85,
#   "ship_qty": 1,
#   "uom": "CA",
#   "ship_qty_as_cs": 1,
#   "rebate": 20.85,
#   "cost": 28.5,
#   "check_license": false,
#   "postal": "0",
#   "__date__": {
#     "$date": "2022-03-01T00:00:00.000Z"
#   }
# }

#   'APTITUDE',  'HEALTHTRUST',
#   'HPG',       'LIJ',
#   'MAGNET',    'MEDASSETS',
#   'MEDIGROUP', 'MHA',
#   'PREMIER',   'SCMA',
#   'TRG',       'UNITY'


class GPO(str, Enum):
    MEDASSETS = "MEDASSETS"
    PREMIER = "PREMIER"
    VIZIENT = "MEDASSETS"
    MAGNET = "MAGNET"
    APTITUDE = "APTITUDE"
    HPG = "HPG"
    LIJ = "LIJ"
    MEDIGROUP = "MEDIGROUP"
    MHA = "MHA"
    SCMA = "SCMA"
    TRG = "TRG"
    UNITY = "UNITY"
    HEALTHTRUST = "HEALTHTRUST"
    INJX = "INJX"


class Tracing(BaseModel):
    period: str
    name: str
    addr: str
    city: str
    state: str
    gpo: GPO
    license: str
    searchScore: float
    contract: str
    claim_nbr: str
    order_nbr: str
    invoice_nbr: str
    invoice_date: str
    part: str
    unit_rebate: float
    ship_qty: int
    uom: str
    ship_qty_as_cs: int
    rebate: float
    cost: float
    check_license: bool
    postal: str
    __date__: str
