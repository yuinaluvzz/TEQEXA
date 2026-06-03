import time
from decimal import Decimal

def now_ts():
    return int(time.time())

def safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default

def decimal_to_int(d):
    return int(Decimal(d))
