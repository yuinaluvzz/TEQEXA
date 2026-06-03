from amm import AMM
from decimal import Decimal

def test_price_calc():
    amm = AMM()
    assert amm._price(Decimal(1000), Decimal(100)) == Decimal("10")
