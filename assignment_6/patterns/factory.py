
from typing import Dict, Any
from ..models import Stock, Bond, ETF, Instrument
class InstrumentFactory:
    @staticmethod
    def create_instrument(data: Dict[str,Any]) -> Instrument:
        t=(data.get("type") or data.get("Type") or "").lower()
        symbol=data.get("symbol") or data.get("Symbol"); name=data.get("name") or data.get("Name") or symbol
        if t=="stock": return Stock(symbol,name)
        if t=="bond": return Bond(symbol,name)
        if t=="etf": return ETF(symbol,name)
        return Stock(symbol,name)
