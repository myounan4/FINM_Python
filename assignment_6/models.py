
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
@dataclass
class MarketDataPoint:
    symbol: str; ts: str; price: float; volume: float=0.0; extra: Dict[str,Any]=field(default_factory=dict)
class Instrument:
    def __init__(self, symbol: str, name: str, meta: Optional[Dict[str,Any]]=None):
        self.symbol, self.name, self.meta = symbol, name, meta or {}; self.prices: List[float]=[]
    def add_price(self, p: float): self.prices.append(float(p))
    def get_metrics(self) -> Dict[str,Any]:
        last = self.prices[-1] if self.prices else float("nan"); return {"symbol":self.symbol,"last":last}
    def __repr__(self): return f"{self.__class__.__name__}({self.symbol})"
class Stock(Instrument): ...
class Bond(Instrument): ...
class ETF(Instrument): ...
class PortfolioComponent:
    def get_value(self) -> float: raise NotImplementedError
    def get_positions(self) -> List['Position']: raise NotImplementedError
class Position(PortfolioComponent):
    def __init__(self, instrument: Instrument, quantity: float, price: float):
        self.instrument, self.quantity, self.price = instrument, quantity, price
    def get_value(self) -> float:
        px = self.instrument.prices[-1] if self.instrument.prices else self.price
        return self.quantity*px
    def get_positions(self) -> List['Position']: return [self]
class PortfolioGroup(PortfolioComponent):
    def __init__(self, name: str):
        self.name=name; self.children: List[PortfolioComponent]=[]
    def add(self, comp: PortfolioComponent):
        self.children.append(comp); return self
    def get_value(self) -> float: return sum(c.get_value() for c in self.children)
    def get_positions(self) -> List['Position']:
        out: List[Position]=[]
        for c in self.children: out.extend(c.get_positions())
        return out
class InstrumentDecorator(Instrument):
    def __init__(self, inner: Instrument):
        # Don't call super().__init__ here; it would trigger the prices setter too early.
        self.inner = inner
        self.symbol = inner.symbol
        self.name = inner.name
        self.meta = inner.meta

    @property
    def prices(self):
        return self.inner.prices

    @prices.setter
    def prices(self, v):
        self.inner.prices = v

    def add_price(self, p: float):
        self.inner.add_price(p)

    def get_metrics(self) -> Dict[str, Any]:
        return self.inner.get_metrics().copy()

class VolatilityDecorator(InstrumentDecorator):
    def get_metrics(self):
        m=super().get_metrics()
        if len(self.prices)>1:
            rets=[self.prices[i]/self.prices[i-1]-1.0 for i in range(1,len(self.prices))]
            import statistics as st; m["volatility"]=st.pstdev(rets)*(12**0.5)
        else: m["volatility"]=float("nan")
        return m
class BetaDecorator(InstrumentDecorator):
    def __init__(self, inner: Instrument, market_returns=None):
        super().__init__(inner); self.market_returns=market_returns
    def get_metrics(self):
        m=super().get_metrics()
        if self.market_returns and len(self.prices)>1:
            rs=[self.prices[i]/self.prices[i-1]-1.0 for i in range(1,len(self.prices))]
            n=min(len(rs),len(self.market_returns)); x=self.market_returns[-n:]; y=rs[-n:]
            mx=sum(x)/n; my=sum(y)/n
            cov=sum((x[i]-mx)*(y[i]-my) for i in range(n))/n; var=sum((x[i]-mx)**2 for i in range(n))/n
            m["beta"]=cov/var if var!=0 else float("nan")
        else: m["beta"]=float("nan")
        return m
class DrawdownDecorator(InstrumentDecorator):
    def get_metrics(self):
        m=super().get_metrics(); peak=-1e18; dd=0.0
        for p in self.prices:
            peak=max(peak,p); 
            if peak>0: dd=min(dd,p/peak-1.0)
        m["max_drawdown"]=dd; return m
