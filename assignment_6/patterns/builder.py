
from ..models import PortfolioGroup, Position, Instrument
class PortfolioBuilder:
    def __init__(self,name="root"): self.group=PortfolioGroup(name); self.owner=None
    def add_position(self,instrument:Instrument,quantity:float,price:float):
        self.group.add(Position(instrument,quantity,price)); return self
    def add_subportfolio(self,name:str,builder:"PortfolioBuilder"):
        self.group.add(builder.group); return self
    def set_owner(self,name:str): self.owner=name; return self
    def build(self): return self.group
