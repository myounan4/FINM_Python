
import json, xml.etree.ElementTree as ET, csv
from typing import List
from .models import MarketDataPoint
class YahooFinanceAdapter:
    def __init__(self,path): self.path=path
    def get_data(self,symbol:str)->List[MarketDataPoint]:
        with open(self.path,"r") as f: data=json.load(f)
        out=[]
        for row in data.get(symbol,[]):
            out.append(MarketDataPoint(symbol,row["date"],float(row["price"]),float(row.get("volume",0)),row))
        return out
class BloombergXMLAdapter:
    def __init__(self,path): self.path=path
    def get_data(self,symbol:str)->List[MarketDataPoint]:
        tree=ET.parse(self.path); root=tree.getroot(); out=[]
        for sec in root.findall(".//security"):
            if sec.get("symbol")==symbol:
                for r in sec.findall("row"):
                    ts=r.findtext("date"); pr=float(r.findtext("price")); vol=float(r.findtext("volume") or 0)
                    out.append(MarketDataPoint(symbol,ts,pr,vol,{}))
        return out
def load_instruments_csv(path):
    with open(path,newline="") as f: return list(csv.DictReader(f))
