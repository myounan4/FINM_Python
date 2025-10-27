
from typing import List, Dict
def summarize_orders(book:List[Dict])->Dict[str,int]:
    out={}
    for o in book: out[(o["symbol"],o["action"])]=out.get((o["symbol"],o["action"]),0)+o["qty"]
    return out
