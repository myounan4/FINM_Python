
from typing import List, Dict, Protocol
class Observer(Protocol):
    def update(self,signal:Dict): ...
class SignalPublisher:
    def __init__(self): self._obs: List[Observer]=[]
    def attach(self,o:Observer): self._obs.append(o)
    def detach(self,o:Observer): self._obs.remove(o)
    def notify(self,signal:Dict):
        for o in list(self._obs): o.update(signal)
class LoggerObserver:
    def __init__(self,log_list:List[Dict]): self.log=log_list
    def update(self,signal:Dict): self.log.append(signal)
class AlertObserver:
    def __init__(self,threshold_qty=500): self.th=threshold_qty; self.alerts=[]
    def update(self,signal:Dict):
        if abs(signal.get("qty",0))>=self.th: self.alerts.append(signal)
