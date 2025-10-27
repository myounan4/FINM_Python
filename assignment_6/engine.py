
from typing import List, Dict
from .patterns.strategy import Strategy
from .patterns.observer import SignalPublisher
from .patterns.command import ExecuteOrderCommand, CommandInvoker
class Engine:
    def __init__(self,strategy:Strategy,publisher:SignalPublisher):
        self.strategy=strategy; self.publisher=publisher; self.invoker=CommandInvoker(); self.order_book: List[Dict]=[]
    def on_tick(self,tick):
        for sig in self.strategy.generate_signals(tick):
            self.publisher.notify(sig)
            cmd=ExecuteOrderCommand(self.order_book,{"symbol":sig["symbol"],"action":sig["action"],"qty":sig["qty"]}); self.invoker.do(cmd)
    def undo_last(self): self.invoker.undo()
    def redo_last(self): self.invoker.redo()
