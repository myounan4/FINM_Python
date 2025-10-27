
from typing import List, Dict
class Command:
    def execute(self): ...
    def undo(self): ...
class ExecuteOrderCommand(Command):
    def __init__(self,book:List[Dict],order:Dict): self.book, self.order=book, order
    def execute(self): self.book.append(self.order.copy()); return True
    def undo(self):
        if self.order in self.book: self.book.remove(self.order); return True
        return False
class CommandInvoker:
    def __init__(self): self.done: List[Command]=[]; self.undone: List[Command]=[]
    def do(self,cmd:Command):
        if cmd.execute(): self.done.append(cmd); self.undone.clear()
    def undo(self):
        if self.done: c=self.done.pop(); c.undo(); self.undone.append(c)
    def redo(self):
        if self.undone: c=self.undone.pop(); c.execute(); self.done.append(c)
