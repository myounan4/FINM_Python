
import json, threading
class Config:
    _inst=None; _lock=threading.Lock()
    def __new__(cls,*a,**k):
        with cls._lock:
            if cls._inst is None: cls._inst=super().__new__(cls)
        return cls._inst
    def load(self,path:str):
        with open(path,"r") as f: self.data=json.load(f); return self
    def get(self,key,default=None):
        cur=self.data
        for part in key.split("."):
            if isinstance(cur,dict) and part in cur: cur=cur[part]
            else: return default
        return cur
