
import json, csv
from pathlib import Path
from .patterns.singleton import Config
from .patterns.factory import InstrumentFactory
from .patterns.builder import PortfolioBuilder
from .patterns.strategy import MeanReversionStrategy, BreakoutStrategy
from .patterns.observer import SignalPublisher, LoggerObserver, AlertObserver
from .data_loader import YahooFinanceAdapter, BloombergXMLAdapter, load_instruments_csv
from .models import VolatilityDecorator, BetaDecorator, DrawdownDecorator, Instrument
from .engine import Engine
from .reporting import summarize_orders
def run(project_root: str = None):
    base=Path(project_root or Path(__file__).resolve().parent); data=base/'data'
    cfg=Config().load(str(data/'config.json')).data
    params=json.load(open(data/'strategy_params.json'))
    inst_rows=load_instruments_csv(data/'instruments.csv')
    instruments={}
    for row in inst_rows:
        inst=InstrumentFactory.create_instrument(row); instruments[inst.symbol]=inst
    md_csv=list(csv.DictReader(open(data/'market_data.csv'))) if (data/'market_data.csv').exists() else []
    for r in md_csv:
        sym=r['symbol']; px=float(r['price'])
        if sym in instruments: instruments[sym].add_price(px)
    mr=MeanReversionStrategy(**params.get('MeanReversionStrategy',{}))
    bo=BreakoutStrategy(**params.get('BreakoutStrategy',{}))
    strategy = mr if cfg.get('engine.strategy','mr')=='mr' else bo
    pub=SignalPublisher(); log=[]; logger=LoggerObserver(log); alert=AlertObserver(threshold_qty=cfg.get('alerts.min_qty',500))
    pub.attach(logger); pub.attach(alert)
    eng=Engine(strategy,pub)
    y=YahooFinanceAdapter(data/'external_data_yahoo.json'); b=BloombergXMLAdapter(data/'external_data_bloomberg.xml')
    sample_symbol=next(iter(instruments)) if instruments else 'SPY'
    feed=(y.get_data(sample_symbol) or b.get_data(sample_symbol))[:100]
    for t in feed: eng.on_tick(t)
    decorated: Instrument = DrawdownDecorator(BetaDecorator(VolatilityDecorator(instruments[sample_symbol]), market_returns=[0.0]*200))
    metrics=decorated.get_metrics()
    return {'orders':eng.order_book,'orders_summary':summarize_orders(eng.order_book),'signals_logged':len(log),'alerts':len(alert.alerts),'metrics':metrics}
if __name__=='__main__':
    print(run())
