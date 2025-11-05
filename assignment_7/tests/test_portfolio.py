import json
import math
from data_loader import load_pandas
from metrics import compute_metrics_pandas
from portfolio import aggregate

def test_portfolio_aggregate_smoke():
    pdf = load_pandas("market_data-1.csv")
    mp = compute_metrics_pandas(pdf)
    with open("portfolio_structure-1.json") as f:
        port = json.load(f)
    out = aggregate(port, mp)
    assert "total_value" in out
    assert "aggregate_volatility" in out
    assert "max_drawdown" in out
    assert isinstance(out["total_value"], (int, float))
    assert not math.isnan(out["total_value"])
