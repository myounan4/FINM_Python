import argparse
import alpaca_trade_api as tradeapi

from alpaca_config import API_KEY_ID, API_SECRET_KEY, BASE_URL
from part1_clean import load_and_clean
from backtester.strategy import (
    MovingAverageCrossoverStrategy,
    MACrossoverConfig,
    RSIMeanReversionStrategy,
    RSIMeanReversionConfig,
    MomentumBreakoutStrategy,
    MomentumBreakoutConfig,
)

print("Note that I wasnt' able to pull live data using aplaca which is why i rlied and what i pulled using yahoo fiannce. ")

def build_strategy(name: str, args):
    name = name.lower()

    if name == "mac":
        cfg = MACrossoverConfig(
            ma_fast=args.ma_fast,
            ma_slow=args.ma_slow,
            units=args.units,
        )
        return MovingAverageCrossoverStrategy(cfg), f"MAC {cfg}"

    elif name == "rsi":
        cfg = RSIMeanReversionConfig(
            rsi_period=args.rsi_period,
            oversold=args.rsi_oversold,
            overbought=args.rsi_overbought,
            units=args.units,
        )
        return RSIMeanReversionStrategy(cfg), f"RSI {cfg}"

    elif name == "mom":
        cfg = MomentumBreakoutConfig(
            lookback=args.mom_lookback,
            breakout_pct=args.mom_breakout_pct,
            units=args.units,
        )
        return MomentumBreakoutStrategy(cfg), f"MOM {cfg}"

    raise ValueError(f"Unknown strategy: {name}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a signal from local data and send a PAPER order via Alpaca."
    )

    parser.add_argument(
        "--data-path",
        type=str,
        default="data/market_data.csv",
        help="Local CSV with OHLCV data (e.g. from yfinance / part1_download.py)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["mac", "rsi", "mom"],
        default="mac",
    )
    parser.add_argument("--units", type=int, default=10)

    # MAC
    parser.add_argument("--ma-fast", type=int, default=20)
    parser.add_argument("--ma-slow", type=int, default=60)

    # RSI
    parser.add_argument("--rsi-period", type=int, default=14)
    parser.add_argument("--rsi-oversold", type=float, default=30.0)
    parser.add_argument("--rsi-overbought", type=float, default=70.0)

    # Momentum
    parser.add_argument("--mom-lookback", type=int, default=50)
    parser.add_argument("--mom-breakout-pct", type=float, default=0.01)

    parser.add_argument("--symbol", type=str, default="AAPL")

    return parser.parse_args()


def main():
    args = parse_args()

    # 1) Load your existing cleaned data (from yfinance)
    df = load_and_clean(args.data_path)

    # 2) Build strategy and prepare features
    strat, label = build_strategy(args.strategy, args)
    df = strat.prepare_data(df)

    # 3) Take the last bar and generate a signal
    last_row = df.iloc[-1].to_dict()
    side, qty = strat.generate_order(last_row)

    if side is None or qty <= 0:
        print(f"No trade generated from latest bar using {label}.")
        return

    print(f"Signal from local data: {side} {qty} {args.symbol} using {label}")

    # 4) Submit PAPER order via Alpaca
    api = tradeapi.REST(API_KEY_ID, API_SECRET_KEY, BASE_URL, api_version="v2")

    order = api.submit_order(
        symbol=args.symbol,
        qty=qty,
        side=side.lower(),  # "buy" or "sell"
        type="market",
        time_in_force="day",
    )

    print("Submitted Alpaca PAPER order:")
    print(order)


if __name__ == "__main__":
    main()
