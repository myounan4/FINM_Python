import argparse

from part1_clean import load_and_clean
from backtester.backtest import Backtester
from backtester.strategy import (
    MovingAverageCrossoverStrategy,
    MACrossoverConfig,
    RSIMeanReversionStrategy,
    RSIMeanReversionConfig,
    MomentumBreakoutStrategy,
    MomentumBreakoutConfig,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Run backtests with different strategies.")
    parser.add_argument(
        "--data-path",
        type=str,
        default="data/market_data.csv",
        help="Path to raw market data CSV",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["mac", "rsi", "mom"],
        default="mac",
        help="Which strategy to run",
    )
    parser.add_argument("--starting-cash", type=float, default=100_000.0)

    # MAC hyperparameters
    parser.add_argument("--ma-fast", type=int, default=20)
    parser.add_argument("--ma-slow", type=int, default=60)
    parser.add_argument("--mac-units", dest="mac_units", type=int, default=10)

    # RSI hyperparameters
    parser.add_argument("--rsi-period", type=int, default=14)
    parser.add_argument("--rsi-oversold", type=float, default=30.0)
    parser.add_argument("--rsi-overbought", type=float, default=70.0)
    parser.add_argument("--rsi-units", type=int, default=10)

    # Momentum hyperparameters
    parser.add_argument("--mom-lookback", type=int, default=50)
    parser.add_argument("--mom-breakout-pct", type=float, default=0.01)
    parser.add_argument("--mom-units", type=int, default=10)

    return parser.parse_args()


def build_strategy(args):
    if args.strategy == "mac":
        cfg = MACrossoverConfig(
            ma_fast=args.ma_fast,
            ma_slow=args.ma_slow,
            units=args.mac_units,
        )
        strategy = MovingAverageCrossoverStrategy(cfg)
        title = f"MA Crossover (fast={cfg.ma_fast}, slow={cfg.ma_slow}, units={cfg.units})"
    elif args.strategy == "rsi":
        cfg = RSIMeanReversionConfig(
            rsi_period=args.rsi_period,
            oversold=args.rsi_oversold,
            overbought=args.rsi_overbought,
            units=args.rsi_units,
        )
        strategy = RSIMeanReversionStrategy(cfg)
        title = (
            f"RSI Mean-Reversion (period={cfg.rsi_period}, "
            f"oversold={cfg.oversold}, overbought={cfg.overbought}, units={cfg.units})"
        )
    else:  # "mom"
        cfg = MomentumBreakoutConfig(
            lookback=args.mom_lookback,
            breakout_pct=args.mom_breakout_pct,
            units=args.mom_units,
        )
        strategy = MomentumBreakoutStrategy(cfg)
        title = (
            f"Momentum Breakout (lookback={cfg.lookback}, "
            f"breakout_pct={cfg.breakout_pct}, units={cfg.units})"
        )

    return strategy, title


def main():
    args = parse_args()
    df = load_and_clean(args.data_path)
    strategy, title = build_strategy(args)

    bt = Backtester(df, strategy=strategy, starting_cash=args.starting_cash)
    bt.run()
    bt.report(title=title)


if __name__ == "__main__":
    main()
