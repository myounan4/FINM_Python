import os
import yfinance as yf


def download_intraday_data(
    ticker: str = "AAPL",
    period: str = "7d",
    interval: str = "1m",
    out_path: str = "data/market_data.csv",
):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    data = yf.download(tickers=ticker, period=period, interval=interval)
    data = data.reset_index()  # Datetime is index -> column

    # Keep standard columns
    data = data[["Datetime", "Open", "High", "Low", "Close", "Volume"]]
    data.to_csv(out_path, index=False)
    print(f"Saved {len(data)} rows of {ticker} intraday data to {out_path}")


if __name__ == "__main__":
    download_intraday_data()
