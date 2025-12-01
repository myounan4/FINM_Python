import sqlite3
import pandas as pd

from data_loader import load_and_validate_market_data
from sqlite_storage import (
    init_sqlite_db,
    populate_db,
    get_tsla_between_dates,
    get_avg_daily_volume_per_ticker,
    get_top3_tickers_by_total_return,
    get_first_last_price_per_ticker_per_day,
)


def test_sqlite_schema_and_inserts(tmp_path):
    """End-to-end test: create DB, insert data, and run queries."""
    db_path = tmp_path / "market_data.db"
    schema_path = "schema.sql"

    market_df, tickers_df = load_and_validate_market_data(
        "market_data_multi.csv", "tickers.csv"
    )

    conn = init_sqlite_db(str(db_path), schema_path)
    populate_db(conn, market_df, tickers_df)

    # Basic integrity checks
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM tickers")
    n_tickers = cur.fetchone()[0]
    assert n_tickers == len(tickers_df)

    cur.execute("SELECT COUNT(*) FROM prices")
    n_prices = cur.fetchone()[0]
    assert n_prices == len(market_df)

    # Query helpers should return non-empty frames
    tsla = get_tsla_between_dates(conn)
    assert not tsla.empty
    assert set(tsla["symbol"]) == {"TSLA"}

    avg_vol = get_avg_daily_volume_per_ticker(conn)
    assert not avg_vol.empty
    assert set(avg_vol["symbol"]) == set(tickers_df["symbol"])

    top3 = get_top3_tickers_by_total_return(conn)
    assert len(top3) == 3

    first_last = get_first_last_price_per_ticker_per_day(conn)
    assert not first_last.empty
