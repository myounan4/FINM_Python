# Query Tasks

## SQLite3

### 1. Retrieve all data for TSLA between 2025-11-17 and 2025-11-18

**SQL:**

```sql
SELECT p.timestamp,
       t.symbol,
       p.open, p.high, p.low, p.close, p.volume
FROM prices p
JOIN tickers t ON p.ticker_id = t.ticker_id
WHERE t.symbol = 'TSLA'
  AND DATE(p.timestamp) BETWEEN '2025-11-17' AND '2025-11-18'
ORDER BY p.timestamp;
```

**Result summary:**

- Rows returned: **782**
- First row: 2025-11-17 09:30:00 close=268.07, volume=1609
- Last row: 2025-11-18 16:00:00 close=266.68, volume=1821

---

### 2. Calculate average daily volume per ticker

**SQL:**

```sql
WITH daily AS (
  SELECT t.symbol,
         DATE(p.timestamp) AS trade_date,
         SUM(p.volume) AS daily_volume
  FROM prices p
  JOIN tickers t ON p.ticker_id = t.ticker_id
  GROUP BY t.symbol, DATE(p.timestamp)
)
SELECT symbol,
       AVG(daily_volume) AS avg_daily_volume
FROM daily
GROUP BY symbol
ORDER BY symbol;
```

**Result (averaged over trading days in the sample):**

| symbol | avg_daily_volume |
|--------|------------------|
| AAPL | 1082222.6 |
| AMZN | 1076588.8 |
| GOOG | 1071402.8 |
| MSFT | 1050441.4 |
| TSLA | 1085973.0 |

---

### 3. Identify top 3 tickers by return over the full period

**SQL:**

```sql
WITH ordered AS (
  SELECT p.*, t.symbol,
         ROW_NUMBER() OVER (
           PARTITION BY p.ticker_id
           ORDER BY p.timestamp ASC
         ) AS rn_asc,
         ROW_NUMBER() OVER (
           PARTITION BY p.ticker_id
           ORDER BY p.timestamp DESC
         ) AS rn_desc
  FROM prices p
  JOIN tickers t ON p.ticker_id = t.ticker_id
),
first_last AS (
  SELECT symbol,
         MAX(CASE WHEN rn_asc = 1 THEN close END) AS first_close,
         MAX(CASE WHEN rn_desc = 1 THEN close END) AS last_close
  FROM ordered
  GROUP BY symbol
)
SELECT symbol,
       first_close,
       last_close,
       (last_close - first_close) / first_close AS total_return
FROM first_last
ORDER BY total_return DESC
LIMIT 3;
```

**Result (full ranking):**

| symbol | first_close | last_close | total_return |
|--------|-------------|-----------:|-------------:|
| MSFT | 183.89 | 245.70 | 0.3361 |
| AAPL | 270.88 | 334.57 | 0.2351 |
| GOOG | 139.43 | 153.90 | 0.1038 |
| TSLA | 268.07 | 292.32 | 0.0905 |
| AMZN | 125.46 | 77.16 | -0.3850 |

Top 3 tickers by total return: **MSFT, AAPL, GOOG**.

---

### 4. First and last trade price for each ticker per day

**SQL:**

```sql
WITH ordered AS (
  SELECT p.*, t.symbol,
         DATE(p.timestamp) AS trade_date,
         ROW_NUMBER() OVER (
           PARTITION BY t.symbol, DATE(p.timestamp)
           ORDER BY p.timestamp ASC
         ) AS rn_asc,
         ROW_NUMBER() OVER (
           PARTITION BY t.symbol, DATE(p.timestamp)
           ORDER BY p.timestamp DESC
         ) AS rn_desc
  FROM prices p
  JOIN tickers t ON p.ticker_id = t.ticker_id
)
SELECT symbol,
       trade_date,
       MAX(CASE WHEN rn_asc = 1 THEN close END) AS first_close,
       MAX(CASE WHEN rn_desc = 1 THEN close END) AS last_close
FROM ordered
GROUP BY symbol, trade_date
ORDER BY symbol, trade_date;
```

**Sample output (first few days for each ticker):**

| symbol | trade_date | first_close | last_close |
|--------|------------|------------:|-----------:|
| AAPL | 2025-11-17 | 270.88 | 287.68 |
| AAPL | 2025-11-18 | 287.48 | 289.52 |
| AAPL | 2025-11-19 | 288.80 | 295.87 |
| AAPL | 2025-11-20 | 296.99 | 319.43 |
| AAPL | 2025-11-21 | 319.63 | 334.57 |
| AMZN | 2025-11-17 | 125.46 | 141.03 |
| AMZN | 2025-11-18 | 140.06 | 133.47 |
| AMZN | 2025-11-19 | 131.98 | 94.76 |
| AMZN | 2025-11-20 | 95.48 | 94.05 |
| AMZN | 2025-11-21 | 93.05 | 77.16 |
| GOOG | 2025-11-17 | 139.43 | 105.00 |
| GOOG | 2025-11-18 | 104.20 | 113.78 |
| GOOG | 2025-11-19 | 114.55 | 139.30 |
| GOOG | 2025-11-20 | 139.73 | 162.43 |
| GOOG | 2025-11-21 | 163.00 | 153.90 |
| MSFT | 2025-11-17 | 183.89 | 215.36 |
| MSFT | 2025-11-18 | 214.90 | 242.24 |
| MSFT | 2025-11-19 | 241.16 | 253.08 |
| MSFT | 2025-11-20 | 253.28 | 284.98 |
| MSFT | 2025-11-21 | 286.81 | 245.70 |
| TSLA | 2025-11-17 | 268.07 | 286.86 |
| TSLA | 2025-11-18 | 286.16 | 266.68 |
| TSLA | 2025-11-19 | 266.19 | 272.15 |
| TSLA | 2025-11-20 | 271.09 | 265.61 |
| TSLA | 2025-11-21 | 266.75 | 292.32 |

---

## Parquet (using pandas-equivalent logic)

> In this environment, Parquet engines (`pyarrow` / `fastparquet`) are not installed,
> so the computations below are demonstrated using in-memory pandas. On your local
> machine, these same operations would be run on Parquet files.

### 1. Load all data for AAPL and compute 5-minute rolling average of close price

Python logic (after loading the AAPL Parquet partition and sorting by timestamp):

```python
aapl = df_aapl.sort_values("timestamp")
aapl["rolling_5min_close"] = aapl["close"].rolling(window=5).mean()
```

First few rows:

| timestamp | close | rolling_5min_close |
|-----------|-------|--------------------|
| 2025-11-17 09:30:00 | 270.88 |  |
| 2025-11-17 09:31:00 | 269.24 |  |
| 2025-11-17 09:32:00 | 270.86 |  |
| 2025-11-17 09:33:00 | 269.28 |  |
| 2025-11-17 09:34:00 | 269.32 | 269.916 |
| 2025-11-17 09:35:00 | 270.23 | 269.786 |

The first 4 rows are NaN because a 5-row (5-minute) window is required before the
rolling average is defined.

---

### 2. Compute 5-day rolling volatility (std dev) of returns for each ticker

Daily close and returns:

```python
daily_close = (
    full_df.sort_values(["ticker", "timestamp"])
           .groupby(["ticker", "date"])["close"]
           .last()
           .reset_index()
)
daily_close["return"] = daily_close.groupby("ticker")["close"].pct_change()
```

Example (first few rows):

```text
ticker       date  close    return  vol_5d_min1
  AAPL 2025-11-17 287.68       NaN          NaN
  AAPL 2025-11-18 289.52  0.006396          NaN
  AAPL 2025-11-19 295.87  0.021933     0.010986
  AAPL 2025-11-20 319.43  0.079630     0.038586
  AAPL 2025-11-21 334.57  0.047397     0.032018
  AMZN 2025-11-17 141.03       NaN          NaN
  AMZN 2025-11-18 133.47 -0.053606          NaN
  AMZN 2025-11-19  94.76 -0.290028     0.167176
  AMZN 2025-11-20  94.05 -0.007493     0.151574
  AMZN 2025-11-21  77.16 -0.179585     0.127649
```

5-day rolling volatility (with `min_periods=1` to get illustrative values in this short sample):

```python
daily_close["vol_5d"] = (
    daily_close.groupby("ticker")["return"]
               .rolling(window=5, min_periods=1)
               .std()
               .reset_index(level=0, drop=True)
)
```

Final (latest) 5-day rolling vols per ticker in this dataset:

| ticker | vol_5d (min_periods=1) |
|--------|------------------------|
| AAPL | 0.0320 |
| AMZN | 0.1276 |
| GOOG | 0.1200 |
| MSFT | 0.1242 |
| TSLA | 0.0728 |

In a longer sample you would typically use `min_periods=5` so that each volatility
estimate is based on a full 5 trading days of returns.

---

### 3. Compare query time and file size with SQLite3 for Task 1

Measured in this environment:

- `market_data_multi.csv` size: **559040 bytes**
- `market_data.db` size: **688128 bytes**

Approximate query times for *"TSLA between 2025-11-17 and 2025-11-18"*:

- SQLite3 (`SELECT` with `JOIN` + `DATE` filter): **0.006364 s**
- Parquet-style in-memory filter (simulating Parquet load + filter in pandas): **0.002138 s**

Both approaches return **782** rows for that date range. On a real
system with Parquet files and `pyarrow`, the Parquet version can often be faster
for large, columnar analytics workloads, especially when combined with
partition pruning (e.g., `ticker=TSLA` partitions).
