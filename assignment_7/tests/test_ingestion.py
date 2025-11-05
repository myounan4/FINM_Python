from data_loader import load_pandas, load_polars

def test_ingestion_equivalence():
    pdf = load_pandas("market_data-1.csv")
    pld = load_polars("market_data-1.csv")

    assert len(pdf) == pld.height
    assert pdf["symbol"].nunique() == pld["symbol"].n_unique()
    assert pdf["timestamp"].min() == pld["timestamp"].min()
    assert pdf["timestamp"].max() == pld["timestamp"].max()
