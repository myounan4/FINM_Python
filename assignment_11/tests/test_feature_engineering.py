
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from feature_engineering import (
    load_market_data,
    add_features,
    add_label_direction,
    build_feature_matrix,
    prepare_dataset,
)


@pytest.fixture
def synthetic_market_csv(tmp_path: Path) -> Path:
    dates = pd.date_range("2024-01-01", periods=20, freq="D")
    data = []
    for ticker in ["AAA", "BBB"]:
        price = 100.0
        for d in dates:
            open_p = price
            close_p = price * (1 + np.random.normal(0, 0.01))
            high_p = max(open_p, close_p) * 1.01
            low_p = min(open_p, close_p) * 0.99
            vol = np.random.randint(1_000, 10_000)
            data.append(
                {
                    "Date": d.strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "Open": open_p,
                    "High": high_p,
                    "Low": low_p,
                    "close": close_p,
                    "Volume": vol,
                }
            )
            price = close_p

    df = pd.DataFrame(data)
    path = tmp_path / "synthetic_market.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def synthetic_features_config(tmp_path: Path) -> Path:
    cfg = {
        "features": [
            "return_1d",
            "return_3d",
            "return_5d",
            "sma_5",
            "sma_10",
            "rsi_14",
            "macd",
        ],
        "label": "direction",
    }
    path = tmp_path / "features_config.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f)
    return path


def test_load_and_add_features(synthetic_market_csv: Path):
    df = load_market_data(synthetic_market_csv)
    assert {"Date", "ticker", "close"}.issubset(df.columns)
    df_feat = add_features(df)
    for col in ["return_1d", "return_3d", "return_5d", "sma_5", "sma_10", "rsi_14", "macd"]:
        assert col in df_feat.columns
    assert df_feat["return_1d"].notna().sum() > 0


def test_add_label_direction(synthetic_market_csv: Path):
    df = load_market_data(synthetic_market_csv)
    df_feat = add_features(df)
    df_lab = add_label_direction(df_feat)
    assert "direction" in df_lab.columns
    assert set(df_lab["direction"].dropna().unique()).issubset({0, 1})


def test_build_feature_matrix(synthetic_market_csv: Path, synthetic_features_config: Path):
    df = load_market_data(synthetic_market_csv)
    df = add_features(df)
    df = add_label_direction(df)
    X, y, meta = build_feature_matrix(df, synthetic_features_config)
    assert len(X) == len(y) == len(meta)
    assert not X.isna().any().any()
    assert not y.isna().any()
    assert "Date" in meta.columns and "ticker" in meta.columns


def test_prepare_dataset_integration(synthetic_market_csv: Path, synthetic_features_config: Path, monkeypatch):
    def fake_load_market_data(path):
        return load_market_data(synthetic_market_csv)

    monkeypatch.setattr("feature_engineering.load_market_data", fake_load_market_data)
    X, y, meta = prepare_dataset("ignored.csv", synthetic_features_config)
    assert len(X) == len(y) == len(meta)
    assert X.shape[1] > 0
    assert y.nunique() <= 2
