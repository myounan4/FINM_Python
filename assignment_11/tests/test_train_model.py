
import json
from pathlib import Path

import numpy as np
import pytest

from train_model import load_model_params, make_models


@pytest.fixture
def synthetic_model_params(tmp_path: Path) -> Path:
    params = {
        "LogisticRegression": {"max_iter": 200},
        "RandomForestClassifier": {"n_estimators": 10, "max_depth": 3, "random_state": 0},
    }
    path = tmp_path / "model_params.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(params, f)
    return path


def test_make_models_and_shapes(synthetic_model_params: Path):
    params = load_model_params(synthetic_model_params)
    models = make_models(params)
    assert "LogisticRegression" in models
    assert "RandomForestClassifier" in models

    X = np.random.randn(50, 5)
    y = np.random.randint(0, 2, size=50)

    for name, model in models.items():
        model.fit(X, y)
        y_pred = model.predict(X)
        assert y_pred.shape == (50,)
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X)
            assert proba.shape == (50, 2)
