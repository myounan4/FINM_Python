
import json
from pathlib import Path
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from feature_engineering import prepare_dataset

def load_model_params(path):
    with open(path, "r") as f:
        return json.load(f)

def make_models(params):
    models = {}
    if "LogisticRegression" in params:
        models["LogisticRegression"] = LogisticRegression(**params["LogisticRegression"])
    if "RandomForestClassifier" in params:
        models["RandomForestClassifier"] = RandomForestClassifier(**params["RandomForestClassifier"])
    return models

def evaluate(model, X, y):
    tscv = TimeSeriesSplit(n_splits=5)
    acc = cross_val_score(model, X, y, cv=tscv, scoring="accuracy")
    prec = cross_val_score(model, X, y, cv=tscv, scoring="precision")
    rec = cross_val_score(model, X, y, cv=tscv, scoring="recall")
    model.fit(X, y)
    y_pred = model.predict(X)
    return {
        "cv_accuracy": float(acc.mean()),
        "cv_precision": float(prec.mean()),
        "cv_recall": float(rec.mean()),
        "confusion_matrix": confusion_matrix(y, y_pred).tolist()
    }

def main():
    X, y, meta = prepare_dataset("market_data_ml.csv", "features_config.json")
    params = load_model_params("model_params.json")
    models = make_models(params)
    results = {}
    for name, model in models.items():
        results[name] = evaluate(model, X, y)
        print(name, results[name])
    with open("model_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
