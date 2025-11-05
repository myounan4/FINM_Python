# reporting.py
import pandas as pd
from pathlib import Path

class Report:
    def __init__(self, out_md="performance_report.md"):
        self.out_md = out_md
        self.df = pd.DataFrame(columns=["metric","pandas","polars"])

    def add_row(self, metric, pandas_value, polars_value):
        i = len(self.df)
        self.df.loc[i] = [metric, pandas_value, polars_value]

    def write(self):
        Path(self.out_md).parent.mkdir(parents=True, exist_ok=True)
        with open(self.out_md, "w", encoding="utf-8") as f:
            f.write("# Performance Report\n\n")
            f.write(self.df.to_markdown(index=False))
            f.write("\n")
