"""
Unit tests for the sales ETL pipeline.
Run with: pytest tests/
"""
import os
import sys
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.transform import clean


class TestClean:
    """Tests for the clean() transformation function."""

    def _sample_df(self):
        return pd.DataFrame({
            "order_id": ["O1", "O2", "O3", "O1"],
            "product_id": ["P1", "P2", "P3", "P1"],   # O1+P1 is duplicate
            "customer_id": ["C1", "C2", "C3", "C1"],
            "customer_name": ["Alice", "Bob", "Carol", "Alice"],
            "region": ["East", "West", "East", "East"],
            "sales": [100.0, None, 200.0, 100.0],       # O2 has null sales
            "profit": [20.0, 10.0, None, 20.0],          # O3 has null profit
            "discount": [0.1, 0.2, None, 0.1],
            "order_date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-01"],
            "ship_date": ["2023-01-05", "2023-01-06", "2023-01-07", "2023-01-05"],
            "product_name": ["Prod A", "Prod B", "Prod C", "Prod A"],
            "category": ["Tech", "Office", "Tech", "Tech"],
            "sub_category": ["Phones", "Paper", "Laptops", "Phones"],
            "quantity": [2, 3, 1, 2],
        })

    def test_removes_null_sales(self):
        df = clean(self._sample_df())
        assert df["sales"].isnull().sum() == 0

    def test_removes_null_profit(self):
        df = clean(self._sample_df())
        assert df["profit"].isnull().sum() == 0

    def test_fills_null_discount(self):
        df = self._sample_df()
        df.loc[0, "discount"] = None
        result = clean(df)
        assert result["discount"].isnull().sum() == 0

    def test_removes_duplicates(self):
        df = clean(self._sample_df())
        assert df.duplicated(subset=["order_id", "product_id"]).sum() == 0

    def test_derives_order_month(self):
        df = clean(self._sample_df())
        assert "order_month" in df.columns
        assert df["order_month"].notna().all()

    def test_derives_order_year(self):
        df = clean(self._sample_df())
        assert "order_year" in df.columns
        assert df["order_year"].notna().all()

    def test_derives_profit_margin(self):
        df = clean(self._sample_df())
        assert "profit_margin" in df.columns
        # profit_margin = profit / sales
        row = df[df["order_id"] == "O1"].iloc[0]
        expected = round(row["profit"] / row["sales"], 4)
        assert abs(row["profit_margin"] - expected) < 0.0001

    def test_zero_sales_profit_margin(self):
        df = self._sample_df()
        df.loc[0, "sales"] = 0.0
        result = clean(df)
        row = result[result["order_id"] == "O1"]
        if not row.empty:
            assert row.iloc[0]["profit_margin"] == 0.0
