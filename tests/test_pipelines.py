"""
Unit tests for pipeline utilities and transformations.
Run: pytest tests/test_pipelines.py -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_quality import check_null_threshold, check_duplicates, validate_schema, run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, label_encode, remove_outliers_iqr


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "Age": [25, 30, None, 45, 50],
        "Sex": ["male", "female", "male", None, "female"],
        "Fare": [7.25, 71.28, 7.92, 53.10, 8.05],
        "Pclass": [3, 1, 3, 1, 3],
        "Survived": [0, 1, 1, 1, 0]
    })


@pytest.fixture
def df_with_duplicates():
    df = pd.DataFrame({"A": [1, 2, 2, 3], "B": ["x", "y", "y", "z"]})
    return df


# ─────────────────────────────────────────────
# Data Quality Tests
# ─────────────────────────────────────────────

def test_null_check_pass(sample_df):
    result = check_null_threshold(sample_df, threshold=0.5)
    assert result["Age"]["status"] == "PASS"  # 20% nulls < 50%


def test_null_check_fail():
    df = pd.DataFrame({"col": [None, None, None, 1, 2]})  # 60% nulls
    result = check_null_threshold(df, threshold=0.10)
    assert result["col"]["status"] == "FAIL"


def test_duplicate_check(df_with_duplicates):
    result = check_duplicates(df_with_duplicates)
    assert result["duplicate_count"] == 1
    assert result["status"] == "WARNING"


def test_schema_validation_pass(sample_df):
    result = validate_schema(sample_df, ["Age", "Sex", "Fare"])
    assert result["status"] == "PASS"
    assert result["missing_columns"] == []


def test_schema_validation_fail(sample_df):
    result = validate_schema(sample_df, ["Age", "NonExistentCol"])
    assert result["status"] == "FAIL"
    assert "NonExistentCol" in result["missing_columns"]


def test_run_all_checks(sample_df):
    report = run_all_checks(sample_df, ["Age", "Sex", "Fare", "Pclass", "Survived"])
    assert "overall_status" in report
    assert report["row_count"] == 5


# ─────────────────────────────────────────────
# Transformation Tests
# ─────────────────────────────────────────────

def test_fill_nulls_numeric(sample_df):
    df = fill_nulls(sample_df, strategy="median")
    assert df["Age"].isnull().sum() == 0


def test_fill_nulls_categorical(sample_df):
    df = fill_nulls(sample_df, strategy="median")
    assert df["Sex"].isnull().sum() == 0


def test_remove_duplicates(df_with_duplicates):
    df = remove_duplicates(df_with_duplicates)
    assert len(df) == 3
    assert df.duplicated().sum() == 0


def test_label_encode(sample_df):
    df = fill_nulls(sample_df)
    df = label_encode(df, ["Sex"])
    assert df["Sex"].dtype in [np.int64, np.int32, int]


def test_remove_outliers_iqr():
    df = pd.DataFrame({"value": [10, 12, 11, 13, 1000, 9, 11]})
    df_clean = remove_outliers_iqr(df, "value")
    assert 1000 not in df_clean["value"].values


def test_pipeline_shape_after_transform(sample_df):
    df = fill_nulls(sample_df)
    df = remove_duplicates(df)
    df = label_encode(df, ["Sex"])
    assert df.shape[0] == 5
    assert df.isnull().sum().sum() == 0
