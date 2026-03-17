import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
from utils.logger import get_logger

logger = get_logger("transformations")


def fill_nulls(df: pd.DataFrame, strategy: str = "median") -> pd.DataFrame:
    """Fill numeric nulls with mean/median, categorical with mode."""
    df = df.copy()
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            if df[col].dtype in [np.float64, np.int64]:
                fill_val = df[col].median() if strategy == "median" else df[col].mean()
                df[col] = df[col].fillna(fill_val)
                logger.info(f"Filled nulls in '{col}' with {strategy}={fill_val:.2f}")
            else:
                fill_val = df[col].mode()[0]
                df[col] = df[col].fillna(fill_val)
                logger.info(f"Filled nulls in '{col}' with mode='{fill_val}'")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows."""
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    after = len(df)
    logger.info(f"Removed {before - after} duplicate rows")
    return df


def label_encode(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Label encode specified categorical columns."""
    df = df.copy()
    le = LabelEncoder()
    for col in columns:
        if col in df.columns:
            df[col] = le.fit_transform(df[col].astype(str))
            logger.info(f"Label encoded column: '{col}'")
    return df


def one_hot_encode(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """One-hot encode specified categorical columns."""
    df = pd.get_dummies(df, columns=columns, drop_first=True)
    logger.info(f"One-hot encoded columns: {columns}")
    return df


def scale_features(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Standard scale specified numeric columns."""
    df = df.copy()
    scaler = StandardScaler()
    df[columns] = scaler.fit_transform(df[columns])
    logger.info(f"Scaled columns: {columns}")
    return df


def remove_outliers_iqr(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Remove outliers using IQR method."""
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    before = len(df)
    df = df[~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]
    logger.info(f"Removed {before - len(df)} outliers from '{column}' using IQR")
    return df.reset_index(drop=True)


def split_multi_value_column(df: pd.DataFrame, column: str, delimiter: str = ",") -> pd.DataFrame:
    """Explode a multi-value column (e.g. genres) into separate rows."""
    df = df.copy()
    df[column] = df[column].str.split(delimiter)
    df = df.explode(column)
    df[column] = df[column].str.strip()
    logger.info(f"Exploded multi-value column: '{column}'")
    return df.reset_index(drop=True)
