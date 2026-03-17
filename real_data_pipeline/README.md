# Real Data ETL Pipeline — Kaggle Datasets

Production-ready ETL pipeline using **real Kaggle datasets** downloaded from public mirrors.

## Datasets (Real Data)

| Dataset | Source | Rows | Use Case |
|---------|--------|------|----------|
| Titanic | public mirror | 891 | Survival prediction |
| House Prices | public mirror | 20,640 | Price prediction |
| Netflix Titles | public mirror | 7,787 | Content analytics |

## Pipeline Flow

```
data/bronze/ (raw) → pipeline.py → data/silver/ (cleaned) → data/gold/ (features)
```

## Transformations Applied

### All Datasets
- Fill all nulls with `0`
- Normalize column names (lowercase, underscores)

### Titanic
- Label encode `Sex` (male=1, female=0)
- Feature: `familysize`, `isalone`, `farebin`

### House Prices
- Label encode `ocean_proximity`
- Features: `rooms_per_household`, `bedrooms_per_room`, `population_per_household`

### Netflix
- Parse `date_added` → `added_year`, `added_month`
- Extract `primary_country` from comma-separated list
- Gold: content by type×year, country stats

## Quick Start

```bash
pip install -r requirements_pipeline.txt
python3 pipeline.py --bronze-path data/bronze --output-path data
```

## Output Files

```
data/
├── bronze/           ← raw downloaded CSVs
├── silver/           ← cleaned data
│   ├── titanic_clean.csv
│   ├── house_prices_clean.csv
│   └── netflix_clean.csv
└── gold/             ← feature-engineered / aggregated
    ├── titanic_features.csv
    ├── house_prices_features.csv
    ├── netflix_aggregated.csv
    └── netflix_country_stats.csv
```
