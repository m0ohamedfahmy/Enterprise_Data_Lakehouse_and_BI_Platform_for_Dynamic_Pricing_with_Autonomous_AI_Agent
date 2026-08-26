import pandas as pd
import numpy as np
import statsmodels.api as sm
import warnings
from dataclasses import dataclass
import joblib
from lightgbm import LGBMRegressor 
from sklearn.metrics import mean_absolute_error
from dremio_simple_query.connectv2 import DremioConnection
import os


features = [
    # Categories 
    'category', 'sub_category', 'store_region',
    
    # Prices & Margins
    'log_unit_retail', 'avg_unit_cost', 'avg_competitor_price',
    'min_competitor_price', 'max_competitor_price',
    'competitor_price_ratio', 'price_difference', 'competitor_price_gap_pct',
    'current_profit_margin_pct', 'is_higher_than_competitor', 'is_undercut',
    
    # Lags & Rollings
    'quantity_lag_1_week', 'quantity_lag_4_weeks', 
    'quantity_rolling_avg_4_weeks', 'price_change_pct_weekly', 'price_rolling_avg_4_weeks',
    
    # Seasonality
    'month', 'quarter', 'week_of_year'
]

categorical_cols = ['category', 'sub_category', 'store_region', 'is_higher_than_competitor', 'is_undercut']
    
target =  "log_quantity"  


@dataclass
class TrainResult:
    model: LGBMRegressor
    test_mae: float
    train_mae: float
    naive_elasticity: float
    # simple log-log OLS coefficient, sanity-check only
    # LightGBM maps pandas categorical columns to integer codes internally
    # based on the exact set of categories seen at fit time. If prediction-
    # time data has a different (even just differently-ordered) set of
    # categories, the same code can silently refer to a different category -
    # wrong predictions with no error raised. Persisting these dtypes and
    # re-applying them at prediction time (see price_optimizer.py) is what
    # prevents that.
    category_dtypes: dict

def connection():
    dremio = DremioConnection(
    location="grpc://dremio:32010",  
    username="mofah",
    password="fahmy12345")
    return dremio
    


def read_data(conn) -> pd.DataFrame:
    df = conn.toPandas("SELECT * FROM nessie.marts.ml_dynamic_competitive_pricing")
    return df
def read_data_api_predict(conn,product_id: int) -> pd.DataFrame:
    
    query = f"""
        WITH ranked_sales AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                    PARTITION BY store_id 
                    ORDER BY sales_week DESC ) as rn
            FROM nessie.marts.ml_dynamic_competitive_pricing
            WHERE product_id = {product_id} )
        SELECT * , DATE_ADD(sales_week, INTERVAL '1' DAY) AS sales_week_next
        FROM ranked_sales
        WHERE rn = 1
        """
    df = conn.toPandas(query)

    if "sales_week_next" in df.columns:
        df['sales_week'] = df['sales_week_next']
        df = df.drop(columns=["sales_week_next"])
    if "rn" in df.columns:
        df = df.drop(columns=["rn"])
    return df    
def Feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(['product_id', 'store_key', 'sales_week']).reset_index(drop=True)
    grouped = df.groupby(['product_id', 'store_key'])

    #df['log_quantity'] = np.log1p(df['quantity_sold'])
    df['log_unit_retail'] = np.log(df['avg_unit_retail'])
    #df['total_revenue'] = df['quantity_sold'] * df['avg_unit_retail']
    #df['units_per_transaction'] = df['quantity_sold'] / np.maximum(df['total_transactions'], 1)
    df['competitor_price_ratio'] = df['avg_unit_retail'] / df['avg_competitor_price']
    df['price_difference'] = df['avg_unit_retail'] - df['avg_competitor_price']
    df['competitor_price_gap_pct'] = df['price_difference'] / df['avg_competitor_price']

    # No fillna(0) - a missing margin is unknown, not zero. LightGBM
    # handles real NaN natively; manufacturing 0 just teaches the model
    # a false "no margin" signal for rows where margin is simply unknown.
    df['current_profit_margin_pct'] = (df['avg_unit_retail'] - df['avg_unit_cost']) / df['avg_unit_retail']
    #df['current_profit_margin_pct'] = df['quantity_sold'] * (df['avg_unit_retail'] - df['avg_unit_cost'])
    

    # np.nan, not pd.NA - keeps this a clean float64 column instead of
    # collapsing to object dtype, which is what caused the crash.
    df['is_higher_than_competitor'] = np.where(
        df['avg_competitor_price'].isna(), np.nan,
        (df['avg_unit_retail'] > df['avg_competitor_price']).astype(float)
    )
    df['is_undercut'] = np.where(
        df['min_competitor_price'].isna(), np.nan,
        (df['avg_unit_retail'] > df['min_competitor_price']).astype(float)
    )

    # No fillna(0) on lags - a real gap in history, left as NaN, is what
    # LightGBM is designed to split around correctly.
    df['quantity_lag_1_week'] = grouped['quantity_sold'].shift(1)
    df['quantity_lag_4_weeks'] = grouped['quantity_sold'].shift(4)
    df['price_change_pct_weekly'] = grouped['avg_unit_retail'].pct_change(periods=1)

    df['quantity_rolling_avg_4_weeks'] = (
        df.groupby(['product_id', 'store_key'])['quantity_sold']
          .transform(lambda s: s.shift(1).rolling(window=4, min_periods=1).mean())
    )
    # No fillna(df['avg_unit_retail']) - that fallback was silently
    # setting "4-week rolling avg price" equal to "today's price" for new
    # product-stores, artificially zeroing out any price-change signal
    # exactly where the model most needs to see "no history yet" honestly.
    df['price_rolling_avg_4_weeks'] = (
        df.groupby(['product_id', 'store_key'])['avg_unit_retail']
          .transform(lambda s: s.shift(1).rolling(window=4, min_periods=1).mean())
    )

    df['month'] = df['sales_week'].dt.month
    df['quarter'] = df['sales_week'].dt.quarter
    df['week_of_year'] = df['sales_week'].dt.isocalendar().week.astype(int)

    return df

def prepare_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    float_cols = [
        'avg_unit_retail',
        'avg_unit_cost',
        'avg_competitor_price',
        'min_competitor_price',
        'max_competitor_price' ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    
    df['sales_week'] = pd.to_datetime(df['sales_week'])

    return df
def _prepare_after_feature_eng(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in categorical_cols:
        df[col] = df[col].astype('category')
    df['log_quantity'] = np.log1p(df['quantity_sold'])    
    df = df.dropna(subset=['quantity_lag_4_weeks']).reset_index(drop=True)
    return df

def _naive_elasticity_check(df: pd.DataFrame) -> float:
    """
    Fits a plain log-log OLS (quantity ~ price only) as an interpretable
    sanity-check baseline. If this coefficient and the ML model's implied
    elasticity (see evaluate_elasticity in price_optimizer.py) disagree by
    a wide margin, that's a signal to investigate before trusting the ML
    model's price recommendations - not something to silently ignore.
    """
    X = sm.add_constant(df["log_unit_retail"])
    y = df["log_quantity"]
    result = sm.OLS(y, X).fit()
    return float(result.params["log_unit_retail"])


def train(df: pd.DataFrame, test_days: int = 28) -> TrainResult:
    df = _prepare_after_feature_eng(df)
    df = df.sort_values("sales_week")

    cutoff = df["sales_week"].max() - pd.Timedelta(days=test_days)
    train_df = df[df["sales_week"] <= cutoff]
    test_df = df[df["sales_week"] > cutoff]
    x_train = train_df[features]
    y_train = train_df[target]
    x_test = test_df[features]
    y_test = test_df[target]

    if train_df.empty or test_df.empty:
        raise ValueError(
            f"Time-based split produced an empty set (test_days={test_days}). "
            "Check that pricing_features actually spans more than test_days "
            "of history before training.")

    monotonic = [-1 if col == "log_unit_retail" else 0 for col in features ]

    model = LGBMRegressor(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=6,
            monotone_constraints=monotonic,
            monotone_constraints_method="advanced",
            random_state=42)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit( x_train , y_train,categorical_feature=categorical_cols)

    train_preds = model.predict(x_train)
    test_preds = model.predict(x_test)
    
    train_mae = mean_absolute_error(y_train,train_preds)
    test_mae = mean_absolute_error(y_test, test_preds)
    #test_rmse = float(np.sqrt(mean_squared_error(y_test, test_preds)))
    #r2 = r2_score(y_test, preds)

    naive_elasticity = _naive_elasticity_check(train_df)

    category_dtypes = {col: train_df[col].dtype for col in categorical_cols}

    return TrainResult(
        model=model,
        train_mae=train_mae,
        test_mae=test_mae,
        #r2 = r2,
        naive_elasticity=naive_elasticity,
        category_dtypes=category_dtypes,
    )

def save(result: TrainResult, path: str = "demand_model.joblib") -> None:

    joblib.dump({"model": result.model, "category_dtypes": result.category_dtypes}, path)
    print(f" SUCCESS: Model trained and successfully saved to Volume!")


def load(path: str) -> tuple:
    """Returns (model, category_dtypes). Use this instead of joblib.load directly."""
    bundle = joblib.load(path)
    return bundle["model"], bundle["category_dtypes"]

