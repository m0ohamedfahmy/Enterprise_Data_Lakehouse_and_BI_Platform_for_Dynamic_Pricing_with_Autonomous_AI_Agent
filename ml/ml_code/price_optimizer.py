import pandas as pd
import numpy as np
import statsmodels.api as sm
from dataclasses import dataclass
from src.train_demand_model import features

@dataclass
class PriceRecommendation:
    target_sales_week: str
    product_id: int
    store_id: int
    current_price: float
    current_weekly_quantity: int
    current_weekly_margin: float
    recommended_price: float
    predicted_weekly_quantity: float
    predicted_weekly_margin: float
    candidates: pd.DataFrame
        

def fix_prediction_dtypes(rows: pd.DataFrame, category_dtypes: dict) -> pd.DataFrame:
    """
    base_row.to_frame().T turns a single pandas Series into a one-row
    DataFrame - and since a Series holds one dtype for its whole length,
    EVERY column comes out as generic `object`, not just the categoricals.
    LightGBM rejects object-dtyped numeric columns outright (as our own
    test caught). Two separate fixes needed:
    1. Categorical columns get the exact dtype (same categories, same
       order) captured at training time - re-inferring categories fresh
       from a single repeated row would only ever see one category level,
       which LightGBM either rejects or, worse, could silently map to the
       wrong code if it doesn't reject it.
    2. Every other feature column gets coerced back to numeric explicitly -
       object dtype survives even for what were originally float/int/bool
       values, since the Series-transpose step erases that distinction.
    """
    rows = rows.copy()
    for col, dtype in category_dtypes.items():
        rows[col] = rows[col].astype(dtype)

    numeric_columns = [c for c in features if c not in category_dtypes]
    for col in numeric_columns:
        rows[col] = pd.to_numeric(rows[col], errors="raise")

    return rows    

def evaluate_elasticity( model, base_row: pd.Series, price_range: tuple[float, float], category_dtypes: dict, n_points: int = 20 ) -> float:
    """
    Reads the model's own implied elasticity at a specific product-store's
    current context, by finite-differencing predicted demand across a
    small price range. Compare this against train_demand_model.py's naive
    OLS baseline before trusting a recommendation - large disagreement
    means the model is extrapolating unreliably for this product-store,
    not that it has found a genuinely different elasticity.
    """
    prices = np.linspace(price_range[0], price_range[1], n_points)
    rows = pd.concat([base_row.to_frame().T] * n_points, ignore_index=True)
    rows["log_unit_retail"] = np.log(prices)
    rows = fix_prediction_dtypes(rows, category_dtypes)
    preds = model.predict(rows[features])
    # elasticity = d(log quantity) / d(log price)
    slope = np.polyfit(np.log(prices), preds, 1)[0]
    return float(slope)


def recommend_price(model,base_row: pd.Series,category_dtypes: dict,min_margin_pct: float = 0.10,max_competitive_gap_pct: float = 0.05,n_candidates: int = 50) -> PriceRecommendation:
    """
    base_row: one row of engineered features for a specific
    (product_key, store_key), at its most recent known state - i.e. the
    latest row from pricing_features for that pair. All non-price features
    are held fixed at their current values; only price is varied across
    the candidate grid.

    Constraints, both enforced as hard filters (candidates violating either
    are excluded outright, not merely penalized):
    - min_margin_pct: price must clear unit_cost by at least this margin.
      This is a floor, not a target - it exists to prevent the optimizer
      from ever recommending a price that loses money or breaks a
      contractual minimum margin, regardless of what the demand curve says
      would maximize predicted quantity.
    - max_competitive_gap_pct: price must stay within this fraction of the
      last known competitor price. This is a business guardrail, not
      something derived from the demand model - it exists because pure
      margin-maximization without a competitive constraint can recommend
      prices far above market that the demand model has never actually
      observed and is extrapolating into blindly.
    """
    unit_cost = base_row["avg_unit_cost"]
    competitor_price = base_row["avg_competitor_price"]
    current_price = base_row["avg_unit_retail"]
    max_date = pd.to_datetime(base_row['sales_week'])
    next_week_date = max_date + pd.Timedelta(days=1)
    base_row['sales_week']=next_week_date

    price_floor = unit_cost * (1 + min_margin_pct)
    if pd.notna(competitor_price):
        competitive_low = competitor_price * (1 - max_competitive_gap_pct)
        competitive_high = competitor_price * (1 + max_competitive_gap_pct)
    else:
        # No competitor price known for this product - fall back to a
        # wider band around current price rather than silently ignoring
        # the constraint entirely. Flag this explicitly in the output
        # rather than pretending the constraint was meaningfully applied.
        competitive_low = current_price * 0.85
        competitive_high = current_price * 1.15

    grid_low = max(price_floor, competitive_low)
    grid_high = max(grid_low * 1.01, competitive_high)  # guard against inverted/degenerate range

    candidate_prices = np.linspace(grid_low, grid_high, n_candidates)

    rows = pd.concat([base_row.to_frame().T] * n_candidates, ignore_index=True)
    rows["log_unit_retail"] = np.log(candidate_prices)
    rows["avg_unit_retail"] = candidate_prices
    rows['competitor_price_ratio'] = candidate_prices / rows['avg_competitor_price']
    rows['price_difference'] = candidate_prices - rows['avg_competitor_price']
    rows['competitor_price_gap_pct'] = rows['price_difference'] / rows['avg_competitor_price']
    rows['current_profit_margin_pct'] = (candidate_prices - rows['avg_unit_cost']) / candidate_prices
    
   
    rows['is_higher_than_competitor'] = np.where(
        rows['avg_competitor_price'].isna(), np.nan,
        ( candidate_prices > rows['avg_competitor_price']).astype(float))
    rows['is_undercut'] = np.where(
        rows['min_competitor_price'].isna(), np.nan,
        (candidate_prices > rows['min_competitor_price']).astype(float))    
    
    rows = fix_prediction_dtypes(rows, category_dtypes)

    predicted_log_quantity = model.predict(rows[features])
    predicted_quantity = np.expm1(predicted_log_quantity).clip(min=0)
    predicted_margin = predicted_quantity * (candidate_prices - unit_cost)
    

    candidates_df = pd.DataFrame({
        "price": candidate_prices,
        "predicted_quantity": predicted_quantity,
        "predicted_margin": predicted_margin})

    best_idx = candidates_df["predicted_margin"].idxmax()
    best = candidates_df.loc[best_idx]
 
    return PriceRecommendation(
        product_id=base_row["product_id"],
        target_sales_week=base_row['sales_week'],
        store_id=base_row["store_id"],
        current_price=current_price,
        current_weekly_quantity=base_row["quantity_sold"],
        current_weekly_margin= base_row["current_profit_margin_pct"],
        recommended_price=float(best["price"]),
        predicted_weekly_quantity=float(best["predicted_quantity"]),
        predicted_weekly_margin=float(best["predicted_margin"]),
        candidates=candidates_df)
