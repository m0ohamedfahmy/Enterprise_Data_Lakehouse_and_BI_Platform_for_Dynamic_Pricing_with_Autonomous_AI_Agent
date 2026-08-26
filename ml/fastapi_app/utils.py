import pandas as pd
import os
from src.train_demand_model import features,connection,read_data,Feature_engineering,prepare_raw_data,load
from src.price_optimizer import fix_prediction_dtypes,recommend_price



def score_all(model_path: str,df: pd.DataFrame , min_margin_pct: float, max_competitive_gap_pct: float, n_candidates: int) -> pd.DataFrame:
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"No trained model found at {model_path}. Run train_and_save "
            "first - scoring should never silently fall back to training "
            "inline, since that would retrain on a schedule meant only for "
            "scoring, hiding when the model actually last changed."
        )
    model, category_dtypes = load(model_path)
 
    df_pre = prepare_raw_data(df)
    features_df = Feature_engineering(df_pre)
    

    results = []
    for _, row in features_df.iterrows():
        rec = recommend_price(
            model, row, category_dtypes,
            min_margin_pct=min_margin_pct,
            max_competitive_gap_pct=max_competitive_gap_pct, n_candidates=n_candidates)
        results.append({
            "product_id": rec.product_id,
            "target_sales_week": rec.target_sales_week,
            "store_id": rec.store_id,
            "current_price": rec.current_price,
            "current_weekly_quantity": rec.current_weekly_quantity,
            "current_weekly_margin": rec.current_weekly_margin,
            "recommended_price": round(rec.recommended_price),
            "predicted_weekly_quantity": round(rec.predicted_weekly_quantity),
            "predicted_weekly_margin": round(rec.predicted_weekly_margin) })

    out_df = pd.DataFrame(results)
    return out_df 