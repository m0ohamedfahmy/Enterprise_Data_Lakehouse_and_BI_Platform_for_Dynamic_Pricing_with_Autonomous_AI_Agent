import argparse
import os
import pandas as pd
from src.price_optimizer import recommend_price
from src.train_demand_model import load, save, train,connection,prepare_raw_data,Feature_engineering,read_data



def train_and_save(model_path: str) -> None:
    conn = connection()
    df = read_data(conn= conn) 
    df_pre = prepare_raw_data(df)
    df_features = Feature_engineering(df_pre)
    result = train(df_features)

    print(f"Train MAE (log-quantity space): {result.train_mae:.4f}")
    print(f"Test MAE (log-quantity space): {result.test_mae:.4f}")
    #print(f"R² Score: {result.r2 * 100:.2f}%")
    print(f"Naive log-log OLS elasticity (sanity check): {result.naive_elasticity:.4f}")
    
    save(result ,path=model_path)

def score_all(model_path: str, output_path: str, min_margin_pct: float, max_competitive_gap_pct: float,n_candidates: int) -> None:
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"No trained model found at {model_path}. Run train_and_save "
            "first - scoring should never silently fall back to training "
            "inline, since that would retrain on a schedule meant only for "
            "scoring, hiding when the model actually last changed."
        )
    model, category_dtypes = load(model_path)
    conn = connection()
    df = read_data(conn=conn)
    df_pre = prepare_raw_data(df)
    features_df = Feature_engineering(df_pre)
    

    # Score using each product-store's most recent row only - recommending
    # a price is inherently a "given today's context" decision, not
    # something to backfill across history.
    latest = (
        features_df.sort_values("sales_week")
        .groupby(["product_id", "store_id"], as_index=False)
        .tail(1)
    )

    results = []
    for _, row in latest.iterrows():
        rec = recommend_price(
            model, row, category_dtypes,
            min_margin_pct=min_margin_pct,
            max_competitive_gap_pct=max_competitive_gap_pct,
            n_candidates=n_candidates)
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
    grouped_df = out_df .groupby(['product_id', 'target_sales_week'], as_index=False).agg({
        'current_price': 'mean',
        'current_weekly_quantity': 'sum' ,
        'current_weekly_margin': 'mean' ,
        'recommended_price': 'mean',
        'predicted_weekly_quantity': 'sum',
        'predicted_weekly_margin': 'sum'
        })
    grouped_df['current_weekly_margin'] = (grouped_df['current_weekly_quantity'] * grouped_df['current_price'] * grouped_df['current_weekly_margin']).round(0).astype(int)
    
    grouped_df.to_parquet(output_path, index=False)
    print(f"Wrote {len(grouped_df)} price recommendations to {output_path}")    



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["train", "score"])
    parser.add_argument("--model-path", default="/home/ninja/lakehouse-docker/ml/ml_models/demand_model.joblib")
    parser.add_argument("--output-path", default="/home/ninja/lakehouse-docker/ml/tmp/price_recommendations.parquet")
    parser.add_argument("--min-margin-pct", type=float, default=0.10)
    parser.add_argument("--max-competitive-gap-pct", type=float, default=0.05)
    parser.add_argument("--n-candidates", type=int, default=5)
    args = parser.parse_args()
    if args.mode == "train":
        train_and_save(args.model_path)
    else:
        score_all(args.model_path, args.output_path, args.min_margin_pct, args.max_competitive_gap_pct, args.n_candidates)