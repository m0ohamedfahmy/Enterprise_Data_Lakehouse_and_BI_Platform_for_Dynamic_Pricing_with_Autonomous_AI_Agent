from fastapi import FastAPI, HTTPException
from utils import  score_all
from src.train_demand_model import read_data_api_predict,connection
import pandas as pd
import os
import uvicorn



## Load the Model
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/models/demand_model.joblib")
RECOMMENDATIONS_PARQUET_PATH = os.environ.get("RECOMMENDATIONS_PARQUET_PATH", "/app/output/price_recommendations.parquet")
app = FastAPI()


@app.post('/demand_prediction')
async def predict_demand(
    product_id: int, 
    min_margin_pct: float, 
    max_competitive_gap_pct: float, 
    n_candidates: int
):
    conn = connection()
    df = read_data_api_predict(conn= conn, product_id=product_id ) 
    
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found")

    res_df = score_all(
        model_path=MODEL_PATH,
        df=df,
        min_margin_pct=min_margin_pct,
        max_competitive_gap_pct=max_competitive_gap_pct,
        n_candidates=n_candidates
    )
    grouped_df = res_df .groupby('product_id', as_index=False).agg({
    'current_price': 'mean',
    'current_weekly_quantity': 'sum' ,
    'current_weekly_margin': 'mean' ,
    'recommended_price': 'mean',
    'predicted_weekly_quantity': 'sum',
    'predicted_weekly_margin': 'sum'
    })
    grouped_df['current_weekly_margin'] = (grouped_df['current_weekly_quantity'] * grouped_df['current_price'] * grouped_df['current_weekly_margin']).round(0).astype(int)



    return grouped_df.to_dict(orient="records")
    

@app.get("/recommendations")
def recommendations(store_key: int | None = None, category: str | None = None):
    """
    Serves the precomputed recommendation table - this is the endpoint
    Power BI's Web connector should point at for dashboard charts. See
    module docstring for why this is precomputed rather than live.
    """
    try:
        df = pd.read_parquet(RECOMMENDATIONS_PARQUET_PATH)
  
    except FileNotFoundError:
        raise HTTPException(
            503,
            "No recommendations available yet - the scoring pipeline "
            "hasn't produced output. Run run_pricing_pipeline.py score first.",
        )

    # NaN doesn't serialize to valid JSON (produces literal `NaN` tokens,
    # which most JSON parsers - including Power BI's - reject outright).
    # None serializes to `null`, which is the correct, portable representation.
    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")    

# if __name__ == "__main__":
#     uvicorn.run("main:app", host="0.0.0.0", port=8100, reload=False)