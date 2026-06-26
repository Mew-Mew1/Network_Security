import sys
import os

import certifi
ca = certifi.where()

from dotenv import load_dotenv
load_dotenv()

import pymongo
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging
from network_security.pipeline.training_pipeline import TrainingPipeline

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import JSONResponse
from uvicorn import run as app_run
import pandas as pd

from network_security.utils.main_utils.utils import load_object
from network_security.utils.ml_utils.model.estimator import NetworkModel
from network_security.utils.main_utils.live_url_extractor import LiveURLExtractor

from network_security.constants.training_pipeline import DATA_INGESTION_COLLECTION_NAME
from network_security.constants.training_pipeline import DATA_INGESTION_DATABASE_NAME

_client = None


def get_mongo_collection():
    global _client
    mongo_db_url = os.getenv("MONGO_DB_URL")
    if not mongo_db_url:
        raise RuntimeError("MONGO_DB_URL environment variable is not set")
    if _client is None:
        _client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
    database = _client[DATA_INGESTION_DATABASE_NAME]
    return database[DATA_INGESTION_COLLECTION_NAME]

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi.templating import Jinja2Templates
templates = Jinja2Templates(directory="./templates")

@app.get("/", tags=["authentication"])
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html", {"request": request})

@app.get("/train")
async def train_route_get(request: Request):
    return templates.TemplateResponse(request, "index.html", {"request": request})

@app.post("/train")
async def train_route(request: Request):
    try:
        train_pipeline=TrainingPipeline()
        train_pipeline.run_pipeline()
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "train_message": "Training completed successfully.",
            },
        )
    except Exception as e:
        raise NetworkSecurityException(e,sys)
    
@app.post("/predict")
async def predict_route(request: Request, file: UploadFile = File(...)):
    try:
        preprocessor_path = "final_model/preprocessor.pkl"
        model_path = "final_model/model.pkl"
        if not os.path.exists(preprocessor_path) or not os.path.exists(model_path):
            return JSONResponse(
                status_code=400,
                content={
                    "detail": "Model artifacts are missing. Train the model locally first before calling /predict."
                },
            )

        df = pd.read_csv(file.file)
        preprocesor = load_object(preprocessor_path)
        final_model = load_object(model_path)
        network_model = NetworkModel(preprocessor=preprocesor, model=final_model)
        y_pred = network_model.predict(df)
        df['predicted_column'] = y_pred
        os.makedirs('prediction_output', exist_ok=True)
        df.to_csv('prediction_output/output.csv', index=False)
        prediction_counts = df['predicted_column'].value_counts().to_dict()
        table_html = df.to_html(classes='results-table', index=False)
        return templates.TemplateResponse(
            request,
            "table.html",
            {
                "request": request,
                "table": table_html,
                "row_count": len(df),
                "prediction_counts": prediction_counts,
            },
        )

    except Exception as e:
        raise NetworkSecurityException(e, sys)


@app.post("/predict_live_url")
async def predict_live_url(request: Request):
    try:
        form = await request.form()
        url = form.get("url")
        if not url:
            return JSONResponse(status_code=400, content={"detail": "Missing 'url' in form data"})

        preprocessor_path = "final_model/preprocessor.pkl"
        model_path = "final_model/model.pkl"
        if not os.path.exists(preprocessor_path) or not os.path.exists(model_path):
            return JSONResponse(
                status_code=400,
                content={"detail": "Model artifacts are missing. Train the model locally first before calling /predict_live_url."},
            )

        extractor = LiveURLExtractor(url)
        features_df = extractor.extract_features()

        preprocessor = load_object(preprocessor_path)
        final_model = load_object(model_path)

        print("/predict_live_url features:", features_df.to_dict(orient="records")[0])
        print("/predict_live_url extraction_log:", extractor.extraction_log)

        transformed = preprocessor.transform(features_df)
        print("/predict_live_url transformed:", transformed.tolist())

        ##pred = final_model.predict(transformed)
        # Check probability instead of raw class
        proba = final_model.predict_proba(transformed)
        # Example: Only flag as phishing if the model is > 80% sure
        pred = 1 if proba[0][1] > 0.8 else 0

        return JSONResponse(status_code=200, content={
            "url": url,
            "prediction_code": int(pred),
            "risk_status": "HIGH RISK PHISHING DETECTED" if pred == 1 else "SAFE DOMAIN",
            "confidence": round(extractor.confidence, 3),
            "missing_feature_count": extractor.missing_feature_count,
            "missing_features": extractor.missing_features,
            "features": features_df.to_dict(orient="records")[0],
            "extraction_log": extractor.extraction_log,
            "transformed": transformed.tolist()[0],
        })

    except Exception as e:
        raise NetworkSecurityException(e, sys)

    
if __name__=="__main__":
    app_run(app,host="0.0.0.0",port=8000)
