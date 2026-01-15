import sys
import os
import time
import json
from pathlib import Path
from typing import Optional

import structlog
import mlflow
import mlflow.keras
from mlflow.tracking import MlflowClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import numpy as np
import pandas as pd

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import settings

# configure structlog simple console output
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger(__name__)


def build_autoencoder(input_dim: int, code_size: int = 16, lr: float = 1e-3):
    try:
        import tensorflow as tf
        from tensorflow.keras.layers import Input, Dense
        from tensorflow.keras.models import Model
    except Exception as exc:
        logger.error("tensorflow.import_failed", error=str(exc))
        raise

    inp = Input(shape=(input_dim,), name="input")
    x = Dense(64, activation="relu", name="enc_1")(inp)
    code = Dense(code_size, activation="relu", name="code")(x)
    x = Dense(64, activation="relu", name="dec_1")(code)
    out = Dense(input_dim, activation="linear", name="output")(x)

    model = Model(inputs=inp, outputs=out, name="autoencoder")
    model.compile(optimizer="adam", loss="mse")
    return model


def wait_for_mlflow(uri: str, timeout_s: int = 30, interval: float = 1.0) -> Optional[str]:
    client = None
    deadline = time.time() + timeout_s
    last_exc = None
    while time.time() < deadline:
        try:
            mlflow.set_tracking_uri(uri)
            client = MlflowClient(tracking_uri=uri)
            # simple call
            client.list_experiments()  # may raise if unreachable
            logger.info("mlflow.reachable", uri=uri)
            return uri
        except Exception as exc:
            last_exc = exc
            logger.warning("mlflow.unreachable_retry", uri=uri, error=str(exc), sleep_s=interval)
            time.sleep(interval)
    logger.error("mlflow.unreachable", uri=uri, error=str(last_exc))
    return None


def ensure_experiment(name: str):
    try:
        mlflow.set_experiment(name)
        logger.info("mlflow.experiment_set", experiment_name=name)
    except Exception as exc:
        logger.warning("mlflow.set_experiment_failed", name=name, error=str(exc))
        # fallback: attempt to create via client
        try:
            client = MlflowClient()
            exp = client.get_experiment_by_name(name)
            if exp is None:
                client.create_experiment(name)
                logger.info("mlflow.experiment_created", experiment_name=name)
            else:
                logger.info("mlflow.experiment_exists", experiment_id=exp.experiment_id, name=name)
            mlflow.set_experiment(name)
        except Exception as exc2:
            logger.error("mlflow.experiment_fallback_failed", error=str(exc2))
            raise


def main(
    train_csv: str = "data/train_batch.csv",
    epochs: int = 50,
    batch_size: int = 128,
    learning_rate: float = 1e-3,
    code_size: int = 16,
    run_name: str = "autoencoder_train",
    mlflow_uri: Optional[str] = None
):
    logger.info("train.started", train_csv=train_csv, epochs=epochs, batch_size=batch_size)

    # MLflow tracking URI from settings or override
    mlflow_uri = mlflow_uri or getattr(settings, "MLFLOW_TRACKING_URI", None) or "http://localhost:5000"
    reachable = wait_for_mlflow(mlflow_uri, timeout_s=15, interval=1.0)
    if reachable is None:
        # fallback to local file store
        local_store = f"file://{os.path.abspath('mlruns')}"
        logger.warning("mlflow.fallback_local", local_store=local_store)
        mlflow.set_tracking_uri(local_store)
    else:
        mlflow.set_tracking_uri(mlflow_uri)

    # enable keras autologging (safe)
    try:
        mlflow.keras.autolog()
    except Exception as exc:
        logger.warning("mlflow.autolog_failed", error=str(exc))

    # ensure experiment exists (use a clear experiment name)
    experiment_name = "NetworkAnomalyDetector_experiment"
    try:
        ensure_experiment(experiment_name)
    except Exception:
        logger.warning("mlflow.ensure_experiment_failed_using_default")
        # proceed anyway

    # Spark session
    spark = SparkSession.builder.appName("IDS_AutoEncoder").getOrCreate()
    logger.info("spark.started")

    if not os.path.exists(train_csv):
        logger.error("train_csv_not_found", path=train_csv)
        raise FileNotFoundError(train_csv)

    # Read CSV into Spark DataFrame; cast numeric columns
    logger.info("loading.csv", path=train_csv)
    df = spark.read.option("header", True).csv(train_csv)
    df = df.cache()
    rows = None
    try:
        rows = df.count()
    except Exception as exc:
        logger.warning("spark.count_failed", error=str(exc))
    logger.info("csv.loaded", path=train_csv, rows=rows, columns=df.columns[:200])

    # Cast columns to float and assemble features via pandas for Keras
    # If dataframe has one column 'scaled' or 'features' handle generically
    pandas_df = df.toPandas()
    if pandas_df.empty:
        logger.error("no_rows_after_load", path=train_csv)
        raise RuntimeError("No rows to train on")

    # select numeric columns only
    feature_cols = pandas_df.select_dtypes(include=[np.number]).columns.tolist()
    if not feature_cols:
        # attempt to cast all to float
        for c in pandas_df.columns:
            pandas_df[c] = pd.to_numeric(pandas_df[c], errors="coerce")
        feature_cols = pandas_df.select_dtypes(include=[np.number]).columns.tolist()
    logger.info("features.detected", count=len(feature_cols), features=feature_cols[:200])

    X = pandas_df[feature_cols].astype(np.float32).to_numpy()

    input_dim = X.shape[1]
    logger.info("model.build", input_dim=input_dim, code_size=code_size)

    model = build_autoencoder(input_dim=input_dim, code_size=code_size, lr=learning_rate)

    # start mlflow run
    try:
        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
            logger.info("mlflow.run_started", run_id=run_id)

            # log params explicitly
            mlflow.log_param("epochs", epochs)
            mlflow.log_param("batch_size", batch_size)
            mlflow.log_param("learning_rate", learning_rate)
            mlflow.log_param("code_size", code_size)
            mlflow.log_param("input_dim", input_dim)
            mlflow.log_param("train_csv", train_csv)

            # train
            logger.info("training.start")
            history = model.fit(
                X, X,
                epochs=epochs,
                batch_size=batch_size,
                validation_split=0.2,
                shuffle=True,
                verbose=2
            )
            logger.info("training.completed", epochs=epochs)

            # extract metrics
            train_losses = history.history.get("loss", [])
            val_losses = history.history.get("val_loss", [])

            final_train_loss = float(train_losses[-1]) if train_losses else None
            final_val_loss = float(val_losses[-1]) if val_losses else None
            mlflow.log_metric("final_train_loss", final_train_loss)
            mlflow.log_metric("final_val_loss", final_val_loss)

            # compute reconstruction errors on full training set to produce threshold
            recon = model.predict(X, batch_size=batch_size)
            mse_per_sample = np.mean(np.square(recon - X), axis=1)
            max_mse = float(np.max(mse_per_sample))
            min_mse = float(np.min(mse_per_sample))
            mean_mse = float(np.mean(mse_per_sample))
            mlflow.log_metric("mse_max", max_mse)
            mlflow.log_metric("mse_min", min_mse)
            mlflow.log_metric("mse_mean", mean_mse)

            logger.info("metrics.logged", final_train_loss=final_train_loss, final_val_loss=final_val_loss,
                        mse_max=max_mse, mse_min=min_mse)

            # ensure local model dir exists and save local copy (preferred native .keras)
            out_dir = Path("data/model_autoencoder")
            out_dir.mkdir(parents=True, exist_ok=True)
            local_model_keras = out_dir / "autoencoder.keras"
            try:
                model.save(str(local_model_keras))
                logger.info("model.saved_local", path=str(local_model_keras))
            except Exception as exc:
                logger.warning("model.save_local_failed", error=str(exc))
                # fallback to h5
                try:
                    local_model_h5 = out_dir / "autoencoder.h5"
                    model.save(str(local_model_h5))
                    logger.info("model.saved_local_h5", path=str(local_model_h5))
                    local_model_keras = local_model_h5
                except Exception as exc2:
                    logger.error("model.save_all_failed", error=str(exc2))

            # Try to log model to MLflow. handle different mlflow client signatures and fallbacks.
            try:
                # preferred signature (older/newer mlflow may differ)
                mlflow.keras.log_model(model, artifact_path="autoencoder", registered_model_name="NetworkAnomalyDetector")
                logger.info("mlflow.model_logged_and_registered", registered_model="NetworkAnomalyDetector", artifact_path="autoencoder")
            except TypeError as exc_type:
                logger.warning("mlflow.log_model_typeerror", error=str(exc_type))
                try:
                    # try without registering (some mlflow versions expect positional 'model' param)
                    mlflow.keras.log_model(model, artifact_path="autoencoder")
                    logger.info("mlflow.model_logged", artifact_path="autoencoder")
                except Exception as exc2:
                    logger.warning("mlflow.log_model_failed", error=str(exc2))
                    # fallback: upload the local saved model file as an artifact
                    try:
                        if local_model_keras.exists():
                            mlflow.log_artifact(str(local_model_keras), artifact_path="autoencoder_files")
                            logger.info("mlflow.artifact_logged", path=str(local_model_keras), artifact_path="autoencoder_files")
                        else:
                            logger.error("no_local_model_to_log", path=str(local_model_keras))
                    except Exception as exc3:
                        logger.error("mlflow.log_artifact_failed", error=str(exc3))
            except Exception as exc:
                logger.warning("mlflow.log_model_unexpected_error", error=str(exc))
                # fallback to artifact upload
                try:
                    if local_model_keras.exists():
                        mlflow.log_artifact(str(local_model_keras), artifact_path="autoencoder_files")
                        logger.info("mlflow.artifact_logged_after_error", path=str(local_model_keras))
                except Exception as exc_art:
                    logger.error("mlflow.log_artifact_failed_final", error=str(exc_art))
    except Exception as exc_outer:
        logger.error("training.run_failed", error=str(exc_outer))
        raise
    finally:
        spark.stop()
        logger.info("spark.stopped")

    logger.info("train.finished")


if __name__ == "__main__":
    # minimal CLI
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-csv", default="data/train_batch.csv")
    parser.add_argument("--epochs", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--code-size", type=int, default=16)
    args = parser.parse_args()

    main(
        train_csv=args.train_csv,
        epochs=args.epochs,
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        code_size=args.code_size
    )