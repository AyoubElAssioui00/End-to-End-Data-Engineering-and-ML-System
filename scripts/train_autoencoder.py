import os
import argparse
import numpy as np
import mlflow
import mlflow.keras

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

from config.settings import settings

# Simple Keras builder
def build_autoencoder(input_dim: int, code_size: int = 16, lr: float = 1e-3):
    try:
        import tensorflow as tf
        from tensorflow.keras.layers import Input, Dense
        from tensorflow.keras.models import Model
    except Exception as exc:
        raise RuntimeError("TensorFlow is required. Install tensorflow package.") from exc

    inp = Input(shape=(input_dim,), name="input")
    x = Dense(64, activation="relu", name="enc_1")(inp)
    code = Dense(code_size, activation="relu", name="code")(x)
    x = Dense(64, activation="relu", name="dec_1")(code)
    out = Dense(input_dim, activation="linear", name="output")(x)

    model = Model(inputs=inp, outputs=out, name="autoencoder")
    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr), loss="mse")
    return model


def main(
    train_csv: str = "data/train_batch.csv",
    epochs: int = 50,
    batch_size: int = 128,
    learning_rate: float = 1e-3,
    code_size: int = 16,
    run_name: str = "autoencoder_train"
):
    # MLflow config
    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
    mlflow.keras.autolog()  # enable keras autologging

    # Spark
    spark = SparkSession.builder.appName("IDS_AutoEncoder").getOrCreate()

    if not os.path.exists(train_csv):
        raise FileNotFoundError(f"Training CSV not found: {train_csv}")

    # Load CSV
    df = spark.read.option("header", True).csv(train_csv)

    # Ensure numeric and cast to double
    feature_cols = df.columns
    for c in feature_cols:
        df = df.withColumn(c, col(c).cast("double"))

    # Assemble features if needed (we'll convert full DF to pandas)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vec = assembler.transform(df).select("features")

    # Collect to driver as Pandas (assumes fits in memory)
    pandas_df = df.toPandas()
    X = pandas_df[feature_cols].to_numpy(dtype=np.float32)

    input_dim = X.shape[1]

    # Build and train model, track with MLflow
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_param("epochs", epochs)
        mlflow.log_param("batch_size", batch_size)
        mlflow.log_param("learning_rate", learning_rate)
        mlflow.log_param("code_size", code_size)
        mlflow.log_param("input_dim", input_dim)
        mlflow.log_param("train_csv", train_csv)

        model = build_autoencoder(input_dim=input_dim, code_size=code_size, lr=learning_rate)

        # Fit with validation split to capture val_loss
        history = model.fit(
            X, X,
            epochs=epochs,
            batch_size=batch_size,
            validation_split=0.2,
            shuffle=True,
            verbose=2
        )

        # Extract losses
        train_losses = history.history.get("loss", [])
        val_losses = history.history.get("val_loss", [])

        final_train_loss = float(train_losses[-1]) if train_losses else None
        final_val_loss = float(val_losses[-1]) if val_losses else None
        max_val_loss = float(max(val_losses)) if val_losses else None
        min_val_loss = float(min(val_losses)) if val_losses else None

        mlflow.log_metric("final_train_loss", final_train_loss)
        mlflow.log_metric("final_val_loss", final_val_loss)
        if max_val_loss is not None:
            mlflow.log_metric("val_loss_max", max_val_loss)
        if min_val_loss is not None:
            mlflow.log_metric("val_loss_min", min_val_loss)

        # Save and register model
        # Register under name "NetworkAnomalyDetector"
        try:
            mlflow.keras.log_model(
                keras_model=model,
                artifact_path="autoencoder_model",
                registered_model_name="NetworkAnomalyDetector"
            )
        except Exception:
            # fallback: simple log without registration
            mlflow.keras.log_model(keras_model=model, artifact_path="autoencoder_model")

        # Also save a local copy
        out_dir = "data/model_autoencoder"
        os.makedirs(out_dir, exist_ok=True)
        try:
            model.save(os.path.join(out_dir, "autoencoder.h5"))
        except Exception:
            pass

    spark.stop()
    print("Training complete. MLflow run id:", run.info.run_id)


if __name__ == "__main__":
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