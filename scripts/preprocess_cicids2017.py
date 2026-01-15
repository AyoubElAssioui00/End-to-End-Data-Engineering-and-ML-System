import os
import glob
import shutil
import pickle
import time
import json
from pathlib import Path
import re

import structlog
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.linalg import VectorUDT
import numpy as np
import pandas as pd

logger = structlog.get_logger(__name__)


def write_single_csv(spark_df, out_path):
    tmp_dir = out_path + ".tmp"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    spark_df.coalesce(1).write.option("header", True).mode("overwrite").csv(tmp_dir)
    # find part file
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No part file found in {tmp_dir}")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    shutil.move(part_files[0], out_path)
    shutil.rmtree(tmp_dir)
    logger.info("output.saved", path=out_path)


def map_label_simple(lbl: str) -> str:
    if lbl is None:
        return "Other"
    l = lbl.strip().lower()
    if l == "benign":
        return "BENIGN"
    # DDoS / DoS
    if any(token in l for token in ["ddos", "distributed", "dos", "hulk", "goldeneye", "slowloris", "slowhttptest"]):
        # differentiate: labels that explicitly contain 'ddos'
        if "ddos" in l:
            return "DDoS"
        return "DoS"
    # PortScan
    if "portscan" in l or "port scan" in l:
        return "PortScan"
    # Bot
    if "bot" in l:
        return "Bot"
    # Brute Force / Patator
    if any(token in l for token in ["patator", "brute", "bruteforce", "brute-force"]):
        return "BruteForce"
    # Web attacks (xss / sql injection / web attack)
    if any(token in l for token in ["web attack", "xss", "sql", "injection"]):
        return "WebAttack"
    # fallback: keep original (capitalized)
    return lbl


def sanitize_column_name(name: str) -> str:
    # strip, replace problematic chars (dots, spaces, slashes, parentheses, etc.) with underscore
    n = name.strip()
    # replace any sequence of non-alnum/_ with underscore
    n = re.sub(r"[^0-9A-Za-z_]+", "_", n)
    # collapse multiple underscores
    n = re.sub(r"_+", "_", n)
    # remove leading/trailing underscores
    n = n.strip("_")
    # if empty or starts with digit, prefix with col_
    if not n or re.match(r"^\d", n):
        n = f"col_{n}"
    return n


def safe_count(df, label="rows"):
    try:
        return df.count()
    except Exception as exc:
        logger.warning("count.failed", error=str(exc), label=label)
        return None


def collect_label_counts(df, label_col="Label"):
    try:
        rows = df.groupBy(label_col).count().collect()
        return {r[label_col]: r["count"] for r in rows}
    except Exception as exc:
        logger.warning("label_counts_failed", error=str(exc))
        return {}


def main():
    start = time.time()
    spark = SparkSession.builder.appName("CICIDS2017-Preprocess").getOrCreate()

    raw_dir = Path("data/raw_view")
    csv_files = sorted([str(p) for p in raw_dir.glob("*.csv")])
    logger.info("loading.files", folder=str(raw_dir), files_count=len(csv_files), files=csv_files[:50])

    if not csv_files:
        logger.error("no_csv_found", folder=str(raw_dir))
        raise RuntimeError(f"No CSV files found in {raw_dir}")

    # 1) Loading: read and concatenate
    t0 = time.time()
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_files)
    df = df.cache()
    total_rows = safe_count(df, "initial_rows")
    logger.info("data.loaded", files=len(csv_files), rows=total_rows, elapsed_s=time.time() - t0, columns=df.columns)

    # SANITIZE column names immediately to avoid dots and chars that Spark treats as nested field access
    t0 = time.time()
    new_names = {}
    used = set()
    for c in df.columns:
        sanitized = sanitize_column_name(c)
        # ensure uniqueness
        base = sanitized
        i = 1
        while sanitized in used:
            sanitized = f"{base}_{i}"
            i += 1
        used.add(sanitized)
        if sanitized != c:
            new_names[c] = sanitized

    if new_names:
        logger.info("columns.sanitizing", mappings=new_names)
    for old, new in new_names.items():
        df = df.withColumnRenamed(old, new)
    logger.info("columns.sanitized", elapsed_s=time.time() - t0, new_columns=df.columns)

    # 2) Cleaning
    # Strip whitespace from any remaining column names (should be already handled)
    for c in df.columns:
        stripped = c.strip()
        if stripped != c:
            df = df.withColumnRenamed(c, stripped)

    before = safe_count(df, "before_drop_duplicates")
    df = df.dropDuplicates()
    after = safe_count(df, "after_drop_duplicates")
    logger.info("duplicates.removed", before=before, after=after, removed=(before - after) if (before is not None and after is not None) else None)

    # Replace 'Infinity' and '-Infinity' with null then drop rows with nulls
    replace_vals = ["Infinity", "-Infinity"]
    inf_replacements = {}
    for c in df.columns:
        try:
            cnt_inf = df.filter(col(c).isin(replace_vals)).count()
            if cnt_inf:
                inf_replacements[c] = int(cnt_inf)
            df = df.withColumn(c, when(col(c).isin(replace_vals), None).otherwise(col(c)))
        except Exception as exc:
            logger.warning("replace_infinity.failed", column=c, error=str(exc))
    logger.info("infinity.replacements", replacements=inf_replacements)

    before_na = safe_count(df, "before_dropna")
    df = df.na.drop()
    after_na = safe_count(df, "after_dropna")
    logger.info("na.drop", before=before_na, after=after_na, dropped=(before_na - after_na) if (before_na is not None and after_na is not None) else None)

    # Standardize Label mapping
    map_udf = spark.udf.register("map_label_udf", map_label_simple, StringType())
    # try to find Label column (after sanitization common variants)
    label_candidates = [c for c in df.columns if c.lower() == "label" or c.lower().endswith("_label")]
    if "Label" not in df.columns:
        if label_candidates:
            df = df.withColumnRenamed(label_candidates[0], "Label")
            logger.info("label.column.renamed", from_column=label_candidates[0])
        else:
            logger.error("label.column.not.found", columns=df.columns)
            raise RuntimeError("No 'Label' column found in dataset")

    # Log label distribution before mapping
    try:
        raw_label_counts = collect_label_counts(df, label_col=label_candidates[0] if label_candidates else "Label")
        logger.info("label.dist.before_mapping", counts=raw_label_counts)
    except Exception:
        pass

    df = df.withColumn("Label", map_udf(col("Label")))

    # Log label distribution after mapping
    label_counts_after = collect_label_counts(df, label_col="Label")
    logger.info("label.dist.after_mapping", counts=label_counts_after)

    # 3) Feature Selection (Drop Columns)
    drop_cols_raw = [
        "Destination Port",
        "Total Backward Packets",
        "Total Length of Bwd Packets",
        "Subflow Bwd Bytes",
        "Avg Fwd Segment Size",
        "Avg Bwd Segment Size",
        "ECE Flag Count",
        "RST Flag Count",
        "Fwd URG Flags",
        "Idle Std",
        "Fwd PSH Flags",
        "Active Std",
        "Down/Up Ratio",
        "URG Flag Count"
    ]

    def san_or_none(name):
        s = sanitize_column_name(name)
        if s in df.columns:
            return s
        for c in df.columns:
            if c.lower().replace("_", "") == name.lower().replace(" ", "").replace("_", ""):
                return c
        return None

    existing_drops = []
    for rc in drop_cols_raw:
        sc = san_or_none(rc)
        if sc and sc in df.columns:
            existing_drops.append(sc)

    logger.info("columns.to.drop", requested=drop_cols_raw, found=existing_drops)
    if existing_drops:
        df = df.drop(*existing_drops)

    # 4) Semi-Supervised Split
    total_after_clean = safe_count(df, "after_clean_total")
    benign_df = df.filter(col("Label") == "BENIGN")
    attacks_df = df.filter(col("Label") != "BENIGN")

    benign_count = safe_count(benign_df, "benign_total")
    attacks_count = safe_count(attacks_df, "attacks_total")
    logger.info("split.benign_attacks_counts", benign=benign_count, attacks=attacks_count, total=total_after_clean)

    benign_train, benign_test = benign_df.randomSplit([0.8, 0.2], seed=42)
    bt_train_count = safe_count(benign_train, "benign_train")
    bt_test_count = safe_count(benign_test, "benign_test")
    logger.info("benign.split", train=bt_train_count, test=bt_test_count)

    train_df = benign_train.drop("Label")
    streaming_df = benign_test.unionByName(attacks_df)

    logger.info("datasets.prepared", train_rows=safe_count(train_df, "train_rows"), streaming_rows=safe_count(streaming_df, "stream_rows"))

    # 5) Scaling using pandas/numpy (no Spark StandardScaler / no pipeline)
    # Determine numeric feature columns (exclude Label and any non-numeric)
    feature_cols = [f.name for f in train_df.schema.fields if f.name != "Label" and f.dataType.typeName() in ("integer", "long", "double", "float", "short")]
    if not feature_cols:
        feature_cols = [c for c in train_df.columns if c != "Label"]
    logger.info("feature.columns.selected", count=len(feature_cols), columns=feature_cols[:200])

    # Cast feature columns to double for consistent processing
    for fc in feature_cols:
        train_df = train_df.withColumn(fc, col(fc).cast("double"))
        streaming_df = streaming_df.withColumn(fc, col(fc).cast("double"))

    # Collect training features to driver (pandas) to compute mean/std and scale.
    # If dataset is large, sample to avoid OOM.
    train_count = safe_count(train_df, "train_count_for_scaling")
    if train_count is None:
        train_count = 0

    SAMPLE_ROW_LIMIT = 500_000  # adjust if needed
    if train_count > SAMPLE_ROW_LIMIT:
        logger.warning("train.too_large_for_memory", rows=train_count, sampling=SAMPLE_ROW_LIMIT)
        train_pd = train_df.sample(withReplacement=False, fraction=float(SAMPLE_ROW_LIMIT) / float(train_count), seed=42).toPandas()[feature_cols]
    else:
        train_pd = train_df.select(*feature_cols).toPandas()

    if train_pd.empty:
        raise RuntimeError("No training rows available to fit scaler")

    # Compute mean/std with numpy (ddof=0 for population std, avoid zero-std)
    means = train_pd.mean(axis=0)
    stds = train_pd.std(axis=0, ddof=0).replace(0, 1.0)  # avoid division by zero

    logger.info("scaler.fitted_numpy", rows=len(train_pd), features=len(feature_cols))

    # Scale training set
    train_scaled_np = (train_pd.values - means.values.reshape(1, -1)) / stds.values.reshape(1, -1)
    train_scaled_pd = pd.DataFrame(train_scaled_np, columns=feature_cols)

    # Prepare streaming set (collect to pandas)
    # keep Label for streaming set
    stream_cols = feature_cols + (["Label"] if "Label" in streaming_df.columns else [])
    stream_pd = streaming_df.select(*stream_cols).toPandas()
    if stream_pd.empty:
        logger.warning("streaming.empty", rows=0)
        stream_scaled_pd = pd.DataFrame(columns=feature_cols + ["Label"])
    else:
        stream_features = stream_pd[feature_cols].astype(float)
        stream_scaled_np = (stream_features.values - means.values.reshape(1, -1)) / stds.values.reshape(1, -1)
        stream_scaled_pd = pd.DataFrame(stream_scaled_np, columns=feature_cols)
        if "Label" in stream_pd.columns:
            stream_scaled_pd["Label"] = stream_pd["Label"].values

    # Convert back to Spark DataFrames so downstream code (and write_single_csv) works unchanged
    final_train = spark.createDataFrame(train_scaled_pd)
    final_stream = spark.createDataFrame(stream_scaled_pd)

    # persist/save the scaler parameters (means/stds) into metadata for later use
    scaler_params = {
        "feature_columns": feature_cols,
        "means": means.to_dict(),
        "stds": stds.to_dict()
    }
    # merge into metadata later (metadata is written below)

    # 6) Output: write single CSV files and save preprocessing metadata
    os.makedirs("data", exist_ok=True)
    write_single_csv(final_train, "data/train_batch.csv")
    write_single_csv(final_stream, "data/network_traffic.csv")

    # scaler_dir = "data/scaler_spark_pipeline"
    # if os.path.exists(scaler_dir):
    #     shutil.rmtree(scaler_dir)
    # scaler_pipeline_model.write().overwrite().save(scaler_dir)
    # logger.info("scaler.saved", path=scaler_dir)

    metadata = {
        "feature_columns": feature_cols,
        "dropped_columns": existing_drops,
        "label_mapping": "custom map in script (map_label_simple)",
        # "scaler_spark_model_path": scaler_dir,
        "rows": {
            "initial": total_rows,
            "after_clean": total_after_clean,
            "train": safe_count(final_train, "final_train"),
            "streaming": safe_count(final_stream, "final_stream")
        }
    }
    with open("data/preprocessing.pkl", "wb") as f:
        pickle.dump(metadata, f)
    logger.info("metadata.saved", path="data/preprocessing.pkl", metadata_keys=list(metadata.keys()))

    elapsed = time.time() - start
    logger.info("preprocessing.completed", elapsed_s=elapsed)


if __name__ == "__main__":
    main()