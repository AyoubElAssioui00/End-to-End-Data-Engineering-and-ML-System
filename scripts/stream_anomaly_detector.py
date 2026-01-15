"""
Stream Anomaly Detector using PySpark Streaming and MLFlow
Reads from network_flows topic, processes in mini-batches, classifies, and produces to normal/anomaly topics
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, TimestampType, DoubleType
)
import mlflow
import mlflow.pyfunc
from common_kafka.topics import KafkaTopics
from config.settings import settings
import structlog
import numpy as np
import json
from kafka.admin import KafkaAdminClient, NewTopic

logger = structlog.get_logger(__name__)


class StreamAnomalyDetector:
    """PySpark Streaming processor for network anomaly detection"""
    
    def __init__(self):
        self.spark = None
        self.model = None
        self.model_version = None
    
    def initialize_spark(self):
        """Initialize Spark session"""
        self.spark = SparkSession.builder \
            .appName(settings.SPARK_APP_NAME) \
            .master(settings.SPARK_MASTER) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("spark_session_initialized")
    
    def load_mlflow_model(self):
        """Load model from MLFlow registry"""
        try:
            mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
            
            # Load production model
            model_uri = f"models:/{settings.MLFLOW_MODEL_NAME}/{settings.MLFLOW_MODEL_STAGE}"
            self.model = mlflow.pyfunc.load_model(model_uri)
            
            # Get model version
            client = mlflow.MlflowClient()
            model_version_info = client.get_latest_versions(
                settings.MLFLOW_MODEL_NAME, 
                stages=[settings.MLFLOW_MODEL_STAGE]
            )[0]
            self.model_version = model_version_info.version
            
            logger.info(
                "mlflow_model_loaded",
                model_name=settings.MLFLOW_MODEL_NAME,
                version=self.model_version,
                stage=settings.MLFLOW_MODEL_STAGE
            )
        except Exception as e:
            logger.error("failed_to_load_model", error=str(e))
            # raise
    
    def get_schema(self):
        """Define schema for network flow events (aligned with NetworkFlowEvent)"""
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),

            # network identifiers
            # StructField("src_ip", StringType(), True),
            # StructField("dst_ip", StringType(), True),
            # StructField("src_port", IntegerType(), True),
            # StructField("dst_port", IntegerType(), True),
            # StructField("protocol", StringType(), True),
            StructField("flow_id", StringType(), True),

            # numeric features (use DoubleType for safety)
            StructField("flow_duration", DoubleType(), True),
            StructField("total_fwd_packets", DoubleType(), True),
            StructField("total_length_of_fwd_packets", DoubleType(), True),
            StructField("fwd_packet_length_max", DoubleType(), True),
            StructField("fwd_packet_length_min", DoubleType(), True),
            StructField("fwd_packet_length_mean", DoubleType(), True),
            StructField("fwd_packet_length_std", DoubleType(), True),
            StructField("bwd_packet_length_max", DoubleType(), True),
            StructField("bwd_packet_length_min", DoubleType(), True),
            StructField("bwd_packet_length_mean", DoubleType(), True),
            StructField("bwd_packet_length_std", DoubleType(), True),

            StructField("flow_bytes_s", DoubleType(), True),
            StructField("flow_packets_s", DoubleType(), True),
            StructField("flow_iat_mean", DoubleType(), True),
            StructField("flow_iat_std", DoubleType(), True),
            StructField("flow_iat_max", DoubleType(), True),
            StructField("flow_iat_min", DoubleType(), True),

            StructField("fwd_iat_total", DoubleType(), True),
            StructField("fwd_iat_mean", DoubleType(), True),
            StructField("fwd_iat_std", DoubleType(), True),
            StructField("fwd_iat_max", DoubleType(), True),
            StructField("fwd_iat_min", DoubleType(), True),

            StructField("bwd_iat_total", DoubleType(), True),
            StructField("bwd_iat_mean", DoubleType(), True),
            StructField("bwd_iat_std", DoubleType(), True),
            StructField("bwd_iat_max", DoubleType(), True),
            StructField("bwd_iat_min", DoubleType(), True),

            StructField("bwd_psh_flags", DoubleType(), True),
            StructField("bwd_urg_flags", DoubleType(), True),
            StructField("fwd_header_length", DoubleType(), True),
            StructField("bwd_header_length", DoubleType(), True),

            StructField("fwd_packets_s", DoubleType(), True),
            StructField("bwd_packets_s", DoubleType(), True),
            StructField("min_packet_length", DoubleType(), True),
            StructField("max_packet_length", DoubleType(), True),
            StructField("packet_length_mean", DoubleType(), True),
            StructField("packet_length_std", DoubleType(), True),
            StructField("packet_length_variance", DoubleType(), True),

            StructField("fin_flag_count", DoubleType(), True),
            StructField("syn_flag_count", DoubleType(), True),
            StructField("psh_flag_count", DoubleType(), True),
            StructField("ack_flag_count", DoubleType(), True),
            StructField("cwe_flag_count", DoubleType(), True),

            StructField("average_packet_size", DoubleType(), True),
            StructField("fwd_header_length_1", DoubleType(), True),
            StructField("fwd_avg_bytes_bulk", DoubleType(), True),
            StructField("fwd_avg_packets_bulk", DoubleType(), True),
            StructField("fwd_avg_bulk_rate", DoubleType(), True),

            StructField("bwd_avg_bytes_bulk", DoubleType(), True),
            StructField("bwd_avg_packets_bulk", DoubleType(), True),
            StructField("bwd_avg_bulk_rate", DoubleType(), True),

            StructField("subflow_fwd_packets", DoubleType(), True),
            StructField("subflow_fwd_bytes", DoubleType(), True),
            StructField("subflow_bwd_packets", DoubleType(), True),

            StructField("init_win_bytes_forward", DoubleType(), True),
            StructField("init_win_bytes_backward", DoubleType(), True),
            StructField("act_data_pkt_fwd", DoubleType(), True),
            StructField("min_seg_size_forward", DoubleType(), True),

            StructField("active_mean", DoubleType(), True),
            StructField("active_std", DoubleType(), True),
            StructField("active_max", DoubleType(), True),
            StructField("active_min", DoubleType(), True),
            StructField("idle_mean", DoubleType(), True),
            StructField("idle_std", DoubleType(), True),
            StructField("idle_max", DoubleType(), True),
            StructField("idle_min", DoubleType(), True),

            # label if present
            StructField("label", StringType(), True),
        ])
    
    def prepare_features(self, df):
        """
        Prepare features for model prediction.

        - Cast numeric columns to double and fill NA
        - Return DataFrame containing original columns (no duplicate numeric columns appended)
        """
        # determine numeric columns by schema / actual types
        numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (DoubleType, IntegerType, FloatType))]
        if not numeric_cols:
            # fallback: choose a minimal set
            numeric_cols = ["flow_duration", "flow_bytes_s", "flow_packets_s"]

        # Cast numeric cols to double and fill NA
        out = df
        for c in numeric_cols:
            out = out.withColumn(c, col(c).cast(DoubleType()))
        # fill nulls in numeric cols with 0.0
        out = out.fillna({c: 0.0 for c in numeric_cols})

        # avoid duplicating columns: keep original column order and unique columns only
        seen = set()
        unique_cols = []
        for c in out.columns:
            if c not in seen:
                seen.add(c)
                unique_cols.append(c)

        return out.select(*unique_cols)

    def _sanitize_pandas_df(self, pdf):
        """Make pandas df safe for spark.createDataFrame: fill object cols and numeric nulls"""
        for c, dtype in pdf.dtypes.items():
            if dtype == object:
                pdf[c] = pdf[c].fillna("")
            else:
                pdf[c] = pdf[c].fillna(0)
        return pdf

    def predict_anomalies(self, df):
        """
        Use loaded MLflow model to predict anomalies.
        """
        if df.rdd.isEmpty():
            return df

        pandas_df = df.toPandas()

        # sanitize to avoid NullType inference errors when converting back to Spark
        pandas_df = self._sanitize_pandas_df(pandas_df)

        # select numeric feature columns
        feature_columns = pandas_df.select_dtypes(include=[np.number]).columns.tolist()
        if not feature_columns:
            logger.error("predict_no_numeric_features")
            return self.spark.createDataFrame(pandas_df)
        
        # delete flow_id and label from feature_columns
        feature_columns = [c for c in feature_columns if c.lower() not in ("flow_id", "label", "event_id", "active_std", "idle_std", "metadata", "timestamp", "event_type")]

        X = pandas_df[feature_columns].astype(float).fillna(0.0)

        print(X)

        # try to detect expected input dim from loaded keras model (if available)
        expected_dim = None
        try:
            # mlflow.tensorflow models sometimes expose underlying keras model as _model or model
            keras_model = getattr(self.model, "model", None) or getattr(self.model, "_model", None) or getattr(self.model, "keras_model", None)
            if keras_model is not None and hasattr(keras_model, "input_shape"):
                # input_shape may be (None, N) or similar
                ish = keras_model.input_shape
                if isinstance(ish, (list, tuple)) and len(ish) >= 2:
                    expected_dim = int(ish[1])
        except Exception:
            expected_dim = None

        # if expected dim known, align X columns (trim or pad with zeros)
        if expected_dim is not None:
            if X.shape[1] > expected_dim:
                logger.info("trimming_features", from_cols=X.shape[1], to=expected_dim)
                X = X.iloc[:, :expected_dim]
            elif X.shape[1] < expected_dim:
                logger.info("padding_features", from_cols=X.shape[1], to=expected_dim)
                # add zero columns
                for i in range(expected_dim - X.shape[1]):
                    X[f"_pad_{i}"] = 0.0
                # ensure column order deterministic
                X = X.loc[:, sorted(X.columns)]

        try:

            preds = self.model.predict(X)
        except Exception as e:
            logger.error("model_predict_failed", error=str(e))
            logger.error("X", X=X)
            # mark all as normal fallback
            pandas_df["prediction"] = "normal"
            pandas_df["anomaly_score"] = 0.0
            pandas_df["confidence"] = 0.0
            pandas_df["model_version"] = self.model_version
            
            pandas_df = self._sanitize_pandas_df(pandas_df)
            return self.spark.createDataFrame(pandas_df)

        # Postprocess predictions (same as before)
        try:
            if hasattr(preds, "shape") and preds.shape == X.values.shape:
                mse = np.mean(np.square(preds - X.values), axis=1)
                pandas_df["anomaly_score"] = mse
                thresh = float(getattr(settings, "ANOMALY_MSE_THRESHOLD", 1.0))
                pandas_df["prediction"] = ["anomaly" if v > thresh else "normal" for v in mse]
                pandas_df["confidence"] = [float(1.0 / (1.0 + v)) for v in mse]
            else:
                scores = np.array(preds).reshape(-1)
                pandas_df["anomaly_score"] = scores
                thresh = float(getattr(settings, "ANOMALY_SCORE_THRESHOLD", 0.5))
                pandas_df["prediction"] = ["anomaly" if v > thresh else "normal" for v in scores]
                pandas_df["confidence"] = [float(1.0 / (1.0 + abs(v))) for v in scores]
        except Exception as e:
            logger.error("postprocess_predict_failed", error=str(e))
            pandas_df["prediction"] = "normal"
            pandas_df["anomaly_score"] = 0.0
            pandas_df["confidence"] = 0.0

        pandas_df["model_version"] = self.model_version

        # sanitize again and convert back to Spark DataFrame
        pandas_df = self._sanitize_pandas_df(pandas_df)
        result_df = self.spark.createDataFrame(pandas_df)
        return result_df
    
    def write_to_kafka(self, df, topic: str):
        """
        Write a (static) DataFrame to Kafka topic as JSON messages.
        Used from process_batch for micro-batch outputs.
        """
        if df.rdd.isEmpty():
            return

        # Ensure JSON string in 'value' column
        json_df = df.select(to_json(struct(*df.columns)).alias("value"))
        print(
            "="*80,
            f"\nWriting {json_df.count()} records to Kafka topic '{topic}'",
            "="*80
        )
        # write in append mode
        json_df.write \
            .format("kafka") \
            .mode("append") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", topic) \
            .save()
    
    def process_batch(self, batch_df, batch_id):
        """
        Process a mini-batch of network flows
        """
        if batch_df.isEmpty():
            return

        logger.info("processing_batch", batch_id=batch_id, count=batch_df.count())

        try:
            # Feature engineering: select/cast/fill numeric features
            prepared_df = self.prepare_features(batch_df)

            # Model prediction -> returns dataframe enriched with prediction, anomaly_score, confidence, model_version
            predictions_df = self.predict_anomalies(prepared_df)

            # Split into normal and anomaly
            normal_df = predictions_df.filter(col("prediction") == "normal")
            anomaly_df = predictions_df.filter(col("prediction") == "anomaly")

            # Build output envelopes (keep original flow under original_flow field)
            def envelope(df_in):
                # keep all original fields and add metadata
                cols = df_in.columns
                # add model metadata already present (prediction, confidence, anomaly_score, model_version)
                return df_in

            normal_out = normal_df
            anomaly_out = anomaly_df

            # Write to respective Kafka topics if non-empty
            if not normal_out.rdd.isEmpty():
                self.write_to_kafka(normal_out, "normal_traffic")  #KafkaTopics.NORMAL_TRAFFIC)
            if not anomaly_out.rdd.isEmpty():
                self.write_to_kafka(anomaly_out, "anomaly_alerts")  #KafkaTopics.ANOMALY_ALERTS)

            logger.info(
                "batch_processed",
                batch_id=batch_id,
                normal=int(normal_df.count()),
                anomaly=int(anomaly_df.count())
            )

        except Exception as e:
            logger.error("batch_processing_error", batch_id=batch_id, error=str(e), exc_info=True)
    
    def ensure_topics(self, topics: list):
        """Create missing topics (best-effort). Requires kafka-python installed."""
        try:
            admin = KafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","))
            existing = admin.list_topics()
            to_create = []
            for t in topics:
                if t not in existing:
                    to_create.append(NewTopic(name=t, num_partitions=1, replication_factor=1))
            if to_create:
                admin.create_topics(new_topics=to_create, validate_only=False)
                logger.info("kafka_topics_created", topics=[t.name for t in to_create])
            admin.close()
        except Exception as e:
            logger.warning("ensure_topics_failed", error=str(e))

    def start_streaming(self):
        """Start the streaming pipeline"""
        schema = self.get_schema()

        # ensure topics exist (network flows + outputs)
        self.ensure_topics(
            ["anomaly_alerts", "normal_traffic", "network_flows"]
        )
        # ([KafkaTopics.NETWORK_FLOWS, KafkaTopics.NORMAL_TRAFFIC, KafkaTopics.ANOMALY_ALERTS])

        # stable consumer group id (so it appears in Kafka UI)
        group_id = f"{getattr(settings,'KAFKA_CONSUMER_GROUP_PREFIX','network-anomaly')}-stream"

        # Read from Kafka
        # KafkaTopics.NETWORK_FLOWS)
        stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", "network_flows") \
            .option("startingOffsets", "latest") \
            .option("kafka.group.id", group_id) \
            .load()
        
        # Parse JSON
        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Process in micro-batches
        query = parsed_df.writeStream \
            .foreachBatch(self.process_batch) \
            .option("checkpointLocation", "/tmp/spark_checkpoint") \
            .start()
        
        logger.info("streaming_started")
        query.awaitTermination()


def main():
    """Main entry point"""
    detector = StreamAnomalyDetector()
    
    try:
        # Initialize components
        detector.initialize_spark()
        detector.load_mlflow_model()
        
        # Start streaming
        detector.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("streaming_interrupted")
    except Exception as e:
        logger.error("streaming_failed", error=str(e), exc_info=True)
    finally:
        if detector.spark:
            detector.spark.stop()


if __name__ == "__main__":
    # Configure logging
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer()
        ]
    )
    
    main()