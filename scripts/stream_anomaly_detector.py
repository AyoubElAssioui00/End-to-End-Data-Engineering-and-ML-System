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
    FloatType, TimestampType
)
import mlflow
import mlflow.pyfunc
from common_kafka.topics import KafkaTopics
from config.settings import settings
import structlog

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
        """Define schema for network flow events"""
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("src_ip", StringType(), True),
            StructField("dst_ip", StringType(), True),
            StructField("src_port", IntegerType(), True),
            StructField("dst_port", IntegerType(), True),
            StructField("protocol", StringType(), True),
            StructField("bytes_sent", IntegerType(), True),
            StructField("bytes_received", IntegerType(), True),
            StructField("packets_sent", IntegerType(), True),
            StructField("packets_received", IntegerType(), True),
            StructField("duration", FloatType(), True),
            StructField("flags", StringType(), True),
            StructField("flow_id", StringType(), True),
        ])
    
    def process_batch(self, batch_df, batch_id):
        """
        Process a mini-batch of network flows
        
        Args:
            batch_df: Spark DataFrame containing batch data
            batch_id: Batch identifier
        """
        if batch_df.isEmpty():
            return
        
        logger.info("processing_batch", batch_id=batch_id, count=batch_df.count())
        
        try:
            # TODO: Feature engineering
            # Extract features needed for the model
            features_df = self.prepare_features(batch_df)
            
            # TODO: Model prediction
            # Call MLFlow model for classification
            predictions_df = self.predict_anomalies(features_df)
            
            # Split into normal and anomaly
            normal_df = predictions_df.filter(col("prediction") == "normal")
            anomaly_df = predictions_df.filter(col("prediction") == "anomaly")
            
            # Write to respective Kafka topics
            if not normal_df.isEmpty():
                self.write_to_kafka(normal_df, KafkaTopics.NORMAL_TRAFFIC)
            
            if not anomaly_df.isEmpty():
                self.write_to_kafka(anomaly_df, KafkaTopics.ANOMALY_ALERTS)
            
            logger.info(
                "batch_processed",
                batch_id=batch_id,
                normal=normal_df.count(),
                anomaly=anomaly_df.count()
            )
            
        except Exception as e:
            logger.error("batch_processing_error", batch_id=batch_id, error=str(e), exc_info=True)
    
    def prepare_features(self, df):
        """
        TODO: Prepare features for model prediction
        
        Transform raw network flow data into features expected by the model
        """
        # Example feature engineering (adjust based on your model)
        features_df = df.withColumn("total_bytes", col("bytes_sent") + col("bytes_received")) \
                        .withColumn("total_packets", col("packets_sent") + col("packets_received")) \
                        .withColumn("bytes_per_packet", 
                                  col("total_bytes") / (col("total_packets") + 1))
        
        return features_df
    
    def predict_anomalies(self, df):
        """
        TODO: Use MLFlow model to predict anomalies
        
        Args:
            df: DataFrame with prepared features
        
        Returns:
            DataFrame with predictions and confidence scores
        """
        # Convert to Pandas for MLFlow prediction
        pandas_df = df.toPandas()
        
        # TODO: Select feature columns based on your model
        feature_columns = [
            'bytes_sent', 'bytes_received', 'packets_sent', 
            'packets_received', 'duration', 'total_bytes', 
            'total_packets', 'bytes_per_packet'
        ]
        
        # Make predictions
        predictions = self.model.predict(pandas_df[feature_columns])
        
        # TODO: Adjust based on your model output
        # Assuming model returns binary predictions (0=normal, 1=anomaly)
        pandas_df['prediction'] = ['anomaly' if p == 1 else 'normal' for p in predictions]
        pandas_df['confidence'] = 0.95  # TODO: Get actual confidence from model
        pandas_df['anomaly_score'] = predictions  # TODO: Get actual scores
        pandas_df['model_version'] = self.model_version
        
        # Convert back to Spark DataFrame
        result_df = self.spark.createDataFrame(pandas_df)
        
        return result_df
    
    def write_to_kafka(self, df, topic: str):
        """Write DataFrame to Kafka topic"""
        df.select(
            to_json(struct("*")).alias("value")
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
         .option("topic", topic) \
         .save()
    
    def start_streaming(self):
        """Start the streaming pipeline"""
        schema = self.get_schema()
        
        # Read from Kafka
        stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KafkaTopics.NETWORK_FLOWS) \
            .option("startingOffsets", "latest") \
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