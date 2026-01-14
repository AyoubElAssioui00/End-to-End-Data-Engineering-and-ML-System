# Network Anomaly Detection System

Real-time network traffic anomaly detection using Kafka, PySpark Streaming, and MLFlow.

## Architecture

```
CSV Data → Kafka Producer → Kafka (network_flows) → PySpark Streaming + MLFlow Model
                                                            ↓
                                   ┌─────────────────────────┴──────────────────────┐
                                   ↓                                                ↓
                          Kafka (normal_traffic)                        Kafka (anomaly_alerts)
                                   ↓                                                ↓
                                   └─────────────────────────┬──────────────────────┘
                                                            ↓
                                                  Streamlit Dashboard
```

## Project Structure

```
.
├── config/
│   └── settings.py              # Configuration settings
├── kafka/
│   ├── consumer.py              # Kafka consumer wrapper
│   ├── producer.py              # Kafka producer wrapper
│   └── topics.py                # Topic definitions and event models
├── scripts/
│   ├── simulate_network_traffic.py    # Traffic simulator
│   ├── stream_anomaly_detector.py     # PySpark streaming processor
│   └── streamlit_dashboard.py         # Monitoring dashboard
├── data/
│   └── network_traffic.csv      # Your network traffic dataset
├── docker-compose.yml           # Kafka infrastructure
├── requirements.txt             # Python dependencies
├── main.py                      # CLI entry point
└── README.md                    # This file
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This will start:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Kafka UI (port 8080)

Access Kafka UI at: http://localhost:8080

### 3. Configure Environment

Create a `.env` file in the project root:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Data Configuration
DATA_PATH=./data/network_traffic.csv
SIMULATION_DELAY=0.1

# Streaming Configuration
BATCH_SIZE=100
BATCH_TIMEOUT_MS=5000

# MLFlow Configuration
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_MODEL_NAME=network-anomaly-detector
MLFLOW_MODEL_STAGE=Production

# Dashboard Configuration
DASHBOARD_REFRESH_INTERVAL=5
DASHBOARD_MAX_RECORDS=1000

# Logging
LOG_LEVEL=INFO
```

### 4. Prepare Your Data

Place your network traffic CSV file in `./data/network_traffic.csv`

Expected columns (adjust in `simulate_network_traffic.py` if different):
- `src_ip` / `source_ip`
- `dst_ip` / `destination_ip`
- `src_port` / `source_port`
- `dst_port` / `destination_port`
- `protocol`
- `bytes_sent` / `bytes`
- `bytes_received`
- `packets_sent` / `packets`
- `packets_received`
- `duration`
- `flags` (optional)

### 5. Setup MLFlow Model

Before running the detector, ensure you have a trained model in MLFlow:

```bash
# Start MLFlow server
mlflow server --host 0.0.0.0 --port 5000

# Train and register your model
# See MLFlow documentation for model registration
```

## Usage

### Using the CLI (Recommended)

The project includes a CLI for easy component management:

```bash
# Show all commands
python main.py --help

# 1. Simulate network traffic
python main.py simulate --data-path ./data/network_traffic.csv --delay 0.1

# 2. Run anomaly detector
python main.py detect

# 3. Launch dashboard
python main.py dashboard --port 8501
```

### Running Components Separately

Open 3 terminal windows:

**Terminal 1 - Traffic Simulator:**
```bash
python scripts/simulate_network_traffic.py
```

**Terminal 2 - Anomaly Detector:**
```bash
python scripts/stream_anomaly_detector.py
```

**Terminal 3 - Dashboard:**
```bash
streamlit run scripts/streamlit_dashboard.py
```

## Components

### 1. Traffic Simulator (`simulate_network_traffic.py`)

Reads network traffic from CSV and produces to Kafka topic `network_flows` with configurable delay to simulate real-time traffic.

**Features:**
- CSV data loading and validation
- Real-time simulation with configurable delay
- Automatic event schema generation
- Progress logging

### 2. Anomaly Detector (`stream_anomaly_detector.py`)

PySpark Streaming application that:
- Consumes from `network_flows` topic
- Processes data in mini-batches
- Applies feature engineering
- Uses MLFlow model for classification
- Produces to `normal_traffic` or `anomaly_alerts` topics

**TODOs to Complete:**
- `prepare_features()`: Implement your feature engineering logic
- `predict_anomalies()`: Adjust for your specific model's input/output format

### 3. Dashboard (`streamlit_dashboard.py`)

Real-time monitoring dashboard showing:
- Key metrics (total flows, anomalies, detection rate)
- Traffic timeline (normal vs anomaly)
- Anomaly distribution by protocol and source IP
- Recent anomaly alerts

Access at: http://localhost:8501

## Kafka Topics

- `network_flows`: Raw network traffic data
- `normal_traffic`: Classified normal traffic
- `anomaly_alerts`: Detected anomalies

## Development

### Adding Custom Features

1. **Modify Event Schema** (`kafka/topics.py`):
   - Add fields to `NetworkFlowEvent`
   - Update serialization logic

2. **Update Feature Engineering** (`stream_anomaly_detector.py`):
   - Modify `prepare_features()` method
   - Add domain-specific transformations

3. **Customize Dashboard** (`streamlit_dashboard.py`):
   - Add new visualizations
   - Implement custom metrics

### Testing

```bash
# Test Kafka connection
python -c "from kafka.producer import kafka_producer; import asyncio; asyncio.run(kafka_producer.start())"

# Test data loading
python -c "from scripts.simulate_network_traffic import NetworkTrafficSimulator; s = NetworkTrafficSimulator('./data/network_traffic.csv', 0.1); s.load_data()"
```

## Monitoring

- **Kafka UI**: http://localhost:8080 - View topics, messages, consumer groups
- **Streamlit Dashboard**: http://localhost:8501 - Real-time anomaly monitoring
- **MLFlow UI**: http://localhost:5000 - Model tracking and versioning

## Troubleshooting

### Kafka Connection Issues
```bash
# Check if Kafka is running
docker ps

# View Kafka logs
docker logs kafka

# Restart Kafka infrastructure
docker-compose restart
```

### PySpark Issues
```bash
# Check Spark logs in /tmp/spark_checkpoint
# Ensure Java is installed (required for PySpark)
java -version
```

### Model Loading Issues
```bash
# Verify MLFlow server is running
curl http://localhost:5000/health

# Check model exists
mlflow models list
```

## Performance Tuning

- **Simulation Speed**: Adjust `SIMULATION_DELAY` in `.env`
- **Batch Size**: Modify `BATCH_SIZE` for streaming processor
- **Dashboard Refresh**: Change `DASHBOARD_REFRESH_INTERVAL`
- **Kafka Partitions**: Increase for parallel processing

## License

MIT License

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request