import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import time
import threading
import os
import sys
from pathlib import Path
from collections import deque
from kafka import KafkaConsumer
from datetime import datetime

# --- Configuration & Setup ---
# Add parent directory to path to import settings if needed
sys.path.append(str(Path(__file__).parent.parent))

# Default Settings (Fallbacks if config.settings fails)
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NORMAL = "normal_traffic"
TOPIC_ANOMALY = "anomaly_alerts"
MAX_BUFFER_SIZE = 2000

# Try importing from project settings
try:
    from config.settings import settings
    KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
except ImportError:
    pass

# --- Page Config ---
st.set_page_config(
    page_title="🛡️ IDS Real-Time Monitor",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Shared State (Thread-Safe Storage) ---
if 'data_buffer' not in st.session_state:
    st.session_state.data_buffer = deque(maxlen=MAX_BUFFER_SIZE)
if 'consumer_active' not in st.session_state:
    st.session_state.consumer_active = False

# --- Background Kafka Consumer ---
def kafka_consumer_thread():
    """
    Background thread to consume messages from both topics
    and update the shared session state buffer.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_NORMAL,
            TOPIC_ANOMALY,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='streamlit-dashboard-live-v1',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        st.session_state.consumer_active = True
        print(f"✅ Dashboard Consumer started on {KAFKA_BOOTSTRAP_SERVERS}")

        for message in consumer:
            data = message.value
            
            # Enrich with topic info for filtering later
            data['topic'] = message.topic
            
            # Ensure timestamp is datetime object
            try:
                # Handle ISO format from Spark/JSON
                data['processed_time'] = datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat()))
            except:
                data['processed_time'] = datetime.now()

            # Append to shared buffer
            st.session_state.data_buffer.append(data)
            
    except Exception as e:
        print(f"❌ Consumer Thread Error: {e}")
        st.session_state.consumer_active = False

# Start the consumer thread only once
if not st.session_state.consumer_active:
    thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    thread.start()
    time.sleep(1) # Give it a moment to connect

# --- Helper Functions ---
def get_data_as_df():
    """Convert deque buffer to Pandas DataFrame"""
    if not st.session_state.data_buffer:
        return pd.DataFrame()
    
    df = pd.DataFrame(list(st.session_state.data_buffer))
    # Sort by time desc
    df = df.sort_values(by='processed_time', ascending=False)
    return df

# --- UI Layout ---

# 1. Header
st.title("🛡️ Network Anomaly Detection System")
st.markdown(f"**Status:** Connected to Kafka ({KAFKA_BOOTSTRAP_SERVERS}) | **Topics:** `{TOPIC_NORMAL}`, `{TOPIC_ANOMALY}`")

# 2. Main Refresh Loop
placeholder = st.empty()

# Auto-refresh logic (using sleep inside the loop logic)
# In a real deployment, use st.empty() containers to update without full page reload feels smoother
    
df = get_data_as_df()

if df.empty:
    st.info("⏳ Waiting for network traffic data... Please start the simulation and detection scripts.")
    time.sleep(2)
    st.rerun()

# --- Key Metrics ---
total_packets = len(df)
normal_df = df[df['topic'] == TOPIC_NORMAL]
anomaly_df = df[df['topic'] == TOPIC_ANOMALY]

count_normal = len(normal_df)
count_anomaly = len(anomaly_df)
anomaly_rate = (count_anomaly / total_packets * 100) if total_packets > 0 else 0

# Average Confidence & Scores
avg_conf = df['confidence'].mean() if 'confidence' in df else 0
avg_anomaly_score = anomaly_df['anomaly_score'].mean() if not anomaly_df.empty else 0

# Top Row Metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Flows Processed", f"{total_packets}", help="Buffered records")
col2.metric("Normal Traffic", f"{count_normal}", delta_color="normal")
col3.metric("Anomalies Detected", f"{count_anomaly}", delta=f"{count_anomaly}", delta_color="inverse")
col4.metric("Anomaly Rate", f"{anomaly_rate:.2f}%", delta_color="inverse")

st.markdown("---")

# --- Charts Section ---
c1, c2 = st.columns([2, 1])

with c1:
    st.subheader("📈 Traffic Timeline")
    # Resample by second to show traffic volume
    if not df.empty:
        # Create a time grouping
        df['time_sec'] = df['processed_time'].dt.floor('5s') # Group by 5 seconds
        timeline = df.groupby(['time_sec', 'prediction']).size().reset_index(name='count')
        
        fig_timeline = px.area(
            timeline, 
            x='time_sec', 
            y='count', 
            color='prediction',
            color_discrete_map={'normal': '#00CC96', 'anomaly': '#EF553B'},
            title="Flows per 5 Seconds",
            markers=True
        )
        st.plotly_chart(fig_timeline, use_container_width=True)

with c2:
    st.subheader("🎯 Model Confidence")
    if not df.empty:
        fig_hist = px.histogram(
            df, 
            x="confidence", 
            color="prediction",
            nbins=20,
            title="Prediction Confidence Distribution",
            color_discrete_map={'normal': '#00CC96', 'anomaly': '#EF553B'},
            barmode="overlay"
        )
        st.plotly_chart(fig_hist, use_container_width=True)

# --- Detailed Analysis Row ---
c3, c4 = st.columns(2)

with c3:
    st.subheader("⚠️ Anomaly Score Analysis")
    if not anomaly_df.empty:
        # Scatter plot: Flow Duration vs Anomaly Score
        fig_scatter = px.scatter(
            anomaly_df,
            x="flow_duration",
            y="anomaly_score",
            size="total_length_of_fwd_packets", # Bubble size based on data volume
            color="confidence",
            hover_data=["flow_id", "label"],
            title="Anomaly Score vs Flow Duration",
            labels={"flow_duration": "Duration (normalized)", "anomaly_score": "MSE Score"}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.success("No anomalies to analyze currently.")

with c4:
    st.subheader("📊 Feature Insights")
    # Quick correlation or distribution of a key feature
    if not df.empty:
        # Let's look at Forward Packet Length Max distinction
        fig_box = px.box(
            df, 
            x="prediction", 
            y="fwd_packet_length_mean",
            color="prediction",
            title="Forward Packet Length Mean by Class",
            color_discrete_map={'normal': '#00CC96', 'anomaly': '#EF553B'}
        )
        st.plotly_chart(fig_box, use_container_width=True)

# --- Recent Alerts Table ---
st.subheader("🚨 Recent Anomaly Alerts")
if not anomaly_df.empty:
    # Select relevant columns for display
    display_cols = [
        'timestamp', 'flow_id', 'prediction', 'confidence', 'anomaly_score', 
        'label', 'flow_duration', 'total_fwd_packets'
    ]
    # Filter only columns that actually exist in the dataframe
    actual_cols = [c for c in display_cols if c in anomaly_df.columns]
    
    st.dataframe(
        anomaly_df[actual_cols].head(10), 
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No active alerts.")

# --- Raw Data Expander ---
with st.expander("View Latest Raw Data Logs"):
    st.dataframe(df.head(20), use_container_width=True)

# --- Auto Refresh Mechanism ---
# Add a small delay and rerun to create the "Live" effect
time.sleep(2)
st.rerun()