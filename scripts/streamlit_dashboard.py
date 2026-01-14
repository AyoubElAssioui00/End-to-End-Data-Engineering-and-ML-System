"""
Streamlit Dashboard for Network Anomaly Monitoring
Consumes from normal_traffic and anomaly_alerts topics and visualizes metrics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import asyncio
import sys
from pathlib import Path
from collections import deque

sys.path.append(str(Path(__file__).parent.parent))

from kafka.consumer import KafkaConsumerClient
from kafka.topics import KafkaTopics
from config.settings import settings
import structlog

logger = structlog.get_logger(__name__)


class DashboardState:
    """Maintain dashboard state"""
    
    def __init__(self, max_records=1000):
        self.max_records = max_records
        self.normal_traffic = deque(maxlen=max_records)
        self.anomalies = deque(maxlen=max_records)
        self.metrics = {
            'total_flows': 0,
            'normal_count': 0,
            'anomaly_count': 0,
            'anomaly_rate': 0.0
        }
    
    def add_normal(self, data):
        """Add normal traffic record"""
        self.normal_traffic.append(data)
        self.metrics['total_flows'] += 1
        self.metrics['normal_count'] += 1
        self._update_rate()
    
    def add_anomaly(self, data):
        """Add anomaly record"""
        self.anomalies.append(data)
        self.metrics['total_flows'] += 1
        self.metrics['anomaly_count'] += 1
        self._update_rate()
    
    def _update_rate(self):
        """Update anomaly rate"""
        if self.metrics['total_flows'] > 0:
            self.metrics['anomaly_rate'] = (
                self.metrics['anomaly_count'] / self.metrics['total_flows']
            ) * 100
    
    def get_normal_df(self):
        """Get normal traffic as DataFrame"""
        return pd.DataFrame(list(self.normal_traffic)) if self.normal_traffic else pd.DataFrame()
    
    def get_anomaly_df(self):
        """Get anomalies as DataFrame"""
        return pd.DataFrame(list(self.anomalies)) if self.anomalies else pd.DataFrame()


# Initialize state
if 'dashboard_state' not in st.session_state:
    st.session_state.dashboard_state = DashboardState(
        max_records=settings.DASHBOARD_MAX_RECORDS
    )


async def consume_messages():
    """Consume messages from Kafka topics"""
    state = st.session_state.dashboard_state
    
    async def handle_message(topic, value):
        """Handle incoming message"""
        if topic == KafkaTopics.NORMAL_TRAFFIC:
            state.add_normal(value)
        elif topic == KafkaTopics.ANOMALY_ALERTS:
            state.add_anomaly(value)
    
    # Create consumers
    consumer = KafkaConsumerClient(
        group_id="dashboard",
        topics=[KafkaTopics.NORMAL_TRAFFIC, KafkaTopics.ANOMALY_ALERTS],
        auto_offset_reset='latest'
    )
    
    try:
        await consumer.start()
        await consumer.consume(handle_message)
    except Exception as e:
        logger.error("consumer_error", error=str(e))
    finally:
        await consumer.stop()


def render_metrics():
    """Render key metrics"""
    state = st.session_state.dashboard_state
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Flows", f"{state.metrics['total_flows']:,}")
    
    with col2:
        st.metric("Normal Traffic", f"{state.metrics['normal_count']:,}")
    
    with col3:
        st.metric(
            "Anomalies Detected", 
            f"{state.metrics['anomaly_count']:,}",
            delta=None if state.metrics['anomaly_count'] == 0 else "Alert"
        )
    
    with col4:
        st.metric(
            "Anomaly Rate", 
            f"{state.metrics['anomaly_rate']:.2f}%",
            delta=f"{state.metrics['anomaly_rate']:.2f}%" if state.metrics['anomaly_rate'] > 5 else None
        )


def render_timeline_chart():
    """Render timeline of normal vs anomaly traffic"""
    st.subheader("Traffic Timeline")
    
    state = st.session_state.dashboard_state
    normal_df = state.get_normal_df()
    anomaly_df = state.get_anomaly_df()
    
    if normal_df.empty and anomaly_df.empty:
        st.info("Waiting for data...")
        return
    
    # Prepare data
    timeline_data = []
    
    if not normal_df.empty:
        normal_counts = normal_df.groupby(
            pd.to_datetime(normal_df['timestamp']).dt.floor('1min')
        ).size().reset_index(name='count')
        normal_counts['type'] = 'Normal'
        timeline_data.append(normal_counts)
    
    if not anomaly_df.empty:
        anomaly_counts = anomaly_df.groupby(
            pd.to_datetime(anomaly_df['timestamp']).dt.floor('1min')
        ).size().reset_index(name='count')
        anomaly_counts['type'] = 'Anomaly'
        timeline_data.append(anomaly_counts)
    
    if timeline_data:
        combined_df = pd.concat(timeline_data, ignore_index=True)
        
        fig = px.line(
            combined_df,
            x='timestamp',
            y='count',
            color='type',
            title='Network Traffic Over Time',
            color_discrete_map={'Normal': 'green', 'Anomaly': 'red'}
        )
        
        st.plotly_chart(fig, use_container_width=True)


def render_anomaly_distribution():
    """Render anomaly distribution by protocol/port"""
    st.subheader("Anomaly Analysis")
    
    state = st.session_state.dashboard_state
    anomaly_df = state.get_anomaly_df()
    
    if anomaly_df.empty:
        st.info("No anomalies detected yet")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Protocol distribution
        if 'original_flow' in anomaly_df.columns:
            protocols = [flow.get('protocol', 'Unknown') for flow in anomaly_df['original_flow']]
            protocol_df = pd.DataFrame({'protocol': protocols})
            protocol_counts = protocol_df['protocol'].value_counts()
            
            fig = px.pie(
                values=protocol_counts.values,
                names=protocol_counts.index,
                title='Anomalies by Protocol'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top source IPs
        if 'original_flow' in anomaly_df.columns:
            src_ips = [flow.get('src_ip', 'Unknown') for flow in anomaly_df['original_flow']]
            src_ip_df = pd.DataFrame({'src_ip': src_ips})
            top_ips = src_ip_df['src_ip'].value_counts().head(10)
            
            fig = px.bar(
                x=top_ips.values,
                y=top_ips.index,
                orientation='h',
                title='Top 10 Anomaly Source IPs',
                labels={'x': 'Count', 'y': 'Source IP'}
            )
            st.plotly_chart(fig, use_container_width=True)


def render_recent_anomalies():
    """Render table of recent anomalies"""
    st.subheader("Recent Anomalies")
    
    state = st.session_state.dashboard_state
    anomaly_df = state.get_anomaly_df()
    
    if anomaly_df.empty:
        st.info("No anomalies detected yet")
        return
    
    # Display most recent anomalies
    recent = anomaly_df.tail(10)[::-1]  # Last 10, reversed
    
    display_df = pd.DataFrame({
        'Timestamp': recent['timestamp'],
        'Flow ID': recent['flow_id'],
        'Confidence': recent['confidence'].apply(lambda x: f"{x:.2%}"),
        'Model Version': recent['model_version']
    })
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def main():
    """Main dashboard function"""
    st.set_page_config(
        page_title="Network Anomaly Dashboard",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Network Anomaly Detection Dashboard")
    st.markdown("Real-time monitoring of network traffic anomalies")
    
    # Render components
    render_metrics()
    st.divider()
    
    render_timeline_chart()
    st.divider()
    
    render_anomaly_distribution()
    st.divider()
    
    render_recent_anomalies()
    
    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Auto-refresh
    st.button("Refresh Data")


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
    
    # Note: To make this truly real-time, you would need to run the Kafka consumer
    # in a separate thread or use Streamlit's experimental features for async updates