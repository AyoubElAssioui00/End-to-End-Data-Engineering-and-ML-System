from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
import uuid


class KafkaTopics(str, Enum):
    """Centralized Kafka topic definitions"""
    
    NETWORK_FLOWS = "network_flows"
    NORMAL_TRAFFIC = "normal_traffic"
    ANOMALY_ALERTS = "anomaly_alerts"


class BaseEvent(BaseModel):
    """Base event model for all Kafka messages"""
    
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Optional[Dict[str, Any]] = None


class NetworkFlowEvent(BaseEvent):
    """Network flow data event (fields aligned with data/network_traffic.csv)"""
    
    event_type: str = "network_flow"
    
    # Optional network identifiers (may be absent in CSV-based flows)
    # src_ip: Optional[str] = None
    # dst_ip: Optional[str] = None
    # src_port: Optional[int] = None
    # dst_port: Optional[int] = None
    # protocol: Optional[str] = None
    flow_id: Optional[str] = None

    # Numeric features (use Optional[float] so partial records are accepted)
    flow_duration: Optional[float] = None
    total_fwd_packets: Optional[float] = None
    total_length_of_fwd_packets: Optional[float] = None
    fwd_packet_length_max: Optional[float] = None
    fwd_packet_length_min: Optional[float] = None
    fwd_packet_length_mean: Optional[float] = None
    fwd_packet_length_std: Optional[float] = None
    bwd_packet_length_max: Optional[float] = None
    bwd_packet_length_min: Optional[float] = None
    bwd_packet_length_mean: Optional[float] = None
    bwd_packet_length_std: Optional[float] = None
    flow_bytes_s: Optional[float] = None
    flow_packets_s: Optional[float] = None
    flow_iat_mean: Optional[float] = None
    flow_iat_std: Optional[float] = None
    flow_iat_max: Optional[float] = None
    flow_iat_min: Optional[float] = None
    fwd_iat_total: Optional[float] = None
    fwd_iat_mean: Optional[float] = None
    fwd_iat_std: Optional[float] = None
    fwd_iat_max: Optional[float] = None
    fwd_iat_min: Optional[float] = None
    bwd_iat_total: Optional[float] = None
    bwd_iat_mean: Optional[float] = None
    bwd_iat_std: Optional[float] = None
    bwd_iat_max: Optional[float] = None
    bwd_iat_min: Optional[float] = None
    bwd_psh_flags: Optional[float] = None
    bwd_urg_flags: Optional[float] = None
    fwd_header_length: Optional[float] = None
    bwd_header_length: Optional[float] = None
    fwd_packets_s: Optional[float] = None
    bwd_packets_s: Optional[float] = None
    min_packet_length: Optional[float] = None
    max_packet_length: Optional[float] = None
    packet_length_mean: Optional[float] = None
    packet_length_std: Optional[float] = None
    packet_length_variance: Optional[float] = None
    fin_flag_count: Optional[float] = None
    syn_flag_count: Optional[float] = None
    psh_flag_count: Optional[float] = None
    ack_flag_count: Optional[float] = None
    cwe_flag_count: Optional[float] = None
    average_packet_size: Optional[float] = None
    fwd_header_length_1: Optional[float] = None
    fwd_avg_bytes_bulk: Optional[float] = None
    fwd_avg_packets_bulk: Optional[float] = None
    fwd_avg_bulk_rate: Optional[float] = None
    bwd_avg_bytes_bulk: Optional[float] = None
    bwd_avg_packets_bulk: Optional[float] = None
    bwd_avg_bulk_rate: Optional[float] = None
    subflow_fwd_packets: Optional[float] = None
    subflow_fwd_bytes: Optional[float] = None
    subflow_bwd_packets: Optional[float] = None
    init_win_bytes_forward: Optional[float] = None
    init_win_bytes_backward: Optional[float] = None
    act_data_pkt_fwd: Optional[float] = None
    min_seg_size_forward: Optional[float] = None
    active_mean: Optional[float] = None
    active_std: Optional[float] = None
    active_max: Optional[float] = None
    active_min: Optional[float] = None
    idle_mean: Optional[float] = None
    idle_std: Optional[float] = None
    idle_max: Optional[float] = None
    idle_min: Optional[float] = None

    # Optional label when present (e.g. BENIGN / attack type)
    label: Optional[str] = None


class AnomalyAlertEvent(BaseEvent):
    """Anomaly detection alert event"""
    
    event_type: str = "anomaly_alert"
    
    flow_id: str
    prediction: str  # 'normal' or 'anomaly'
    confidence: float
    model_version: str
    anomaly_score: Optional[float] = None
    original_flow: Dict[str, Any]


class NormalTrafficEvent(BaseEvent):
    """Normal traffic event"""
    
    event_type: str = "normal_traffic"
    
    flow_id: str
    prediction: str = "normal"
    confidence: float
    model_version: str
    original_flow: Dict[str, Any]