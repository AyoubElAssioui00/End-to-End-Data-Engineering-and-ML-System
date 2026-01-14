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
    """Network flow data event"""
    
    event_type: str = "network_flow"
    
    # Network flow fields (adjust based on your CSV columns)
    src_ip: str
    dst_ip: str
    src_port: int
    dst_port: int
    protocol: str
    bytes_sent: int
    bytes_received: int
    packets_sent: int
    packets_received: int
    duration: float
    flags: Optional[str] = None
    
    # Additional features
    flow_id: Optional[str] = None


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