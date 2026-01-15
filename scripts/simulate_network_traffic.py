"""
Network Traffic Simulator
Reads network traffic data from CSV and produces to Kafka topic in real-time
"""

import asyncio
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from common_kafka.producer import kafka_producer
from common_kafka.topics import KafkaTopics, NetworkFlowEvent
from config.settings import settings
import structlog

logger = structlog.get_logger(__name__)


class NetworkTrafficSimulator:
    """Simulates real-time network traffic by reading from CSV"""
    
    def __init__(self, data_path: str, delay: float = 10.0):
        self.data_path = data_path
        self.delay = delay
        self.df = None
    
    def load_data(self):
        """Load network traffic data from CSV (robust to unquoted commas in fields)."""
        try:
            try:
                # fast C engine first
                self.df = pd.read_csv(self.data_path)
            except pd.errors.ParserError as pe:
                logger.warning("csv_parse_error_default_engine", error=str(pe), path=self.data_path)
                # retry with python engine and permissive options (will warn/skip malformed lines)
                self.df = pd.read_csv(
                    self.data_path,
                    engine='python',
                    sep=',',
                    quotechar='"',
                    skipinitialspace=True,
                    on_bad_lines='warn'  # 'warn' or 'skip' depending on pandas version and preference
                )

            logger.info("data_loaded", path=self.data_path, records=len(self.df))
            logger.info("columns", columns=list(self.df.columns))
        except Exception as e:
            logger.error("failed_to_load_data", path=self.data_path, error=str(e))
            raise
    
    async def simulate(self):
        """Simulate real-time traffic by sending records with delay"""
        if self.df is None:
            self.load_data()
        
        logger.info("starting_simulation", total_records=len(self.df), delay=self.delay)
        
        for idx, row in self.df.iterrows():
            try:
                # helper getters that try multiple possible column names and coerce types
                def _get_str(r, *names, default=None):
                    for n in names:
                        v = r.get(n)
                        if pd.isna(v) or v is None:
                            continue
                        return str(v)
                    return default

                def _get_int(r, *names, default=None):
                    for n in names:
                        v = r.get(n)
                        if pd.isna(v) or v is None:
                            continue
                        try:
                            return int(float(v))
                        except Exception:
                            continue
                    return default

                def _get_float(r, *names, default=None):
                    for n in names:
                        v = r.get(n)
                        if pd.isna(v) or v is None:
                            continue
                        try:
                            return float(v)
                        except Exception:
                            continue
                    return default

                # build event (same mapping as before)
                event = NetworkFlowEvent(
                    src_ip=_get_str(row, "src_ip", "Src_IP", "source_ip", "Source IP"),
                    dst_ip=_get_str(row, "dst_ip", "Dst_IP", "destination_ip", "Destination IP"),
                    src_port=_get_int(row, "src_port", "Src_Port", "Source Port", "source_port"),
                    dst_port=_get_int(row, "dst_port", "Dst_Port", "Destination Port", "destination_port"),
                    protocol=_get_str(row, "protocol", "Protocol"),
                    flow_id=_get_str(row, "flow_id", "Flow_ID", default=f"flow_{idx}"),

                    # numeric features (sanitized names used by preprocessing)
                    flow_duration=_get_float(row, "Flow_Duration", "flow_duration", "duration"),
                    total_fwd_packets=_get_float(row, "Total_Fwd_Packets", "total_fwd_packets"),
                    total_length_of_fwd_packets=_get_float(row, "Total_Length_of_Fwd_Packets", "total_length_of_fwd_packets"),
                    fwd_packet_length_max=_get_float(row, "Fwd_Packet_Length_Max", "fwd_packet_length_max"),
                    fwd_packet_length_min=_get_float(row, "Fwd_Packet_Length_Min", "fwd_packet_length_min"),
                    fwd_packet_length_mean=_get_float(row, "Fwd_Packet_Length_Mean", "fwd_packet_length_mean"),
                    fwd_packet_length_std=_get_float(row, "Fwd_Packet_Length_Std", "fwd_packet_length_std"),
                    bwd_packet_length_max=_get_float(row, "Bwd_Packet_Length_Max", "bwd_packet_length_max"),
                    bwd_packet_length_min=_get_float(row, "Bwd_Packet_Length_Min", "bwd_packet_length_min"),
                    bwd_packet_length_mean=_get_float(row, "Bwd_Packet_Length_Mean", "bwd_packet_length_mean"),
                    bwd_packet_length_std=_get_float(row, "Bwd_Packet_Length_Std", "bwd_packet_length_std"),

                    flow_bytes_s=_get_float(row, "Flow_Bytes_s", "flow_bytes_s"),
                    flow_packets_s=_get_float(row, "Flow_Packets_s", "flow_packets_s"),
                    flow_iat_mean=_get_float(row, "Flow_IAT_Mean", "flow_iat_mean"),
                    flow_iat_std=_get_float(row, "Flow_IAT_Std", "flow_iat_std"),
                    flow_iat_max=_get_float(row, "Flow_IAT_Max", "flow_iat_max"),
                    flow_iat_min=_get_float(row, "Flow_IAT_Min", "flow_iat_min"),

                    fwd_iat_total=_get_float(row, "Fwd_IAT_Total", "fwd_iat_total"),
                    fwd_iat_mean=_get_float(row, "Fwd_IAT_Mean", "fwd_iat_mean"),
                    fwd_iat_std=_get_float(row, "Fwd_IAT_Std", "fwd_iat_std"),
                    fwd_iat_max=_get_float(row, "Fwd_IAT_Max", "fwd_iat_max"),
                    fwd_iat_min=_get_float(row, "Fwd_IAT_Min", "fwd_iat_min"),

                    bwd_iat_total=_get_float(row, "Bwd_IAT_Total", "bwd_iat_total"),
                    bwd_iat_mean=_get_float(row, "Bwd_IAT_Mean", "bwd_iat_mean"),
                    bwd_iat_std=_get_float(row, "Bwd_IAT_Std", "bwd_iat_std"),
                    bwd_iat_max=_get_float(row, "Bwd_IAT_Max", "bwd_iat_max"),
                    bwd_iat_min=_get_float(row, "Bwd_IAT_Min", "bwd_iat_min"),

                    bwd_psh_flags=_get_float(row, "Bwd_PSH_Flags", "bwd_psh_flags"),
                    bwd_urg_flags=_get_float(row, "Bwd_URG_Flags", "bwd_urg_flags"),
                    fwd_header_length=_get_float(row, "Fwd_Header_Length", "fwd_header_length"),
                    bwd_header_length=_get_float(row, "Bwd_Header_Length", "bwd_header_length"),

                    fwd_packets_s=_get_float(row, "Fwd_Packets_s", "fwd_packets_s"),
                    bwd_packets_s=_get_float(row, "Bwd_Packets_s", "bwd_packets_s"),
                    min_packet_length=_get_float(row, "Min_Packet_Length", "min_packet_length"),
                    max_packet_length=_get_float(row, "Max_Packet_Length", "max_packet_length"),
                    packet_length_mean=_get_float(row, "Packet_Length_Mean", "packet_length_mean"),
                    packet_length_std=_get_float(row, "Packet_Length_Std", "packet_length_std"),
                    packet_length_variance=_get_float(row, "Packet_Length_Variance", "packet_length_variance"),

                    fin_flag_count=_get_float(row, "FIN_Flag_Count", "fin_flag_count"),
                    syn_flag_count=_get_float(row, "SYN_Flag_Count", "syn_flag_count"),
                    psh_flag_count=_get_float(row, "PSH_Flag_Count", "psh_flag_count"),
                    ack_flag_count=_get_float(row, "ACK_Flag_Count", "ack_flag_count"),
                    cwe_flag_count=_get_float(row, "CWE_Flag_Count", "cwe_flag_count"),

                    average_packet_size=_get_float(row, "Average_Packet_Size", "average_packet_size"),
                    fwd_header_length_1=_get_float(row, "Fwd_Header_Length_1", "fwd_header_length_1"),
                    fwd_avg_bytes_bulk=_get_float(row, "Fwd_Avg_Bytes_Bulk", "fwd_avg_bytes_bulk"),
                    fwd_avg_packets_bulk=_get_float(row, "Fwd_Avg_Packets_Bulk", "fwd_avg_packets_bulk"),
                    fwd_avg_bulk_rate=_get_float(row, "Fwd_Avg_Bulk_Rate", "fwd_avg_bulk_rate"),

                    bwd_avg_bytes_bulk=_get_float(row, "Bwd_Avg_Bytes_Bulk", "bwd_avg_bytes_bulk"),
                    bwd_avg_packets_bulk=_get_float(row, "Bwd_Avg_Packets_Bulk", "bwd_avg_packets_bulk"),
                    bwd_avg_bulk_rate=_get_float(row, "Bwd_Avg_Bulk_Rate", "bwd_avg_bulk_rate"),

                    subflow_fwd_packets=_get_float(row, "Subflow_Fwd_Packets", "subflow_fwd_packets"),
                    subflow_fwd_bytes=_get_float(row, "Subflow_Fwd_Bytes", "subflow_fwd_bytes"),
                    subflow_bwd_packets=_get_float(row, "Subflow_Bwd_Packets", "subflow_bwd_packets"),

                    init_win_bytes_forward=_get_float(row, "Init_Win_bytes_forward", "init_win_bytes_forward"),
                    init_win_bytes_backward=_get_float(row, "Init_Win_bytes_backward", "init_win_bytes_backward"),
                    act_data_pkt_fwd=_get_float(row, "act_data_pkt_fwd", "act_data_pkt_fwd"),
                    min_seg_size_forward=_get_float(row, "min_seg_size_forward", "min_seg_size_forward"),

                    active_mean=_get_float(row, "Active_Mean", "active_mean"),
                    active_std=_get_float(row, "Active_Std", "active_std"),
                    active_max=_get_float(row, "Active_Max", "active_max"),
                    active_min=_get_float(row, "Active_Min", "active_min"),
                    idle_mean=_get_float(row, "Idle_Mean", "idle_mean"),
                    idle_std=_get_float(row, "Idle_Std", "idle_std"),
                    idle_max=_get_float(row, "Idle_Max", "idle_max"),
                    idle_min=_get_float(row, "Idle_Min", "idle_min"),

                    # label if present
                    label=_get_str(row, "Label", "label")
                )

                # Send to Kafka with timeout and ensure sequential send
                send_timeout = getattr(settings, "KAFKA_SEND_TIMEOUT", 5.0)
                try:
                    await asyncio.wait_for(
                        kafka_producer.send_event(
                            topic=KafkaTopics.NETWORK_FLOWS,
                            event=event,
                            key=event.flow_id
                        ),
                        timeout=send_timeout
                    )
                except asyncio.TimeoutError:
                    logger.error("kafka.send_timeout", index=idx, flow_id=event.flow_id, timeout=send_timeout)
                    # Decide: continue (skip) or retry. Here we skip and continue to next record.
                    continue
                except Exception as e:
                    logger.error("kafka.send_failed", index=idx, flow_id=getattr(event, "flow_id", None), error=str(e))
                    continue

                if (idx + 1) % 100 == 0:
                    logger.info("progress", records_sent=idx + 1, total=len(self.df))

                # Respect configured simulation delay between successive sends
                await asyncio.sleep(self.delay)

            except Exception as e:
                logger.error("failed_to_send_record", index=idx, error=str(e))
                continue

        logger.info("simulation_completed", total_sent=len(self.df))


async def main():
    """Main entry point"""
    simulator = NetworkTrafficSimulator(
        data_path=settings.DATA_PATH,
        delay=settings.SIMULATION_DELAY
    )
    
    try:
        # Start Kafka producer
        await kafka_producer.start()
        
        # Load and validate data
        simulator.load_data()
        
        # Run simulation
        await simulator.simulate()
        
    except KeyboardInterrupt:
        logger.info("simulation_interrupted")
    except Exception as e:
        logger.error("simulation_failed", error=str(e), exc_info=True)
    finally:
        await kafka_producer.stop()


if __name__ == "__main__":
    # Configure logging
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer()
        ]
    )
    
    asyncio.run(main())