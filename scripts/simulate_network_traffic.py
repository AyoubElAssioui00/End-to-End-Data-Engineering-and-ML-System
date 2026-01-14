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
    
    def __init__(self, data_path: str, delay: float = 0.1):
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
                # Create network flow event from CSV row
                # Adjust field names based on your CSV structure
                event = NetworkFlowEvent(
                    src_ip=str(row.get('src_ip', row.get('source_ip', '0.0.0.0'))),
                    dst_ip=str(row.get('dst_ip', row.get('destination_ip', '0.0.0.0'))),
                    src_port=int(row.get('src_port', row.get('source_port', 0))),
                    dst_port=int(row.get('dst_port', row.get('destination_port', 0))),
                    protocol=str(row.get('protocol', 'TCP')),
                    bytes_sent=int(row.get('bytes_sent', row.get('bytes', 0))),
                    bytes_received=int(row.get('bytes_received', row.get('bytes', 0))),
                    packets_sent=int(row.get('packets_sent', row.get('packets', 0))),
                    packets_received=int(row.get('packets_received', row.get('packets', 0))),
                    duration=float(row.get('duration', 0.0)),
                    flags=str(row.get('flags', '')),
                    flow_id=f"flow_{idx}"
                )
                # print(KafkaTopics.NETWORK_FLOWS)
                
                # Send to Kafka
                await kafka_producer.send_event(
                    topic=KafkaTopics.NETWORK_FLOWS,
                    event=event,
                    key=event.flow_id
                )
                
                if (idx + 1) % 100 == 0:
                    logger.info("progress", records_sent=idx + 1, total=len(self.df))
                
                # Simulate real-time delay
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