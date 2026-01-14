"""
Main entry point for the Network Anomaly Detection System
Provides CLI interface to run different components
"""

import asyncio
import click
import structlog
from pathlib import Path

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()
    ]
)

logger = structlog.get_logger(__name__)


@click.group()
def cli():
    """Network Anomaly Detection System CLI"""
    pass


@cli.command()
@click.option('--data-path', default='./data/network_traffic.csv', help='Path to network traffic CSV')
@click.option('--delay', default=0.1, type=float, help='Delay between records (seconds)')
def simulate(data_path, delay):
    """Simulate network traffic from CSV file"""
    from scripts.simulate_network_traffic import NetworkTrafficSimulator, kafka_producer
    
    async def run():
        simulator = NetworkTrafficSimulator(data_path=data_path, delay=delay)
        try:
            await kafka_producer.start()
            simulator.load_data()
            await simulator.simulate()
        finally:
            await kafka_producer.stop()
    
    logger.info("starting_traffic_simulation", data_path=data_path, delay=delay)
    asyncio.run(run())


@cli.command()
def detect():
    """Run anomaly detection streaming processor"""
    from scripts.stream_anomaly_detector import StreamAnomalyDetector
    
    logger.info("starting_anomaly_detector")
    detector = StreamAnomalyDetector()
    
    try:
        detector.initialize_spark()
        detector.load_mlflow_model()
        detector.start_streaming()
    except KeyboardInterrupt:
        logger.info("detector_interrupted")
    finally:
        if detector.spark:
            detector.spark.stop()


@cli.command()
@click.option('--port', default=8501, help='Streamlit dashboard port')
def dashboard(port):
    """Launch Streamlit monitoring dashboard"""
    import subprocess
    
    logger.info("starting_dashboard", port=port)
    
    dashboard_path = Path(__file__).parent / "scripts" / "streamlit_dashboard.py"
    
    subprocess.run([
        "streamlit", "run",
        str(dashboard_path),
        "--server.port", str(port)
    ])


@cli.command()
def setup():
    """Setup Kafka topics and check connections"""
    from kafka_admin import create_topics
    
    logger.info("setting_up_infrastructure")
    
    try:
        create_topics()
        logger.info("setup_completed")
    except Exception as e:
        logger.error("setup_failed", error=str(e))


@cli.command()
def all():
    """Run all components (simulation, detection, dashboard)"""
    logger.info("starting_all_components")
    click.echo("This will start all components. Use separate terminals for better control.")
    click.echo("Run these commands in different terminals:")
    click.echo("  1. python main.py simulate")
    click.echo("  2. python main.py detect")
    click.echo("  3. python main.py dashboard")


if __name__ == '__main__':
    cli()