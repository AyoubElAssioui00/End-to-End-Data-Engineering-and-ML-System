from kafka import KafkaAdminClient
from config.settings import settings
import sys

print(f"--- CONFIG CHECK ---")
print(f"Settings attempting to connect to: {settings.KAFKA_BOOTSTRAP_SERVERS}")

if "9092" in settings.KAFKA_BOOTSTRAP_SERVERS and "29092" not in settings.KAFKA_BOOTSTRAP_SERVERS:
    print("\n[!] WARNING: You are using port 9092. On the host machine, you usually need 29092.")
    print("    Please check your .env file or settings.py.\n")

try:
    print("Attempting connection...")
    admin = KafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    print("SUCCESS! Connected to Kafka.")
    print(f"Cluster topics: {admin.list_topics()}")
    admin.close()
except Exception as e:
    print(f"\nFAILURE: Could not connect to Kafka.")
    print(f"Error: {e}")
    sys.exit(1)