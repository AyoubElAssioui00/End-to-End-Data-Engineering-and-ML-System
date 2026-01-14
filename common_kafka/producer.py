from aiokafka import AIOKafkaProducer
from config.settings import settings
import structlog
from common_kafka.topics import BaseEvent
import asyncio
import json
import inspect
from typing import Optional
from enum import Enum
from typing import Union
from kafka.errors import KafkaError

logger = structlog.get_logger(__name__)


class KafkaProducerClient:
    """Async Kafka producer wrapper"""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
    
    async def start(self):
        """Start AIOKafkaProducer with only supported kwargs (compatible across aiokafka versions)."""
        from aiokafka import AIOKafkaProducer
        # from config import settings

        # build desired kwargs (may include keys unsupported by some aiokafka versions)
        producer_kwargs = {
            "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client_id": getattr(settings, "KAFKA_CLIENT_ID", "network_simulator"),
            "linger_ms": getattr(settings, "KAFKA_LINGER_MS", 0),
            "retries": getattr(settings, "KAFKA_PRODUCER_RETRIES", 5),
            # add other desired params here...
        }

        # filter kwargs to only those accepted by the AIOKafkaProducer constructor
        sig = inspect.signature(AIOKafkaProducer.__init__)
        valid_params = set(sig.parameters.keys()) - {"self"}  # parameter names accepted
        filtered_kwargs = {k: v for k, v in producer_kwargs.items() if k in valid_params}

        self.producer = AIOKafkaProducer(**filtered_kwargs)
        await self.producer.start()
        logger.info("kafka_producer_started", bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    
    async def stop(self):
        """Stop producer"""
        if self.producer:
            await self.producer.stop()
            logger.info("kafka_producer_stopped")
    
    async def send_event(self, topic: str, event: BaseEvent, key: Optional[str] = None):
        if not self.producer:
            raise RuntimeError("Producer not started")
        try:
            event_dict = event.model_dump(mode='json')
            value_bytes = json.dumps(event_dict).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None

            send_timeout = getattr(settings, "KAFKA_SEND_TIMEOUT", 10)
            try:
                # enforce a per-send timeout so the simulator doesn't hang indefinitely
                await asyncio.wait_for(
                    self.producer.send_and_wait(topic, value=value_bytes, key=key_bytes),
                    timeout=send_timeout
                )
            except asyncio.TimeoutError:
                logger.error("kafka_send_timeout", topic=topic, timeout=send_timeout, event_id=str(getattr(event, "event_id", None)))
                raise

            logger.debug(
                "event_sent",
                topic=topic,
                event_type=event.event_type,
                event_id=str(event.event_id)
            )
        except Exception as e:
            logger.error(
                "failed_to_send_event",
                topic=topic,
                event_type=getattr(event, "event_type", None),
                error=str(e)
            )
            # don't re-raise if you want simulation to continue on failures; re-raise if you want stop
            raise
    
    async def send_raw(self, topic: str, data: dict, key: Optional[str] = None):
        """Send raw dictionary data to Kafka topic"""
        if not self.producer:
            raise RuntimeError("Producer not started")
        
        try:
            value_bytes = json.dumps(data).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None

            await self.producer.send_and_wait(
                topic,
                value=value_bytes,
                key=key_bytes,
            )
            logger.debug("raw_data_sent", topic=topic)
        except Exception as e:
            logger.error("failed_to_send_raw_data", topic=topic, error=str(e))
            raise

    async def send(self, event: BaseEvent) -> None:
        """
        Send a BaseEvent to Kafka.
        """
        if self.producer is None:
            raise RuntimeError("Kafka producer not started")

        try:
            record_metadata = await self.producer.send_and_wait(
                topic=event.topic,
                value=event,
            )

            logger.info(
                "kafka.message.sent",
                topic=record_metadata.topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                event_type=type(event).__name__,
            )

        except KafkaError as exc:
            logger.error(
                "kafka.message.failed",
                error=str(exc),
                topic=event.topic,
                event_type=type(event).__name__,
            )
            raise

    # -----------------------------
    # Helpers for backwards-compatible API
    # -----------------------------
    def _normalize_topic(self, topic) -> str:
        """
        Convert topic argument (Enum, object or string) to a plain topic name string.
        """
        # Enum (e.g. KafkaTopics.NETWORK_FLOWS)
        if isinstance(topic, Enum):
            # prefer .value if it's a string, otherwise fallback to name
            return topic.value if isinstance(topic.value, str) else topic.name

        # pydantic models / objects that expose 'value' attribute with a string
        if hasattr(topic, "value") and isinstance(getattr(topic, "value"), str):
            return topic.value

        # plain string
        if isinstance(topic, str):
            return topic

        # fallback to str()
        return str(topic)

    async def send_event(self, topic, event: Union[BaseEvent, dict], key: Optional[str] = None) -> None:
        """
        Compatibility wrapper: accepts topic (Enum or str) and an event (BaseEvent or dict).
        Normalizes topic and JSON-serializes the value to bytes before sending.
        """
        if self.producer is None:
            raise RuntimeError("Kafka producer not started")

        topic_str = self._normalize_topic(topic)
        key_bytes = key.encode("utf-8") if key else None
        send_timeout = getattr(settings, "KAFKA_SEND_TIMEOUT", 10)

        # Build JSON payload bytes
        try:
            if isinstance(event, BaseEvent):
                # Prefer pydantic model_dump if available
                if hasattr(event, "model_dump"):
                    payload = event.model_dump(mode="json")
                else:
                    # fallback to __dict__ or to_dict
                    payload = getattr(event, "to_dict", lambda: event.__dict__)()
            elif isinstance(event, dict):
                payload = event
            elif hasattr(event, "model_dump"):
                payload = event.model_dump(mode="json")
            elif hasattr(event, "to_dict"):
                payload = event.to_dict()
            else:
                # last resort: convert to stringifiable dict
                payload = {"value": str(event)}

            value_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        except Exception as exc:
            logger.error("failed_to_serialize_event", error=str(exc), topic=topic_str, event_type=type(event).__name__)
            return

        try:
            await asyncio.wait_for(
                self.producer.send_and_wait(topic=topic_str, value=value_bytes, key=key_bytes),
                timeout=send_timeout,
            )
            logger.info("kafka.message.sent", topic=topic_str, event_type=type(event).__name__)
        except Exception as exc:
            logger.error("kafka.message.failed", error=str(exc), topic=topic_str, event_type=type(event).__name__)
            # swallow for simulator resilience; remove return to propagate errors
            return


# Global producer instance
kafka_producer = KafkaProducerClient()