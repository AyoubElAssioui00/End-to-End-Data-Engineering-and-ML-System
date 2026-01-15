from aiokafka import AIOKafkaConsumer
from config.settings import settings
import structlog
from typing import Callable, List, Optional
import json
import asyncio

logger = structlog.get_logger(__name__)


class KafkaConsumerClient:
    """Async Kafka consumer wrapper"""
    
    def __init__(self, group_id: str, topics: List[str], auto_offset_reset: str = 'earliest'):
        self.group_id = f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-{group_id}"
        self.topics = [self._normalize_topic(t) for t in topics]
        self.auto_offset_reset = auto_offset_reset
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False

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
    
    async def start(self):
        """Initialize and start consumer"""
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=True,
            max_poll_records=500,
        )
        await self.consumer.start()
        self.running = True
        logger.info(
            "kafka_consumer_started",
            group_id=self.group_id,
            topics=self.topics
        )
    
    async def stop(self):
        """Stop consumer"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("kafka_consumer_stopped")
    
    async def consume(self, handler: Callable):
        """
        Consume messages and pass to handler
        
        Args:
            handler: Async function that processes messages
        """
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                
                try:
                    await handler(msg.topic, msg.value)
                    logger.debug(
                        "message_processed",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset
                    )
                except Exception as e:
                    logger.error(
                        "error_processing_message",
                        topic=msg.topic,
                        error=str(e),
                        exc_info=True
                    )
        except Exception as e:
            logger.error("consumer_error", error=str(e), exc_info=True)
            raise
    
    async def consume_batch(self, handler: Callable, batch_size: int = 100, timeout_ms: int = 1000):
        """
        Consume messages in batches
        
        Args:
            handler: Async function that processes batch of messages
            batch_size: Number of messages per batch
            timeout_ms: Timeout for batch collection
        """
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        
        batch = []
        
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                
                batch.append({
                    'topic': msg.topic,
                    'value': msg.value,
                    'partition': msg.partition,
                    'offset': msg.offset,
                    'timestamp': msg.timestamp
                })
                
                if len(batch) >= batch_size:
                    try:
                        await handler(batch)
                        logger.debug("batch_processed", batch_size=len(batch))
                        batch = []
                    except Exception as e:
                        logger.error("error_processing_batch", error=str(e), exc_info=True)
                        batch = []
        except Exception as e:
            logger.error("consumer_batch_error", error=str(e), exc_info=True)
            raise