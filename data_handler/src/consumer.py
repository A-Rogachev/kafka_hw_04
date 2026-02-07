import asyncio
import json
import logging
import signal

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from aiokafka import errors as kafka_errors

from src.config import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

YELLOW_COLOR = "\033[93m"
GREEN_COLOR = "\033[92m"
RESET_COLOR = "\033[0m"


def print_message(msg: ConsumerRecord) -> None:
    print(f"\n{YELLOW_COLOR + '=' * 50 + RESET_COLOR}")
    print(f"{GREEN_COLOR}Topic:{RESET_COLOR} {msg.topic}")
    print(f"{GREEN_COLOR}Partition:{RESET_COLOR} {msg.partition}")
    print(f"{GREEN_COLOR}Offset:{RESET_COLOR} {msg.offset}")
    print(f"{GREEN_COLOR}-- Key:{RESET_COLOR} {str(msg.key)} (record identifier)")
    print(f"{GREEN_COLOR}-- Value:{RESET_COLOR} {json.dumps(msg.value, indent=2)}")


async def consume(config: Settings) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    topic_names: str = ", ".join(config.broker.topic_names)
    consumer, connected = None, False
    while not stop_event.is_set():
        consumer = AIOKafkaConsumer(
            *config.broker.topic_names,
            bootstrap_servers=config.broker.bootstrap_servers,
            group_id=config.broker.consumer_group_id,
            auto_offset_reset="earliest",
            key_deserializer=lambda m: m.decode("utf-8") if m else None,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            request_timeout_ms=40000,
            max_poll_interval_ms=300000,
        )
        try:
            logger.info("Connecting to broker...")
            await asyncio.wait_for(consumer.start(), timeout=10.0)
            logger.info("Successfully connected to Kafka")
            connected = True
            break
        except asyncio.TimeoutError:
            logger.error("Connection timeout, cleaning up and retrying...")
        except kafka_errors.KafkaError as e:
            logger.error("Can't connect to broker: %s", e)
        finally:
            if not connected:
                try:
                    await consumer.stop()
                except Exception:
                    pass
                consumer = None
            await asyncio.sleep(2)

    try:
        logger.info("\nListening to topics: %s\n", topic_names)
        async for msg in consumer:
            if stop_event.is_set():
                break
            print_message(msg)
            await asyncio.sleep(0.2)
    except Exception as e:
        logger.error("Error consuming messages: %s", e)
    finally:
        if consumer is not None:
            await consumer.stop()
            logger.info("Kafka consumer stopped successfully")
