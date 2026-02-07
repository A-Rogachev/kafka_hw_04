import asyncio
import contextlib
import logging
import random
import signal
from typing import cast

import asyncpg
import faker

from src.config import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def _write_data(pool: asyncpg.pool.Pool, stop_event: asyncio.Event) -> int:
    """
    Генерация фейковых данных и вставка их в бд postgres.

    :param pool: пул подключений к базе данных PostgreSQL
    :param stop_event: событие для остановки генерации данных

    :return: общее количество вставленных записей
    """

    fake, total_inserted = faker.Faker(), 0

    async def produce_records() -> None:
        nonlocal total_inserted
        while not stop_event.is_set():
            try:
                async with pool.acquire() as conn:
                    async with cast(asyncpg.Connection, conn).transaction():
                        name, email = fake.name(), fake.email()
                        created_user_id: int = await conn.fetchval(
                            """
                            INSERT INTO users(name, email) VALUES($1, $2) RETURNING id
                            """,
                            name,
                            email,
                        )
                        total_inserted += 1
                        if total_inserted % 15 == 0:
                            logger.info("Inserted: %d", total_inserted)
                        for _ in range(random.randint(1, 2)):
                            product, quantity = fake.word(), random.randint(1, 10)
                            await conn.execute(
                                """
                                INSERT INTO orders(user_id, product_name, quantity)
                                VALUES($1, $2, $3)
                                """,
                                created_user_id,
                                product,
                                quantity,
                            )
                            total_inserted += 1
                            if total_inserted % 15 == 0:
                                logger.info("Inserted: %d", total_inserted)

                await asyncio.sleep(round(random.uniform(0.15, 0.35), 2))
            except asyncpg.PostgresError:
                logger.error("Database error, %s", exc_info=True)
                await asyncio.sleep(random.randint(1, 3))

    task: asyncio.Task = asyncio.create_task(produce_records())
    try:
        await stop_event.wait()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return total_inserted


async def produce(config: Settings) -> None:
    """
    Запускает процесс генерации и вставки данных в базу данных PostgreSQL.
    """
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    try:
        pool: asyncpg.Pool = await asyncpg.create_pool(
            dsn=config.postgres_dsn,
            **config.postgres.pool_config,
        )
    except asyncpg.PostgresError as err:
        logger.error("Failed to connect to the database: %s", err)
        return

    logger.info("DB successfully connected")
    try:
        total: int = await _write_data(pool, stop_event)
    finally:
        await pool.close()
        logger.info("DB connection was closed")
        logger.info("Total records inserted in this run: %d", total)
