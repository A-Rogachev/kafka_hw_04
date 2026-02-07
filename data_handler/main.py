import argparse
import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, Final, TypeAlias

from src.consumer import consume
from src.producer import produce

DataHandler: TypeAlias = Callable[..., Coroutine[Any, Any, None]]
PRODUCE: Final[str] = "produce"
CONSUME: Final[str] = "consume"

HANDLER_REGISTRY: Final[dict[str, DataHandler]] = {
    PRODUCE: produce,
    CONSUME: consume,
}


async def main() -> None:
    """
    В зависимости от переданного аргумента запускается соответствующий обработчик данных.
    """
    from src.config import get_settings

    parser = argparse.ArgumentParser(description="Kafka data handler")
    parser.add_argument("mode", choices=["produce", "consume"])
    args = parser.parse_args()
    await HANDLER_REGISTRY[args.mode](config=get_settings())


if __name__ == "__main__":
    asyncio.run(main())
