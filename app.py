import asyncio
import logging

from glide import (
    ClosingError,
    ConnectionError,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    Logger,
    LogLevel,
    NodeAddress,
    RequestError,
    TimeoutError,
)

# Set up Python logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Set up Glide internal logging level
Logger.set_logger_config(LogLevel.INFO)


async def connect_to_valkey():
    addresses = [
        NodeAddress("config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com", 6379)
    ]
    config = GlideClusterClientConfiguration(addresses=addresses, use_tls=True)
    client = None

    try:
        logger.info("Attempting to connect to Valkey via Glide...")

        # Create the client
        client = await GlideClusterClient.create(config)
        logger.info("Connected to Valkey successfully.")

        # Perform SET operation
        result = await client.set("key", "value")
        logger.info(f"SET operation: key='key', value='value', result={result}")

        # Perform GET operation
        value = await client.get("key")
        logger.info(f"GET operation: key='key', value={value}")

        # Perform PING operation
        ping_response = await client.ping()
        logger.info(f"PING operation response: {ping_response}")

        return {
            "statusCode": 200,
            "body": f"Ping: {ping_response}, GET: {value}"
        }

    except TimeoutError as e:
        logger.error(f"TimeoutError occurred: {e}")
    except RequestError as e:
        logger.error(f"RequestError occurred: {e}")
    except ConnectionError as e:
        logger.error(f"ConnectionError occurred: {e}")
    except ClosingError as e:
        logger.error(f"ClosingError occurred: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        # Ensure client is closed
        if client:
            try:
                await client.close()
                logger.info("Client connection closed successfully.")
            except ClosingError as e:
                logger.error(f"Error while closing client: {e}")


def lambda_handler(event, context):
    return asyncio.run(connect_to_valkey())
