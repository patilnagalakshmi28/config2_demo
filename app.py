import asyncio
import json
import logging
import os

from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    Logger,
    LogLevel,
    TimeoutError,
    RequestError,
    ConnectionError,
    ClosingError
)

# Logging config
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Valkey endpoint from environment variables
VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True

# Enable internal Glide logging
Logger.set_logger_config(LogLevel.INFO)

async def store_to_valkey(key: str, value: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info(f"Connecting to Valkey...")
        client = await GlideClusterClient.create(config)
        logger.info("Connected successfully")

        result = await client.set(key, value)
        logger.info(f"Set key='{key}' result={result}")
        return True
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey Error: {e}")
        return False
    finally:
        if client:
            try:
                await client.close()
                logger.info("Closed Valkey client")
            except ClosingError as e:
                logger.error(f"Error closing Valkey client: {e}")

async def get_from_valkey(key: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info(f"Connecting to Valkey...")
        client = await GlideClusterClient.create(config)
        logger.info("Connected successfully")

        value = await client.get(key)
        logger.info(f"Got value for key='{key}': {value}")
        return value
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey Error: {e}")
        return None
    finally:
        if client:
            try:
                await client.close()
                logger.info("Closed Valkey client")
            except ClosingError as e:
                logger.error(f"Error closing Valkey client: {e}")

def lambda_handler(event, context):
    try:
        method = event["httpMethod"]

        if method == "POST":
            # Handle POST request to store data in Valkey
            body = json.loads(event.get("body", "{}"))
            key = body.get("key")
            value = body.get("value")

            if not key or value is None:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing 'key' or 'value'"})
                }

            # Serialize value to string
            value_str = json.dumps(value)

            success = asyncio.run(store_to_valkey(key, value_str))

            if success:
                return {
                    "statusCode": 200,
                    "body": json.dumps({"message": "Stored successfully in Valkey"})
                }
            else:
                return {
                    "statusCode": 500,
                    "body": json.dumps({"message": "Failed to store in Valkey"})
                }

        elif method == "GET":
            # Handle GET request to fetch data from Valkey using query parameters
            key = event["queryStringParameters"].get("key") if event.get("queryStringParameters") else None

            if not key:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing 'key' in query parameters"})
                }

            value = asyncio.run(get_from_valkey(key))

            if value is None:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": f"Key '{key}' not found in Valkey"})
                }

            return {
                "statusCode": 200,
                "body": json.dumps({"key": key, "value": value})
            }

        else:
            return {
                "statusCode": 405,
                "body": json.dumps({"message": "Method not allowed"})
            }

    except Exception as e:
        logger.exception("Unhandled exception")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }
