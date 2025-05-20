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

# Helper function to convert the structured data into a JSON string
def serialize_config_value(config_value):
    try:
        return json.dumps(config_value)
    except Exception as e:
        logger.error(f"Error serializing config_value: {e}")
        return None

async def store_to_valkey(config_key: str, config_value: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info(f"Connecting to Valkey...")
        client = await GlideClusterClient.create(config)
        logger.info("Connected successfully")

        result = await client.set(config_key, config_value)
        logger.info(f"Set config_key='{config_key}' result={result}")
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

async def get_from_valkey(config_key: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info(f"Connecting to Valkey...")
        client = await GlideClusterClient.create(config)
        logger.info("Connected successfully")

        value = await client.get(config_key)

        if value:
            # Decode the bytes value to string
            value_str = value.decode('utf-8') if isinstance(value, bytes) else value
            logger.info(f"Got value for config_key='{config_key}': {value_str}")
            return value_str
        else:
            logger.info(f"config_key '{config_key}' not found.")
            return None
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
            config_key = body.get("config_key")
            config_value = body.get("config_value")

            if not config_key or not config_value:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing 'config_key' or 'config_value'"})
                }

            # Serialize config_value to JSON string format
            config_value_str = serialize_config_value(config_value)

            if config_value_str is None:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Failed to serialize config_value"})
                }

            success = asyncio.run(store_to_valkey(config_key, config_value_str))

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
            config_key = event["queryStringParameters"].get("config_key") if event.get("queryStringParameters") else None

            if not config_key:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing 'config_key' in query parameters"})
                }

            config_value = asyncio.run(get_from_valkey(config_key))

            if config_value is None:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": f"config_key '{config_key}' not found in Valkey"})
                }

            return {
                "statusCode": 200,
                "body": json.dumps({"config_key": config_key, "config_value": json.loads(config_value)})
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
