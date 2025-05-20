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
        logger.info(f"Stored config_key='{config_key}'")
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
            value_str = value.decode("utf-8") if isinstance(value, bytes) else str(value)
            logger.info(f"Fetched value for config_key='{config_key}'")
            return value_str
        else:
            logger.info(f"Key '{config_key}' not found.")
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
            body = json.loads(event.get("body", "{}"))

            # Extract config_key and config_value
            config_key = body.get("config_key", {}).get("S")
            config_value_map = body.get("config_value", {}).get("M")

            if not config_key or not config_value_map:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing or malformed 'config_key' or 'config_value'"})
                }

            # Flatten value structure
            flat_value = {k: v.get("S") for k, v in config_value_map.items()}
            config_value_str = json.dumps(flat_value)

            success = asyncio.run(store_to_valkey(config_key, config_value_str))

            return {
                "statusCode": 200 if success else 500,
                "body": json.dumps({
                    "message": "Stored successfully in Valkey" if success else "Failed to store in Valkey"
                })
            }

        elif method == "GET":
            query = event.get("queryStringParameters", {})
            config_key = query.get("config_key")

            if not config_key:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing 'config_key' in query parameters"})
                }

            config_value_str = asyncio.run(get_from_valkey(config_key))

            if config_value_str is None:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": f"Key '{config_key}' not found in Valkey"})
                }

            # Convert back to dict if possible
            try:
                config_value_obj = json.loads(config_value_str)
            except Exception:
                config_value_obj = config_value_str

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "config_key": config_key,
                    "config_value": config_value_obj
                })
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
