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

# Setup logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Valkey Config from Environment
VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True

# Enable Glide internal logging
Logger.set_logger_config(LogLevel.INFO)

# ------------------- Valkey Helpers ------------------- #

async def store_to_valkey(config_key: str, config_value: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info("Connecting to Valkey...")
        client = await GlideClusterClient.create(config)
        logger.info("Connected to Valkey")

        await client.set(config_key, config_value)
        logger.info(f"Stored key: {config_key}")
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
                logger.error(f"Error closing client: {e}")

async def get_from_valkey(config_key: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info(f"Fetching key '{config_key}' from Valkey...")
        client = await GlideClusterClient.create(config)

        value = await client.get(config_key)
        if value is None:
            return None

        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey GET error: {e}")
        return None
    finally:
        if client:
            try:
                await client.close()
                logger.info("Closed Valkey client after GET")
            except ClosingError as e:
                logger.error(f"Error closing client: {e}")

# ------------------- Lambda Handler ------------------- #

def lambda_handler(event, context):
    try:
        method = event["httpMethod"]

        if method == "POST":
            return handle_store_or_update(event, action="create")

        elif method == "PATCH":
            return handle_store_or_update(event, action="update")

        elif method == "GET":
            query = event.get("queryStringParameters", {})
            config_key = query.get("config_key")

            if not config_key:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing 'config_key' in query parameters"})
                }

            config_value_str = asyncio.run(get_from_valkey(config_key))

            if config_value_str is None or config_value_str.strip() == "":
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": f"Key '{config_key}' not found in Valkey"})
                }

            try:
                config_value_obj = json.loads(config_value_str)
            except Exception as e:
                logger.warning(f"Failed to parse value as JSON: {e}")
                config_value_obj = config_value_str  # Fallback to raw string

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
        logger.exception("Unhandled exception in Lambda")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }

# ------------------- Shared POST/PATCH Handler ------------------- #

def handle_store_or_update(event, action="create"):
    body = json.loads(event.get("body", "{}"))

    config_key = body.get("config_key", {}).get("S")
    config_value_map = body.get("config_value", {}).get("M")

    if not config_key or not config_value_map:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing or malformed 'config_key' or 'config_value'"})
        }

    # Flatten the DynamoDB-style input into a simple dictionary
    flat_value = {k: v.get("S") for k, v in config_value_map.items()}
    config_value_str = json.dumps(flat_value)

    success = asyncio.run(store_to_valkey(config_key, config_value_str))

    return {
        "statusCode": 200 if success else 500,
        "body": json.dumps({
            "message": f"{'Updated' if action == 'update' else 'Stored'} successfully in Valkey" if success else f"Failed to {action} value in Valkey"
        })
    }
