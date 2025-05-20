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

# Logging setup
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Valkey config
VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True

# Enable internal glide logging
Logger.set_logger_config(LogLevel.INFO)

# -------------------- Valkey Helpers -------------------- #

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
        logger.info(f"Set config_key='{config_key}' with result={result}")
        return True
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey error: {e}")
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
        logger.info(f"Fetching from Valkey...")
        client = await GlideClusterClient.create(config)
        value = await client.get(config_key)

        if value:
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
        else:
            return None
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey error during GET: {e}")
        return None
    finally:
        if client:
            try:
                await client.close()
                logger.info("Closed Valkey client after GET")
            except ClosingError as e:
                logger.error(f"Error closing Valkey client: {e}")

# -------------------- Lambda Handler -------------------- #

def lambda_handler(event, context):
    try:
        method = event["httpMethod"]

        # ------------- POST (Create New Key) -------------
        if method == "POST":
            return handle_store_or_update(event, action="create")

        # ------------- PATCH (Update Existing Key) -------------
        elif method == "PATCH":
            return handle_store_or_update(event, action="update")

        # ------------- GET (Fetch by Key) -------------
        elif method == "GET":
            query = event.get("queryStringParameters", {})
            config_key = query.get("config_key")

            if not config_key:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing 'config_key' in query parameters"})
                }

            config_value_str = asyncio.run(get_from_valkey(config_key))

            if not config_value_str:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": f"Key '{config_key}' not found in Valkey"})
                }

            try:
                config_value_obj = json.loads(config_value_str)
            except Exception as e:
                logger.warning(f"Could not parse value as JSON: {e}")
                config_value_obj = config_value_str

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "config_key": config_key,
                    "config_value": config_value_obj
                })
            }

        # ------------- Unsupported Method -------------
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

# -------------------- Shared Handler -------------------- #

def handle_store_or_update(event, action="create"):
    body = json.loads(event.get("body", "{}"))

    config_key = body.get("config_key", {}).get("S")
    config_value_map = body.get("config_value", {}).get("M")

    if not config_key or not config_value_map:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing or malformed 'config_key' or 'config_value'"})
        }

    # Flatten DynamoDB-like structure
    flat_value = {k: v.get("S") for k, v in config_value_map.items()}
    config_value_str = json.dumps(flat_value)

    success = asyncio.run(store_to_valkey(config_key, config_value_str))

    return {
        "statusCode": 200 if success else 500,
        "body": json.dumps({
            "message": f"{'Updated' if action == 'update' else 'Stored'} successfully in Valkey" if success else f"Failed to {action} value in Valkey"
        })
    }
