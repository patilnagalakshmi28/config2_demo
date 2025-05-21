import json
import boto3
import asyncio
import logging
import os
import socket

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

# -------------- Logging -------------- #
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
Logger.set_logger_config(LogLevel.INFO)

# -------------- AWS & Valkey Setup -------------- #
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True

# Global event loop for Valkey
event_loop = asyncio.get_event_loop()

# -------------- Debug: Test Valkey Connection -------------- #
def test_valkey_reachability():
    try:
        logger.info(f"Testing Valkey reachability at {VALKEY_HOST}:{VALKEY_PORT}")
        socket.create_connection((VALKEY_HOST, VALKEY_PORT), timeout=5)
        logger.info("✅ Successfully reached Valkey!")
    except Exception as e:
        logger.error(f"❌ Valkey unreachable: {e}")

# -------------- Valkey Helpers -------------- #
async def store_to_valkey(config_key: str, config_value: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None
    try:
        logger.info(f"[Valkey] Connecting to Valkey...")
        client = await GlideClusterClient.create(config)
        await client.set(config_key, config_value)
        logger.info(f"[Valkey] Stored key: {config_key}")
        return True
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"[Valkey] Store error: {e}")
        return False
    finally:
        if client:
            try:
                await client.close()
                logger.info("[Valkey] Closed client")
            except ClosingError as e:
                logger.error(f"[Valkey] Close error: {e}")

async def get_from_valkey(config_key: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None
    try:
        logger.info(f"[Valkey] Fetching key: {config_key}")
        client = await GlideClusterClient.create(config)
        value = await client.get(config_key)
        if value is None:
            return None
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"[Valkey] GET error: {e}")
        return None
    finally:
        if client:
            try:
                await client.close()
            except ClosingError as e:
                logger.error(f"[Valkey] Close error after GET: {e}")

# -------------- Lambda Handler -------------- #
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    test_valkey_reachability()  # Debug connectivity test

    try:
        method = event.get("httpMethod", "")
        if method == "POST":
            return handle_post(event)
        elif method == "GET":
            return handle_get(event)
        elif method == "PATCH":
            return handle_patch(event)
        else:
            return {
                "statusCode": 405,
                "body": json.dumps({"message": "Method not allowed"})
            }
    except Exception as e:
        logger.exception("❌ Unhandled exception")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

# -------------- Method Handlers -------------- #
def handle_post(event):
    body = json.loads(event.get("body", "{}"))
    config_key = body.get("config_key")
    config_value = body.get("config_value")

    if not config_key or not config_value:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "config_key and config_value are required"})
        }

    # Store in DynamoDB
    table.put_item(Item={'key': config_key, 'value': config_value})

    # Store in Valkey
    logger.info(f"[POST] Storing key '{config_key}' in Valkey...")
    success = event_loop.run_until_complete(store_to_valkey(config_key, json.dumps(config_value)))

    return {
        "statusCode": 200 if success else 500,
        "body": json.dumps({
            "message": "Stored successfully" if success else "Stored in DynamoDB, but failed in Valkey"
        })
    }

def handle_get(event):
    params = event.get("queryStringParameters") or {}
    config_key = params.get("config_key") or params.get("key")

    if not config_key:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing 'config_key' in query parameters"})
        }

    logger.info(f"[GET] Fetching key '{config_key}' from Valkey...")
    config_value_str = event_loop.run_until_complete(get_from_valkey(config_key))

    if config_value_str:
        try:
            config_value = json.loads(config_value_str)
        except Exception:
            config_value = config_value_str
        return {
            "statusCode": 200,
            "body": json.dumps({
                "config_key": config_key,
                "config_value": config_value
            })
        }

    logger.info(f"[GET] Fallback to DynamoDB for key: {config_key}")
    response = table.get_item(Key={'key': config_key})
    item = response.get('Item')

    if item:
        return {
            "statusCode": 200,
            "body": json.dumps(item)
        }
    else:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Key not found"})
        }

def handle_patch(event):
    body = json.loads(event.get("body", "{}"))
    config_key = body.get("config_key")
    updated_values = body.get("config_value")

    if not config_key or not isinstance(updated_values, dict):
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "config_key and config_value (as object) are required"})
        }

    logger.info(f"[PATCH] Updating key '{config_key}' in DynamoDB...")
    # Update in DynamoDB
    update_expression = "SET "
    expression_attribute_names = {"#v": "value"}
    expression_attribute_values = {}

    for i, (k, v) in enumerate(updated_values.items()):
        path = f"#v.{k}"
        value_key = f":val{i}"
        expression_attribute_values[value_key] = v
        update_expression += f"{path} = {value_key}, "

    update_expression = update_expression.rstrip(", ")

    table.update_item(
        Key={'key': config_key},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )

    logger.info(f"[PATCH] Updating Valkey for key: {config_key}")
    success = event_loop.run_until_complete(store_to_valkey(config_key, json.dumps(updated_values)))

    return {
        "statusCode": 200 if success else 500,
        "body": json.dumps({
            "message": "Updated successfully" if success else "Updated in DynamoDB, but failed in Valkey"
        })
    }
