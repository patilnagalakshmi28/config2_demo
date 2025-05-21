import json
import boto3
import asyncio
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

# ---------- Configuration ---------- #
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True

# ---------- Logging ---------- #
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
Logger.set_logger_config(LogLevel.INFO)

# ---------- Valkey Helpers ---------- #
async def store_to_valkey(config_key: str, config_value: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None
    try:
        client = await GlideClusterClient.create(config)
        await client.set(config_key, config_value)
        return True
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey Error: {e}")
        return False
    finally:
        if client:
            try:
                await client.close()
            except ClosingError as e:
                logger.error(f"Error closing Valkey client: {e}")

async def get_from_valkey(config_key: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None
    try:
        client = await GlideClusterClient.create(config)
        value = await client.get(config_key)
        if value is None:
            return None
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey GET error: {e}")
        return None
    finally:
        if client:
            try:
                await client.close()
            except ClosingError as e:
                logger.error(f"Error closing Valkey client after GET: {e}")

# ---------- Lambda Handler ---------- #
def lambda_handler(event, context):
    try:
        method = event["httpMethod"]

        if method == "POST":
            return handle_post(event)

        elif method == "GET":
            return handle_get(event)

        elif method == "PATCH":
            return handle_patch(event)

        else:
            return {
                "statusCode": 405,
                "body": json.dumps({"error": "Method Not Allowed"})
            }

    except Exception as e:
        logger.exception("Unhandled exception in Lambda")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

# ---------- Handlers for Each Method ---------- #
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
    success = asyncio.run(store_to_valkey(config_key, json.dumps(config_value)))

    return {
        "statusCode": 200 if success else 500,
        "body": json.dumps({"message": "Stored successfully" if success else "Stored in DynamoDB, but failed in Valkey"})
    }

def handle_get(event):
    params = event.get('queryStringParameters') or {}
    config_key = params.get("config_key") or params.get("key")

    if not config_key:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing 'config_key' in query parameters"})
        }

    # Try to get from Valkey
    config_value_str = asyncio.run(get_from_valkey(config_key))

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

    # Fallback to DynamoDB
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
            "body": json.dumps({"error": "Key not found"})
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

    # Update DynamoDB
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

    # Update in Valkey
    success = asyncio.run(store_to_valkey(config_key, json.dumps(updated_values)))

    return {
        "statusCode": 200 if success else 500,
        "body": json.dumps({
            "message": "Updated successfully" if success else "Updated in DynamoDB, but failed in Valkey"
        })
    }
