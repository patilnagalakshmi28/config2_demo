import asyncio
import json
import logging
import boto3
import os

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

# Set up logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Glide Valkey endpoint (use env vars or hardcode for now)
VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True

# DynamoDB config
DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "your-dynamodb-table-name")
dynamodb = boto3.client("dynamodb")


async def store_in_valkey(key: str, value: str):
    Logger.set_logger_config(LogLevel.INFO)
    addresses = [NodeAddress(VALKEY_HOST, VALKEY_PORT)]
    config = GlideClusterClientConfiguration(addresses=addresses, use_tls=USE_TLS)
    client = None

    try:
        logger.info("Connecting to Valkey...")
        client = await GlideClusterClient.create(config)

        # Store key-value pair
        result = await client.set(key, value)
        logger.info(f"Stored in Valkey: {key} -> {result}")
        return True
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey error: {e}")
        return False
    finally:
        if client:
            try:
                await client.close()
                logger.info("Valkey connection closed.")
            except ClosingError as e:
                logger.error(f"Error closing Valkey client: {e}")


def store_in_dynamodb(key, value_dict):
    try:
        response = dynamodb.put_item(
            TableName=DYNAMO_TABLE,
            Item={
                "config_key": {"S": key},
                "config_value": {"M": value_dict}
            }
        )
        logger.info(f"Stored in DynamoDB: {key}")
        return True
    except Exception as e:
        logger.error(f"DynamoDB error: {e}")
        return False


def lambda_handler(event, context):
    try:
        # Parse incoming JSON body
        body = json.loads(event.get("body", "{}"))
        config_key = body.get("config_key", {}).get("S")
        config_value = body.get("config_value", {}).get("M")

        if not config_key or not config_value:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing 'config_key' or 'config_value'"})
            }

        # Store in DynamoDB
        dynamo_result = store_in_dynamodb(config_key, config_value)

        # Store in Valkey as serialized string
        valkey_result = asyncio.run(store_in_valkey(config_key, json.dumps(config_value)))

        if dynamo_result and valkey_result:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Stored in Valkey and DynamoDB"})
            }
        else:
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Error storing in one or both systems"})
            }

    except Exception as e:
        logger.exception("Unexpected error")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }
