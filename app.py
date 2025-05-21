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

# Setup logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

# Valkey Config from Environment
VALKEY_HOST = os.getenv("VALKEY_HOST", "config-valkey-vfeb3i.serverless.use1.cache.amazonaws.com")
VALKEY_PORT = 6379
USE_TLS = True
VALKEY_TIMEOUT = 2  # seconds

# Enable Glide internal logging
Logger.set_logger_config(LogLevel.INFO)

# ------------------- Valkey Helper Functions with Timeout ------------------- #

async def store_to_valkey(config_key: str, config_value: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info("Connecting to Valkey...")
        client = await asyncio.wait_for(
            GlideClusterClient.create(config),
            timeout=VALKEY_TIMEOUT
        )
        logger.info("Connected to Valkey")

        await asyncio.wait_for(
            client.set(config_key, config_value),
            timeout=VALKEY_TIMEOUT
        )
        logger.info(f"Stored key: {config_key}")
        return True
    except asyncio.TimeoutError:
        logger.error("Valkey operation timed out")
        return False
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey Error: {e}")
        return False
    finally:
        if client:
            try:
                await client.close()
                logger.info("Closed Valkey client")
            except (ClosingError, asyncio.TimeoutError) as e:
                logger.error(f"Error closing client: {e}")

async def get_from_valkey(config_key: str):
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    client = None

    try:
        logger.info(f"Fetching key '{config_key}' from Valkey...")
        client = await asyncio.wait_for(
            GlideClusterClient.create(config),
            timeout=VALKEY_TIMEOUT
        )

        value = await asyncio.wait_for(
            client.get(config_key),
            timeout=VALKEY_TIMEOUT
        )
        
        if value is None:
            return None

        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)
    except asyncio.TimeoutError:
        logger.error("Valkey GET operation timed out")
        return None
    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        logger.error(f"Valkey GET error: {e}")
        return None
    finally:
        if client:
            try:
                await client.close()
                logger.info("Closed Valkey client after GET")
            except (ClosingError, asyncio.TimeoutError) as e:
                logger.error(f"Error closing client: {e}")

# ------------------- Combined Lambda Handler ------------------- #

def lambda_handler(event, context):
    method = event['httpMethod']
    
    try:
        # POST - Store config_key and config_value
        if method == 'POST':
            body = json.loads(event['body'])
            config_key = body.get('config_key')
            config_value = body.get('config_value')

            if not config_key or not config_value:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "config_key and config_value required"})
                }

            # Store in DynamoDB first (primary source)
            table.put_item(Item={
                'key': config_key,
                'value': config_value
            })

            # Then try Valkey (best effort)
            try:
                valkey_success = asyncio.get_event_loop().run_until_complete(
                    store_to_valkey(config_key, json.dumps(config_value))
                )
            except Exception as e:
                logger.error(f"Valkey store failed: {e}")
                valkey_success = False

            if not valkey_success:
                logger.warning("Valkey store failed, but data persisted in DynamoDB")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Stored successfully",
                    "valkey_status": "success" if valkey_success else "failed"
                })
            }

        # GET - Retrieve value by config_key
        elif method == 'GET':
            params = event.get('queryStringParameters', {})
            if not params or 'key' not in params:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Missing query parameter 'key'"})
                }

            key = params['key']
            
            # First try Valkey with timeout
            valkey_value = None
            try:
                valkey_value = asyncio.get_event_loop().run_until_complete(
                    get_from_valkey(key)
                )
            except Exception as e:
                logger.error(f"Valkey GET failed: {e}")

            if valkey_value:
                try:
                    val_value = json.loads(valkey_value)
                except json.JSONDecodeError:
                    val_value = valkey_value
                
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "source": "valkey",
                        "value": val_value
                    })
                }
            
            # Fall back to DynamoDB
            try:
                response = table.get_item(Key={'key': key})
                item = response.get('Item')

                if item:
                    return {
                        "statusCode": 200,
                        "body": json.dumps({
                            "source": "dynamodb",
                            "value": item.get('value')
                        })
                    }
            except Exception as e:
                logger.error(f"DynamoDB GET failed: {e}")
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "Failed to retrieve data"})
                }

            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Not found in either Valkey or DynamoDB"})
            }

        # PATCH - Update value for existing config_key
        elif method == 'PATCH':
            body = json.loads(event['body'])
            config_key = body.get('config_key')
            updated_values = body.get('config_value')

            if not config_key or not updated_values:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "config_key and config_value required"})
                }

            # Update DynamoDB first
            update_expression = "SET "
            expression_attribute_names = {}
            expression_attribute_values = {}

            for i, (k, v) in enumerate(updated_values.items()):
                path = f"#v.{k}"
                value_key = f":val{i}"
                expression_attribute_names[f"#v"] = "value"
                expression_attribute_values[value_key] = v
                update_expression += f"{path} = {value_key}, "

            update_expression = update_expression.rstrip(", ")

            table.update_item(
                Key={'key': config_key},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )

            # Then try Valkey (best effort)
            try:
                valkey_success = asyncio.get_event_loop().run_until_complete(
                    store_to_valkey(config_key, json.dumps(updated_values)))
            except Exception as e:
                logger.error(f"Valkey update failed: {e}")
                valkey_success = False

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": f'Updated {config_key} successfully',
                    "valkey_status": "success" if valkey_success else "failed"
                })
            }

        # Unsupported method
        else:
            return {
                "statusCode": 405,
                "body": json.dumps({"error": "Method Not Allowed"})
            }
            
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"})
        }