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

# Enable Glide internal logging
Logger.set_logger_config(LogLevel.INFO)

# ------------------- Valkey Helper Functions ------------------- #

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

            # Store in DynamoDB
            table.put_item(Item={
                'key': config_key,
                'value': config_value
            })

            # Store in Valkey
            valkey_success = asyncio.run(store_to_valkey(config_key, json.dumps(config_value)))
            
            if not valkey_success:
                logger.error("Failed to store in Valkey, but DynamoDB operation succeeded")

            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Stored in both DynamoDB and Valkey"})
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
            
            # First try Valkey
            valkey_value = asyncio.run(get_from_valkey(key))
            if valkey_value:
                try:
                    valkey_value = json.loads(valkey_value)
                except json.JSONDecodeError:
                    pass  # Return as string if not JSON
                
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "source": "valkey",
                        "value": valkey_value
                    })
                }
            
            # Fall back to DynamoDB if not found in Valkey
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
            else:
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

            # Update DynamoDB
            update_expression = "SET "
            expression_attribute_names = {}
            expression_attribute_values = {}

            for i, (k, v) in enumerate(updated_values.items()):
                path = f"#v.{k}"
                value_key = f":val{i}"
                expression_attribute_names[f"#v"] = "value"  # Top-level map
                expression_attribute_values[value_key] = v
                update_expression += f"{path} = {value_key}, "

            update_expression = update_expression.rstrip(", ")

            table.update_item(
                Key={'key': config_key},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )

            # Update Valkey
            valkey_success = asyncio.run(store_to_valkey(config_key, json.dumps(updated_values)))
            
            if not valkey_success:
                logger.error("Failed to update Valkey, but DynamoDB operation succeeded")

            return {
                "statusCode": 200,
                "body": json.dumps({'message': f'Updated {config_key} successfully in both DynamoDB and Valkey'})
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