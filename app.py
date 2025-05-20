import json
import os
import asyncio
import boto3
from concurrent.futures import ThreadPoolExecutor

from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    Logger,
    LogLevel
)

# Get VALKEY_HOST from environment variable
VALKEY_HOST = os.getenv('VALKEY_HOST')  # Fetches from Lambda environment variable
VALKEY_PORT = 6379  # Assuming default port
USE_TLS = True

# Setup DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table')

Logger.set_logger_config(LogLevel.INFO)

# DynamoDB Operations
def write_to_dynamodb(config_key, config_value):
    table.put_item(Item={'key': config_key, 'value': config_value})

def read_from_dynamodb(config_key):
    response = table.get_item(Key={'key': config_key})
    return response.get('Item')

def update_dynamodb(config_key, updated_values):
    update_expression = "SET "
    expression_attribute_names = {"#v": "value"}
    expression_attribute_values = {}

    for i, (k, v) in enumerate(updated_values.items()):
        update_expression += f"#v.{k} = :val{i}, "
        expression_attribute_values[f":val{i}"] = v

    update_expression = update_expression.rstrip(", ")

    table.update_item(
        Key={'key': config_key},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )

# Glide (Valkey) Client setup
async def get_valkey_client():
    if not VALKEY_HOST:
        raise ValueError("VALKEY_HOST environment variable not set!")
    config = GlideClusterClientConfiguration(
        addresses=[NodeAddress(VALKEY_HOST, VALKEY_PORT)],
        use_tls=USE_TLS
    )
    return await GlideClusterClient.create(config)

# Lambda handler function
async def handler(event, context):
    method = event['httpMethod']
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor()

    if method == 'POST':
        body = json.loads(event.get('body', '{}'))
        config_key = body.get('config_key')
        config_value = body.get('config_value')

        if not config_key or not config_value:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "config_key and config_value required"})
            }

        # Write to DynamoDB (non-blocking)
        await loop.run_in_executor(executor, write_to_dynamodb, config_key, config_value)

        # Write to Valkey
        client = await get_valkey_client()
        await client.set(config_key, json.dumps(config_value))
        await client.close()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Stored in DynamoDB and Valkey"})
        }

    elif method == 'GET':
        params = event.get('queryStringParameters', {})
        config_key = params.get('key') if params else None

        if not config_key:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing query parameter 'key'"})
            }

        # Try Valkey
        client = await get_valkey_client()
        val = await client.get(config_key)
        await client.close()

        if val:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "config_key": config_key,
                    "config_value": json.loads(val.decode('utf-8'))
                })
            }

        # Fallback to DynamoDB
        item = await loop.run_in_executor(executor, read_from_dynamodb, config_key)

        if item:
            # Cache back into Valkey
            client = await get_valkey_client()
            await client.set(config_key, json.dumps(item['value']))
            await client.close()

            return {
                "statusCode": 200,
                "body": json.dumps(item)
            }

        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Key not found"})
        }

    elif method == 'PATCH':
        body = json.loads(event.get('body', '{}'))
        config_key = body.get('config_key')
        updated_values = body.get('config_value')

        if not config_key or not updated_values:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "config_key and config_value required"})
            }

        # Update DynamoDB
        await loop.run_in_executor(executor, update_dynamodb, config_key, updated_values)

        # Update Valkey
        client = await get_valkey_client()
        await client.set(config_key, json.dumps(updated_values))
        await client.close()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"{config_key} updated in DynamoDB and Valkey"})
        }

    return {
        "statusCode": 405,
        "body": json.dumps({"message": "Method not allowed"})
    }

# Lambda function entry point
def lambda_handler(event, context):
    return asyncio.run(handler(event, context))
